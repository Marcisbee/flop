package flop

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/marcisbee/flop/internal/cron"
)

const (
	backupSettingsRelPath = "_system/backups.json"
	emailSettingsBackupRelPath = "_system/email.json"
	localBackupsDirName   = "backups"
	backupSecretMask      = "******"
)

var protectedBackupRestorePaths = []string{
	backupSettingsRelPath,
	emailSettingsBackupRelPath,
}

type BackupS3Config struct {
	Enabled        bool   `json:"enabled"`
	Bucket         string `json:"bucket"`
	Region         string `json:"region"`
	Endpoint       string `json:"endpoint"`
	AccessKey      string `json:"accessKey"`
	Secret         string `json:"secret,omitempty"`
	ForcePathStyle bool   `json:"forcePathStyle"`
}

type BackupSettings struct {
	Cron        string         `json:"cron"`
	CronMaxKeep int            `json:"cronMaxKeep"`
	S3          BackupS3Config `json:"s3"`
}

type AdminBackupFile struct {
	Key      string    `json:"key"`
	Size     int64     `json:"size"`
	Modified time.Time `json:"modified"`
}

type backupStorage interface {
	List(ctx context.Context) ([]AdminBackupFile, error)
	Save(ctx context.Context, key, localPath string) error
	Open(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
	Stat(ctx context.Context, key string) (AdminBackupFile, error)
	Test(ctx context.Context) error
}

type backupManager struct {
	db *Database

	settingsMu sync.RWMutex
	settings   BackupSettings

	jobMu     sync.Mutex
	jobCancel context.CancelFunc

	busy atomic.Bool
}

func newBackupManager(db *Database) (*backupManager, error) {
	m := &backupManager{db: db}
	settings, err := loadBackupSettings(db.GetDataDir())
	if err != nil {
		return nil, err
	}
	m.settings = settings
	if err := m.reloadSchedule(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *backupManager) stop() {
	m.jobMu.Lock()
	if m.jobCancel != nil {
		m.jobCancel()
		m.jobCancel = nil
	}
	m.jobMu.Unlock()
}

func (m *backupManager) Busy() bool {
	return m != nil && m.busy.Load()
}

func (m *backupManager) withBusy(fn func() error) error {
	if m == nil {
		return fmt.Errorf("backup manager unavailable")
	}
	if !m.busy.CompareAndSwap(false, true) {
		return fmt.Errorf("another backup or restore operation is already in progress")
	}
	defer m.busy.Store(false)
	return fn()
}

func (m *backupManager) getSettings() BackupSettings {
	m.settingsMu.RLock()
	defer m.settingsMu.RUnlock()
	return cloneBackupSettings(m.settings, true)
}

func (m *backupManager) rawSettings() BackupSettings {
	m.settingsMu.RLock()
	defer m.settingsMu.RUnlock()
	return cloneBackupSettings(m.settings, false)
}

func (m *backupManager) updateSettings(next BackupSettings) (BackupSettings, error) {
	next = normalizeBackupSettings(next)
	if err := validateBackupSettings(next); err != nil {
		return BackupSettings{}, err
	}
	if err := saveBackupSettings(m.db.GetDataDir(), next); err != nil {
		return BackupSettings{}, err
	}
	m.settingsMu.Lock()
	m.settings = next
	m.settingsMu.Unlock()
	if err := m.reloadSchedule(); err != nil {
		return BackupSettings{}, err
	}
	return cloneBackupSettings(next, true), nil
}

func (m *backupManager) testS3(cfg BackupS3Config) error {
	cfg = normalizeBackupS3Config(cfg)
	if !cfg.Enabled {
		return nil
	}
	if err := validateBackupS3Config(cfg); err != nil {
		return err
	}
	storage, err := newS3BackupStorage(cfg)
	if err != nil {
		return err
	}
	return storage.Test(context.Background())
}

func (m *backupManager) List(ctx context.Context) ([]AdminBackupFile, error) {
	storage, err := m.storage()
	if err != nil {
		return nil, err
	}
	items, err := storage.List(ctx)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(items, func(a, b AdminBackupFile) int {
		switch {
		case a.Modified.After(b.Modified):
			return -1
		case a.Modified.Before(b.Modified):
			return 1
		case a.Key < b.Key:
			return -1
		case a.Key > b.Key:
			return 1
		default:
			return 0
		}
	})
	return items, nil
}

func (m *backupManager) Stat(ctx context.Context, key string) (AdminBackupFile, error) {
	storage, err := m.storage()
	if err != nil {
		return AdminBackupFile{}, err
	}
	return storage.Stat(ctx, key)
}

func (m *backupManager) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	storage, err := m.storage()
	if err != nil {
		return nil, err
	}
	return storage.Open(ctx, key)
}

func (m *backupManager) Delete(ctx context.Context, key string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("backup key is required")
	}
	return m.withBusy(func() error {
		storage, err := m.storage()
		if err != nil {
			return err
		}
		return storage.Delete(ctx, key)
	})
}

func (m *backupManager) CreateManual(ctx context.Context) (string, error) {
	return m.create(ctx, false)
}

func (m *backupManager) CreateAuto(ctx context.Context) (string, error) {
	return m.create(ctx, true)
}

func (m *backupManager) Upload(ctx context.Context, filename string, file io.Reader) (string, error) {
	var created string
	err := m.withBusy(func() error {
		startedAt := time.Now()
		log.Printf("flop backup upload: start filename=%q", filename)

		storage, err := m.storage()
		if err != nil {
			log.Printf("flop backup upload: storage init failed filename=%q err=%v", filename, err)
			return err
		}
		log.Printf("flop backup upload: storage=%T filename=%q", storage, filename)

		materializeStarted := time.Now()
		tmpPath, cleanup, err := materializeUploadedBackup(file)
		if err != nil {
			log.Printf("flop backup upload: materialize failed filename=%q dur=%s err=%v", filename, time.Since(materializeStarted), err)
			return err
		}
		if cleanup != nil {
			defer cleanup()
		}
		logUploadFileInfo("flop backup upload: materialized", tmpPath, filename, time.Since(materializeStarted))

		validateStarted := time.Now()
		zipReader, err := zip.OpenReader(tmpPath)
		if err != nil {
			log.Printf("flop backup upload: zip validation failed filename=%q path=%q dur=%s err=%v", filename, tmpPath, time.Since(validateStarted), err)
			return fmt.Errorf("uploaded file must be a valid zip backup")
		}
		_ = zipReader.Close()
		log.Printf("flop backup upload: zip validation ok filename=%q path=%q dur=%s", filename, tmpPath, time.Since(validateStarted))

		key := generateUploadedBackupName(filename)
		saveStarted := time.Now()
		log.Printf("flop backup upload: storage save start filename=%q key=%q path=%q", filename, key, tmpPath)
		if err := storage.Save(ctx, key, tmpPath); err != nil {
			log.Printf("flop backup upload: storage save failed filename=%q key=%q dur=%s err=%v", filename, key, time.Since(saveStarted), err)
			return err
		}
		log.Printf("flop backup upload: storage save complete filename=%q key=%q dur=%s", filename, key, time.Since(saveStarted))
		created = key
		log.Printf("flop backup upload: done filename=%q key=%q total=%s", filename, key, time.Since(startedAt))
		return nil
	})
	return created, err
}

func materializeUploadedBackup(file io.Reader) (string, func(), error) {
	if osFile, ok := file.(*os.File); ok {
		return osFile.Name(), nil, nil
	}

	tmpFile, err := os.CreateTemp("", "flop-upload-backup-*.zip")
	if err != nil {
		return "", nil, err
	}
	tmpPath := tmpFile.Name()
	cleanup := func() { _ = os.Remove(tmpPath) }

	if _, err := io.Copy(tmpFile, file); err != nil {
		_ = tmpFile.Close()
		cleanup()
		return "", nil, err
	}
	if err := tmpFile.Close(); err != nil {
		cleanup()
		return "", nil, err
	}
	return tmpPath, cleanup, nil
}

func logUploadFileInfo(prefix, path, filename string, dur time.Duration) {
	info, err := os.Stat(path)
	if err != nil {
		log.Printf("%s filename=%q path=%q dur=%s stat_err=%v", prefix, filename, path, dur, err)
		return
	}
	log.Printf("%s filename=%q path=%q size=%d dur=%s", prefix, filename, path, info.Size(), dur)
}

func (m *backupManager) create(ctx context.Context, auto bool) (string, error) {
	var created string
	err := m.withBusy(func() error {
		storage, err := m.storage()
		if err != nil {
			return err
		}
		key := generateBackupName(auto)
		tmpZip, err := m.writeSnapshotZip()
		if err != nil {
			return err
		}
		defer os.Remove(tmpZip)

		if err := storage.Save(ctx, key, tmpZip); err != nil {
			return err
		}
		created = key
		if auto {
			if err := m.pruneAutoBackups(ctx, storage); err != nil {
				return err
			}
		}
		return nil
	})
	return created, err
}

func (m *backupManager) Restore(ctx context.Context, key string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("backup key is required")
	}
	return m.withBusy(func() error {
		storage, err := m.storage()
		if err != nil {
			return err
		}
		exists, err := storage.Exists(ctx, key)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("backup not found: %s", key)
		}

		localZip, restoreTmp, err := m.fetchBackupZip(ctx, storage, key)
		if err != nil {
			return err
		}
		defer os.RemoveAll(restoreTmp)

		extractDir := filepath.Join(restoreTmp, "extracted")
		if err := os.MkdirAll(extractDir, 0o755); err != nil {
			return err
		}
		if err := extractBackupZip(localZip, extractDir); err != nil {
			return err
		}

		return m.restoreExtractedDir(extractDir, restoreTmp)
	})
}

func (m *backupManager) restoreExtractedDir(extractDir, restoreTmp string) error {
	if m.db == nil || m.db.app == nil {
		return fmt.Errorf("restore requires app runtime context")
	}

	dataDir := m.db.GetDataDir()
	rollbackDir := filepath.Join(restoreTmp, "rollback")
	failedDir := filepath.Join(restoreTmp, "failed")
	if err := os.MkdirAll(rollbackDir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(failedDir, 0o755); err != nil {
		return err
	}

	if err := m.db.Close(); err != nil {
		return err
	}

	currentEntries, err := restorableEntries(dataDir)
	if err != nil {
		return err
	}
	for _, name := range currentEntries {
		if err := os.Rename(filepath.Join(dataDir, name), filepath.Join(rollbackDir, name)); err != nil {
			return err
		}
	}

	restoredEntries, err := directoryEntries(extractDir)
	if err != nil {
		_ = rollbackDataDir(dataDir, rollbackDir)
		_, reopenErr := m.db.reopen()
		return errors.Join(err, reopenErr)
	}
	for _, name := range restoredEntries {
		if err := os.Rename(filepath.Join(extractDir, name), filepath.Join(dataDir, name)); err != nil {
			_ = moveRestoredToFailed(dataDir, failedDir)
			_ = rollbackDataDir(dataDir, rollbackDir)
			_, reopenErr := m.db.reopen()
			return errors.Join(err, reopenErr)
		}
	}

	if err := restoreProtectedBackupPaths(dataDir, rollbackDir); err != nil {
		_ = moveRestoredToFailed(dataDir, failedDir)
		_ = rollbackDataDir(dataDir, rollbackDir)
		_, reopenErr := m.db.reopen()
		return errors.Join(err, reopenErr)
	}

	reopened, err := m.db.reopen()
	if err != nil {
		_ = moveRestoredToFailed(dataDir, failedDir)
		_ = rollbackDataDir(dataDir, rollbackDir)
		_, reopenErr := m.db.reopen()
		return errors.Join(err, reopenErr)
	}
	if reopened != nil && reopened.backupManager != nil {
		reopened.backupManager.db = reopened
	}
	_ = os.RemoveAll(rollbackDir)
	return nil
}

func (m *backupManager) fetchBackupZip(ctx context.Context, storage backupStorage, key string) (string, string, error) {
	restoreTmp := filepath.Join(m.db.GetDataDir(), fmt.Sprintf(".flop_restore_tmp_%d_%d", time.Now().UnixNano(), rand.Intn(1000)))
	if err := os.MkdirAll(restoreTmp, 0o755); err != nil {
		return "", "", err
	}

	src, err := storage.Open(ctx, key)
	if err != nil {
		return "", "", err
	}
	defer src.Close()

	localZip := filepath.Join(restoreTmp, "backup.zip")
	dst, err := os.Create(localZip)
	if err != nil {
		return "", "", err
	}
	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		return "", "", err
	}
	if err := dst.Close(); err != nil {
		return "", "", err
	}
	return localZip, restoreTmp, nil
}

func (m *backupManager) pruneAutoBackups(ctx context.Context, storage backupStorage) error {
	settings := m.getSettings()
	maxKeep := settings.CronMaxKeep
	if maxKeep <= 0 {
		return nil
	}

	items, err := storage.List(ctx)
	if err != nil {
		return err
	}
	autoItems := make([]AdminBackupFile, 0, len(items))
	for _, item := range items {
		if strings.HasPrefix(item.Key, "@auto_") {
			autoItems = append(autoItems, item)
		}
	}
	slices.SortFunc(autoItems, func(a, b AdminBackupFile) int {
		switch {
		case a.Modified.After(b.Modified):
			return -1
		case a.Modified.Before(b.Modified):
			return 1
		default:
			return strings.Compare(a.Key, b.Key)
		}
	})
	for i := maxKeep; i < len(autoItems); i++ {
		if err := storage.Delete(ctx, autoItems[i].Key); err != nil {
			return err
		}
	}
	return nil
}

func (m *backupManager) storage() (backupStorage, error) {
	settings := m.rawSettings()
	if settings.S3.Enabled {
		return newS3BackupStorage(settings.S3)
	}
	return &localBackupStorage{dir: filepath.Join(m.db.GetDataDir(), localBackupsDirName)}, nil
}

func (m *backupManager) reloadSchedule() error {
	settings := m.getSettings()
	m.jobMu.Lock()
	if m.jobCancel != nil {
		m.jobCancel()
		m.jobCancel = nil
	}
	cronExpr := strings.TrimSpace(settings.Cron)
	if cronExpr == "" {
		m.jobMu.Unlock()
		return nil
	}
	schedule, err := cron.Parse(cronExpr)
	if err != nil {
		m.jobMu.Unlock()
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.jobCancel = cancel
	m.jobMu.Unlock()

	go m.runSchedule(ctx, schedule)
	return nil
}

func (m *backupManager) runSchedule(ctx context.Context, schedule *cron.Schedule) {
	for {
		next := schedule.Next(time.Now().UTC())
		if next.IsZero() {
			return
		}
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			_, _ = m.CreateAuto(context.Background())
		}
	}
}

func (m *backupManager) writeSnapshotZip() (string, error) {
	if err := m.db.Checkpoint(); err != nil {
		return "", err
	}
	tmpFile, err := os.CreateTemp("", "flop-backup-*.zip")
	if err != nil {
		return "", err
	}
	tmpPath := tmpFile.Name()
	zipWriter := zip.NewWriter(tmpFile)

	err = filepath.WalkDir(m.db.GetDataDir(), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == m.db.GetDataDir() {
			return nil
		}

		rel, err := filepath.Rel(m.db.GetDataDir(), path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)

		if entry.IsDir() {
			if shouldSkipBackupEntry(rel) {
				return filepath.SkipDir
			}
			return nil
		}
		if shouldSkipBackupEntry(rel) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		header.Name = rel
		header.Method = zip.Deflate
		writer, err := zipWriter.CreateHeader(header)
		if err != nil {
			return err
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(writer, file)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
	if err != nil {
		_ = zipWriter.Close()
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return "", err
	}
	if err := zipWriter.Close(); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return "", err
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}
	return tmpPath, nil
}

func shouldSkipBackupEntry(rel string) bool {
	rel = strings.Trim(filepath.ToSlash(rel), "/")
	if rel == "" {
		return false
	}
	head := strings.Split(rel, "/")[0]
	if head == localBackupsDirName || head == "lost+found" {
		return true
	}
	if isProtectedSuperadminEntry(head) {
		return true
	}
	if strings.HasPrefix(rel, "_system/backup-") && strings.HasSuffix(rel, ".zip") {
		return true
	}
	if rel == backupSettingsRelPath || rel == emailSettingsBackupRelPath {
		return true
	}
	return strings.HasPrefix(head, ".flop_restore_tmp_")
}

func extractBackupZip(zipPath, dstDir string) error {
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer reader.Close()

	baseDst := filepath.Clean(dstDir) + string(os.PathSeparator)
	for _, file := range reader.File {
		name := strings.TrimSpace(file.Name)
		if name == "" {
			continue
		}
		target := filepath.Join(dstDir, filepath.FromSlash(name))
		cleanTarget := filepath.Clean(target)
		if !strings.HasPrefix(cleanTarget, baseDst) && cleanTarget != filepath.Clean(dstDir) {
			return fmt.Errorf("invalid backup entry path: %s", file.Name)
		}
		if file.FileInfo().IsDir() {
			if err := os.MkdirAll(cleanTarget, 0o755); err != nil {
				return err
			}
			continue
		}
		if !file.Mode().IsRegular() {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(cleanTarget), 0o755); err != nil {
			return err
		}
		rc, err := file.Open()
		if err != nil {
			return err
		}
		out, err := os.OpenFile(cleanTarget, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, file.Mode().Perm())
		if err != nil {
			_ = rc.Close()
			return err
		}
		_, copyErr := io.Copy(out, rc)
		closeOutErr := out.Close()
		closeRCErr := rc.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeOutErr != nil {
			return closeOutErr
		}
		if closeRCErr != nil {
			return closeRCErr
		}
	}
	return nil
}

func restorableEntries(dataDir string) ([]string, error) {
	entries, err := directoryEntries(dataDir)
	if err != nil {
		return nil, err
	}
	out := entries[:0]
	for _, name := range entries {
		if shouldSkipBackupEntry(name) {
			continue
		}
		out = append(out, name)
	}
	return out, nil
}

func restoreProtectedBackupPaths(dataDir, rollbackDir string) error {
	rollbackEntries, err := directoryEntries(rollbackDir)
	if err != nil {
		return err
	}
	for _, name := range rollbackEntries {
		if !isProtectedSuperadminEntry(name) {
			continue
		}
		src := filepath.Join(rollbackDir, name)
		dst := filepath.Join(dataDir, name)
		if err := os.RemoveAll(dst); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if err := copyPathRecursive(src, dst); err != nil {
			return err
		}
	}
	for _, rel := range protectedBackupRestorePaths {
		src := filepath.Join(rollbackDir, filepath.FromSlash(rel))
		dst := filepath.Join(dataDir, filepath.FromSlash(rel))
		if _, err := os.Stat(src); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				if removeErr := os.RemoveAll(dst); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
					return removeErr
				}
				continue
			}
			return err
		}
		if err := os.RemoveAll(dst); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if err := copyPathRecursive(src, dst); err != nil {
			return err
		}
	}
	return nil
}

func isProtectedSuperadminEntry(name string) bool {
	name = strings.TrimSpace(filepath.ToSlash(name))
	return name == systemSuperadminTableName || strings.HasPrefix(name, systemSuperadminTableName+".")
}

func copyPathRecursive(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if info.IsDir() {
		if err := os.MkdirAll(dst, info.Mode().Perm()); err != nil {
			return err
		}
		entries, err := os.ReadDir(src)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := copyPathRecursive(filepath.Join(src, entry.Name()), filepath.Join(dst, entry.Name())); err != nil {
				return err
			}
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode().Perm())
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

func directoryEntries(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.Name())
	}
	return out, nil
}

func rollbackDataDir(dataDir, rollbackDir string) error {
	entries, err := directoryEntries(rollbackDir)
	if err != nil {
		return err
	}
	for _, name := range entries {
		if err := os.Rename(filepath.Join(rollbackDir, name), filepath.Join(dataDir, name)); err != nil {
			return err
		}
	}
	return nil
}

func moveRestoredToFailed(dataDir, failedDir string) error {
	entries, err := restorableEntries(dataDir)
	if err != nil {
		return err
	}
	for _, name := range entries {
		if err := os.Rename(filepath.Join(dataDir, name), filepath.Join(failedDir, name)); err != nil {
			return err
		}
	}
	return nil
}

func generateBackupName(auto bool) string {
	prefix := "@manual"
	if auto {
		prefix = "@auto"
	}
	return fmt.Sprintf("%s_flop_backup_%s.zip", prefix, time.Now().UTC().Format("20060102150405"))
}

func generateUploadedBackupName(filename string) string {
	base := strings.TrimSpace(filepath.Base(filename))
	if base == "" || base == "." || base == string(filepath.Separator) {
		base = "backup.zip"
	}
	var builder strings.Builder
	for _, r := range base {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r):
			builder.WriteRune(r)
		case r == '.', r == '-', r == '_':
			builder.WriteRune(r)
		default:
			builder.WriteByte('_')
		}
	}
	safe := strings.Trim(builder.String(), "._")
	if safe == "" {
		safe = "backup"
	}
	if !strings.HasSuffix(strings.ToLower(safe), ".zip") {
		safe += ".zip"
	}
	return fmt.Sprintf("@upload_%s_%s", time.Now().UTC().Format("20060102150405"), safe)
}

func normalizeBackupSettings(settings BackupSettings) BackupSettings {
	settings.Cron = strings.TrimSpace(settings.Cron)
	if settings.CronMaxKeep < 0 {
		settings.CronMaxKeep = 0
	}
	settings.S3 = normalizeBackupS3Config(settings.S3)
	if !settings.S3.Enabled {
		settings.S3 = BackupS3Config{}
	}
	return settings
}

func normalizeBackupS3Config(cfg BackupS3Config) BackupS3Config {
	cfg.Bucket = strings.TrimSpace(cfg.Bucket)
	cfg.Region = strings.TrimSpace(cfg.Region)
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)
	cfg.AccessKey = strings.TrimSpace(cfg.AccessKey)
	cfg.Secret = strings.TrimSpace(cfg.Secret)
	return cfg
}

func validateBackupSettings(settings BackupSettings) error {
	if settings.Cron != "" {
		if _, err := cron.Parse(settings.Cron); err != nil {
			return err
		}
		if settings.CronMaxKeep < 1 {
			return fmt.Errorf("cronMaxKeep must be at least 1 when auto backups are enabled")
		}
	}
	if settings.S3.Enabled {
		if err := validateBackupS3Config(settings.S3); err != nil {
			return err
		}
	}
	return nil
}

func validateBackupS3Config(cfg BackupS3Config) error {
	if !cfg.Enabled {
		return nil
	}
	switch {
	case cfg.Endpoint == "":
		return fmt.Errorf("s3 endpoint is required")
	case cfg.Bucket == "":
		return fmt.Errorf("s3 bucket is required")
	case cfg.Region == "":
		return fmt.Errorf("s3 region is required")
	case cfg.AccessKey == "":
		return fmt.Errorf("s3 access key is required")
	case cfg.Secret == "":
		return fmt.Errorf("s3 secret is required")
	default:
		return nil
	}
}

func cloneBackupSettings(settings BackupSettings, maskSecret bool) BackupSettings {
	clone := settings
	if maskSecret && clone.S3.Secret != "" {
		clone.S3.Secret = backupSecretMask
	}
	return clone
}

func loadBackupSettings(dataDir string) (BackupSettings, error) {
	path := filepath.Join(dataDir, backupSettingsRelPath)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return BackupSettings{}, nil
		}
		return BackupSettings{}, err
	}
	var settings BackupSettings
	if err := json.Unmarshal(data, &settings); err != nil {
		return BackupSettings{}, err
	}
	return normalizeBackupSettings(settings), nil
}

func saveBackupSettings(dataDir string, settings BackupSettings) error {
	path := filepath.Join(dataDir, backupSettingsRelPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

type localBackupStorage struct {
	dir string
}

func (s *localBackupStorage) ensureDir() error {
	return os.MkdirAll(s.dir, 0o755)
}

func (s *localBackupStorage) List(_ context.Context) ([]AdminBackupFile, error) {
	if err := s.ensureDir(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	out := make([]AdminBackupFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		out = append(out, AdminBackupFile{
			Key:      entry.Name(),
			Size:     info.Size(),
			Modified: info.ModTime(),
		})
	}
	return out, nil
}

func (s *localBackupStorage) Save(_ context.Context, key, localPath string) error {
	startedAt := time.Now()
	log.Printf("flop backup upload: local save start key=%q path=%q", key, localPath)
	if err := s.ensureDir(); err != nil {
		log.Printf("flop backup upload: local save ensure dir failed key=%q err=%v", key, err)
		return err
	}
	src, err := os.Open(localPath)
	if err != nil {
		log.Printf("flop backup upload: local save open src failed key=%q err=%v", key, err)
		return err
	}
	defer src.Close()
	dstPath := filepath.Join(s.dir, key)
	dst, err := os.Create(dstPath)
	if err != nil {
		log.Printf("flop backup upload: local save create dst failed key=%q dst=%q err=%v", key, dstPath, err)
		return err
	}
	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		log.Printf("flop backup upload: local save copy failed key=%q dst=%q dur=%s err=%v", key, dstPath, time.Since(startedAt), err)
		return err
	}
	if err := dst.Close(); err != nil {
		log.Printf("flop backup upload: local save close failed key=%q dst=%q dur=%s err=%v", key, dstPath, time.Since(startedAt), err)
		return err
	}
	log.Printf("flop backup upload: local save complete key=%q dst=%q dur=%s", key, dstPath, time.Since(startedAt))
	return nil
}

func (s *localBackupStorage) Open(_ context.Context, key string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(s.dir, key))
}

func (s *localBackupStorage) Delete(_ context.Context, key string) error {
	return os.Remove(filepath.Join(s.dir, key))
}

func (s *localBackupStorage) Exists(_ context.Context, key string) (bool, error) {
	_, err := os.Stat(filepath.Join(s.dir, key))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (s *localBackupStorage) Stat(_ context.Context, key string) (AdminBackupFile, error) {
	info, err := os.Stat(filepath.Join(s.dir, key))
	if err != nil {
		return AdminBackupFile{}, err
	}
	return AdminBackupFile{
		Key:      key,
		Size:     info.Size(),
		Modified: info.ModTime(),
	}, nil
}

func (s *localBackupStorage) Test(_ context.Context) error {
	return s.ensureDir()
}

type s3BackupStorage struct {
	client         *awss3.Client
	pathClient     *awss3.Client
	bucket         string
	endpoint       string
	forcePathStyle bool
}

func newS3BackupStorage(cfg BackupS3Config) (*s3BackupStorage, error) {
	if err := validateBackupS3Config(cfg); err != nil {
		return nil, err
	}
	client := newS3Client(cfg, cfg.ForcePathStyle)
	var pathClient *awss3.Client
	if !cfg.ForcePathStyle {
		pathClient = newS3Client(cfg, true)
	}
	return &s3BackupStorage{
		client:         client,
		pathClient:     pathClient,
		bucket:         cfg.Bucket,
		endpoint:       cfg.Endpoint,
		forcePathStyle: cfg.ForcePathStyle,
	}, nil
}

func (s *s3BackupStorage) List(ctx context.Context) ([]AdminBackupFile, error) {
	var out []AdminBackupFile
	pager := awss3.NewListObjectsV2Paginator(s.client, &awss3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			if s.shouldRetryPathStyle(err) {
				return s.listWithClient(ctx, s.pathClient)
			}
			return nil, err
		}
		for _, obj := range page.Contents {
			out = append(out, AdminBackupFile{
				Key:      aws.ToString(obj.Key),
				Size:     aws.ToInt64(obj.Size),
				Modified: aws.ToTime(obj.LastModified),
			})
		}
	}
	return out, nil
}

func (s *s3BackupStorage) listWithClient(ctx context.Context, client *awss3.Client) ([]AdminBackupFile, error) {
	if client == nil {
		return nil, fmt.Errorf("s3 client unavailable")
	}
	var out []AdminBackupFile
	pager := awss3.NewListObjectsV2Paginator(client, &awss3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			out = append(out, AdminBackupFile{
				Key:      aws.ToString(obj.Key),
				Size:     aws.ToInt64(obj.Size),
				Modified: aws.ToTime(obj.LastModified),
			})
		}
	}
	return out, nil
}

func (s *s3BackupStorage) Save(ctx context.Context, key, localPath string) error {
	startedAt := time.Now()
	logUploadFileInfo("flop backup upload: s3 save source", localPath, key, 0)
	log.Printf("flop backup upload: s3 save start key=%q bucket=%q endpoint=%q pathStyle=%t", key, s.bucket, s.endpoint, s.forcePathStyle)
	file, err := os.Open(localPath)
	if err != nil {
		log.Printf("flop backup upload: s3 save open src failed key=%q err=%v", key, err)
		return err
	}
	defer file.Close()
	_, err = s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        file,
		ContentType: aws.String("application/zip"),
	})
	if err != nil && s.shouldRetryPathStyle(err) {
		log.Printf("flop backup upload: s3 save retry with path-style key=%q err=%v", key, err)
		if _, retryErr := file.Seek(0, io.SeekStart); retryErr == nil {
			_, err = s.pathClient.PutObject(ctx, &awss3.PutObjectInput{
				Bucket:      aws.String(s.bucket),
				Key:         aws.String(key),
				Body:        file,
				ContentType: aws.String("application/zip"),
			})
		} else {
			log.Printf("flop backup upload: s3 save seek retry failed key=%q err=%v", key, retryErr)
		}
	}
	if err != nil {
		log.Printf("flop backup upload: s3 save failed key=%q dur=%s err=%v", key, time.Since(startedAt), err)
		return err
	}
	log.Printf("flop backup upload: s3 save complete key=%q dur=%s", key, time.Since(startedAt))
	return err
}

func (s *s3BackupStorage) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if s.shouldRetryPathStyle(err) {
			out, err = s.pathClient.GetObject(ctx, &awss3.GetObjectInput{
				Bucket: aws.String(s.bucket),
				Key:    aws.String(key),
			})
		}
	}
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

func (s *s3BackupStorage) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil && s.shouldRetryPathStyle(err) {
		_, err = s.pathClient.DeleteObject(ctx, &awss3.DeleteObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
	}
	return err
}

func (s *s3BackupStorage) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil && s.shouldRetryPathStyle(err) {
		_, err = s.pathClient.HeadObject(ctx, &awss3.HeadObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
	}
	if err == nil {
		return true, nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey") {
		return false, nil
	}
	return false, err
}

func (s *s3BackupStorage) Stat(ctx context.Context, key string) (AdminBackupFile, error) {
	out, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil && s.shouldRetryPathStyle(err) {
		out, err = s.pathClient.HeadObject(ctx, &awss3.HeadObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
	}
	if err != nil {
		return AdminBackupFile{}, err
	}
	return AdminBackupFile{
		Key:      key,
		Size:     aws.ToInt64(out.ContentLength),
		Modified: aws.ToTime(out.LastModified),
	}, nil
}

func (s *s3BackupStorage) Test(ctx context.Context) error {
	_, err := s.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucket),
		MaxKeys: aws.Int32(1),
	})
	if err != nil && s.shouldRetryPathStyle(err) {
		_, err = s.pathClient.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket:  aws.String(s.bucket),
			MaxKeys: aws.Int32(1),
		})
	}
	return err
}

func (s *s3BackupStorage) shouldRetryPathStyle(err error) bool {
	if s == nil || s.pathClient == nil || s.forcePathStyle {
		return false
	}
	if !strings.Contains(strings.ToLower(s.endpoint), ".r2.cloudflarestorage.com") {
		return false
	}
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "SignatureDoesNotMatch"
}

func newS3Client(cfg BackupS3Config, forcePathStyle bool) *awss3.Client {
	awsCfg := aws.Config{
		Region:      cfg.Region,
		Credentials: credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.Secret, ""),
		HTTPClient:  &http.Client{Timeout: 30 * time.Second},
	}
	return awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		o.UsePathStyle = forcePathStyle
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})
}
