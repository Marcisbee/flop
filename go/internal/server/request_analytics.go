package server

import (
	"bufio"
	"fmt"
	"github.com/marcisbee/flop/internal/jsonx"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultRequestLogRetention = 7 * 24 * time.Hour
	requestLogQueueSize        = 8192
	requestMaxDetailBytes      = 2048
	requestMaxErrorBytes       = 512
)

var latencyUpperBoundsMs = [...]float64{
	1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000,
}

type AnalyticsEvent struct {
	Timestamp    time.Time
	RouteType    string
	RouteName    string
	Method       string
	Path         string
	Transport    string
	Duration     time.Duration
	OK           bool
	StatusCode   int
	ErrorMessage string
	UserID       string
	Details      map[string]interface{}
}

type requestLogRecord struct {
	Ts           int64
	ID           string
	Time         string
	RouteType    string
	RouteName    string
	Method       string
	Path         string
	Transport    string
	Status       string
	OK           bool
	StatusCode   int
	DurationMs   float64
	ErrorMessage string
	Details      string
	UserID       string
}

type requestLogDiskRecord struct {
	Ts  int64                  `json:"Ts"`
	Row map[string]interface{} `json:"Row"`
}

type requestMinuteBucket struct {
	MinuteTs     int64
	RouteType    string
	RouteName    string
	Count        int
	ErrorCount   int
	TotalMs      float64
	LatencyCount [len(latencyUpperBoundsMs) + 1]int
}

type requestAgg struct {
	Count      int
	ErrorCount int
	TotalMs    float64
	Latency    [len(latencyUpperBoundsMs) + 1]int
}

type analyticsPoint struct {
	Ts         int64   `json:"ts"`
	Count      int     `json:"count"`
	ErrorCount int     `json:"errorCount"`
	AvgMs      float64 `json:"avgMs"`
	P95Ms      float64 `json:"p95Ms"`
}

type analyticsRouteSummary struct {
	RouteType  string  `json:"routeType"`
	RouteName  string  `json:"routeName"`
	Count      int     `json:"count"`
	ErrorCount int     `json:"errorCount"`
	ErrorRate  float64 `json:"errorRate"`
	AvgMs      float64 `json:"avgMs"`
	P95Ms      float64 `json:"p95Ms"`
}

type analyticsSummary struct {
	Count      int     `json:"count"`
	ErrorCount int     `json:"errorCount"`
	ErrorRate  float64 `json:"errorRate"`
	AvgMs      float64 `json:"avgMs"`
	P95Ms      float64 `json:"p95Ms"`
}

type AnalyticsSeries struct {
	Points         []analyticsPoint        `json:"points"`
	Summary        analyticsSummary        `json:"summary"`
	Routes         []analyticsRouteSummary `json:"routes"`
	DroppedEvents  uint64                  `json:"droppedEvents"`
	RetentionHours float64                 `json:"retentionHours"`
}

type RequestAnalytics struct {
	mu        sync.RWMutex
	retention time.Duration
	logs      []requestLogRecord
	metrics   map[string]*requestMinuteBucket
	lastPrune int64
	storage   string
	appendf   *os.File
	seq       atomic.Uint64
	dropped   atomic.Uint64
	queue     chan AnalyticsEvent
}

func NewRequestAnalytics(retention time.Duration) *RequestAnalytics {
	return NewRequestAnalyticsWithStorage(retention, "")
}

func NewRequestAnalyticsWithStorage(retention time.Duration, storagePath string) *RequestAnalytics {
	if retention <= 0 {
		retention = DefaultRequestLogRetention
	}
	ra := &RequestAnalytics{
		retention: retention,
		logs:      make([]requestLogRecord, 0, 1024),
		metrics:   make(map[string]*requestMinuteBucket),
		lastPrune: time.Now().UnixMilli(),
		queue:     make(chan AnalyticsEvent, requestLogQueueSize),
	}
	ra.initStorage(storagePath)
	go ra.loop()
	return ra
}

func (ra *RequestAnalytics) initStorage(storagePath string) {
	storagePath = strings.TrimSpace(storagePath)
	if storagePath == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(storagePath), 0755); err != nil {
		return
	}
	ra.storage = storagePath
	ra.loadFromDiskLocked()
	if err := ra.compactLocked(); err != nil {
		// Continue without persistence if rewrite fails.
		ra.storage = ""
		return
	}
}

func (ra *RequestAnalytics) loadFromDiskLocked() {
	f, err := os.Open(ra.storage)
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	cutoff := time.Now().Add(-ra.retention).UnixMilli()
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var diskRec requestLogDiskRecord
		if err := jsonx.Unmarshal([]byte(line), &diskRec); err != nil {
			continue
		}
		if diskRec.Ts <= 0 {
			diskRec.Ts = int64(toFloatValue(diskRec.Row["ts"]))
		}
		if diskRec.Ts <= 0 || diskRec.Row == nil || diskRec.Ts < cutoff {
			continue
		}
		rec := recordFromRow(diskRec.Ts, diskRec.Row)
		ra.logs = append(ra.logs, rec)
		ra.addMetricsFromRecordLocked(rec)
	}
	ra.seq.Store(uint64(len(ra.logs)))
}

func (ra *RequestAnalytics) loop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case event := <-ra.queue:
			ra.apply(event)
		case <-ticker.C:
			ra.mu.Lock()
			changed := ra.pruneLocked(time.Now())
			if changed {
				_ = ra.compactLocked()
			}
			ra.lastPrune = time.Now().UnixMilli()
			ra.mu.Unlock()
		}
	}
}

func (ra *RequestAnalytics) Record(event AnalyticsEvent) {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	select {
	case ra.queue <- event:
	default:
		ra.dropped.Add(1)
	}
}

func (ra *RequestAnalytics) Retention() time.Duration {
	ra.mu.RLock()
	defer ra.mu.RUnlock()
	return ra.retention
}

func (ra *RequestAnalytics) SetRetention(retention time.Duration) {
	if retention < time.Hour {
		retention = time.Hour
	}
	ra.mu.Lock()
	ra.retention = retention
	if ra.pruneLocked(time.Now()) {
		_ = ra.compactLocked()
	}
	ra.lastPrune = time.Now().UnixMilli()
	ra.mu.Unlock()
}

func (ra *RequestAnalytics) DroppedEvents() uint64 {
	return ra.dropped.Load()
}

func (ra *RequestAnalytics) QueryLogs(page, limit int, search, filterExpr string) ([]map[string]interface{}, int, error) {
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}

	var matchFn func(map[string]interface{}) bool
	var err error
	if strings.TrimSpace(filterExpr) != "" {
		matchFn, err = ParseAndEvalFilter(filterExpr)
		if err != nil {
			return nil, 0, err
		}
	}
	search = strings.ToLower(strings.TrimSpace(search))
	offset := (page - 1) * limit

	ra.mu.RLock()
	logs := make([]requestLogRecord, len(ra.logs))
	copy(logs, ra.logs)
	ra.mu.RUnlock()

	total := 0
	rows := make([]map[string]interface{}, 0, limit)
	for i := len(logs) - 1; i >= 0; i-- {
		rec := logs[i]
		if search != "" && !matchesSearchRecord(rec, search) {
			continue
		}
		var row map[string]interface{}
		if matchFn != nil {
			row = rec.toRow()
			if !matchFn(row) {
				continue
			}
		}
		total++
		if total <= offset || len(rows) >= limit {
			continue
		}
		if row == nil {
			row = rec.toRow()
		}
		rows = append(rows, row)
	}
	return rows, total, nil
}

func (ra *RequestAnalytics) QuerySeries(window time.Duration, routeType, routeName string) AnalyticsSeries {
	if window <= 0 {
		window = 24 * time.Hour
	}

	now := time.Now()
	endMinute := floorMinuteUnixMilli(now.UnixMilli())
	startMinute := endMinute - int64(window/time.Minute)*int64(time.Minute/time.Millisecond)
	if startMinute > endMinute {
		startMinute = endMinute
	}

	ra.mu.RLock()
	retention := ra.retention
	dropped := ra.dropped.Load()
	buckets := make([]*requestMinuteBucket, 0, len(ra.metrics))
	for _, b := range ra.metrics {
		buckets = append(buckets, b)
	}
	ra.mu.RUnlock()

	perMinute := make(map[int64]*requestAgg)
	perRoute := make(map[string]*requestAgg)
	summary := &requestAgg{}

	for _, b := range buckets {
		if b.MinuteTs < startMinute || b.MinuteTs > endMinute {
			continue
		}
		if routeType != "" && b.RouteType != routeType {
			continue
		}
		if routeName != "" && b.RouteName != routeName {
			continue
		}

		pm := perMinute[b.MinuteTs]
		if pm == nil {
			pm = &requestAgg{}
			perMinute[b.MinuteTs] = pm
		}
		addBucket(pm, b)
		addBucket(summary, b)

		rk := b.RouteType + "|" + b.RouteName
		pr := perRoute[rk]
		if pr == nil {
			pr = &requestAgg{}
			perRoute[rk] = pr
		}
		addBucket(pr, b)
	}

	points := make([]analyticsPoint, 0, int((endMinute-startMinute)/(int64(time.Minute/time.Millisecond)))+1)
	for ts := startMinute; ts <= endMinute; ts += int64(time.Minute / time.Millisecond) {
		agg := perMinute[ts]
		if agg == nil {
			points = append(points, analyticsPoint{Ts: ts})
			continue
		}
		points = append(points, analyticsPoint{
			Ts:         ts,
			Count:      agg.Count,
			ErrorCount: agg.ErrorCount,
			AvgMs:      averageMs(agg.Count, agg.TotalMs),
			P95Ms:      p95FromLatency(agg.Latency, agg.Count),
		})
	}

	routes := make([]analyticsRouteSummary, 0, len(perRoute))
	for key, agg := range perRoute {
		parts := strings.SplitN(key, "|", 2)
		rt := ""
		rn := ""
		if len(parts) == 2 {
			rt = parts[0]
			rn = parts[1]
		}
		routes = append(routes, analyticsRouteSummary{
			RouteType:  rt,
			RouteName:  rn,
			Count:      agg.Count,
			ErrorCount: agg.ErrorCount,
			ErrorRate:  ratio(agg.ErrorCount, agg.Count),
			AvgMs:      averageMs(agg.Count, agg.TotalMs),
			P95Ms:      p95FromLatency(agg.Latency, agg.Count),
		})
	}
	sort.Slice(routes, func(i, j int) bool {
		if routes[i].Count == routes[j].Count {
			if routes[i].RouteType == routes[j].RouteType {
				return routes[i].RouteName < routes[j].RouteName
			}
			return routes[i].RouteType < routes[j].RouteType
		}
		return routes[i].Count > routes[j].Count
	})

	return AnalyticsSeries{
		Points: points,
		Summary: analyticsSummary{
			Count:      summary.Count,
			ErrorCount: summary.ErrorCount,
			ErrorRate:  ratio(summary.ErrorCount, summary.Count),
			AvgMs:      averageMs(summary.Count, summary.TotalMs),
			P95Ms:      p95FromLatency(summary.Latency, summary.Count),
		},
		Routes:         routes,
		DroppedEvents:  dropped,
		RetentionHours: retention.Hours(),
	}
}

func (ra *RequestAnalytics) apply(event AnalyticsEvent) {
	ts := event.Timestamp.UnixMilli()
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	durationMs := float64(event.Duration.Microseconds()) / 1000
	if durationMs < 0 {
		durationMs = 0
	}

	status := "success"
	if !event.OK {
		status = "error"
	}

	detailText := detailsToText(event.Details)
	errText := truncateText(strings.TrimSpace(event.ErrorMessage), requestMaxErrorBytes)
	rec := requestLogRecord{
		Ts:           ts,
		ID:           fmt.Sprintf("%d-%d", ts, ra.seq.Add(1)),
		Time:         time.UnixMilli(ts).UTC().Format(time.RFC3339Nano),
		RouteType:    event.RouteType,
		RouteName:    event.RouteName,
		Method:       event.Method,
		Path:         event.Path,
		Transport:    event.Transport,
		Status:       status,
		OK:           event.OK,
		StatusCode:   event.StatusCode,
		DurationMs:   durationMs,
		ErrorMessage: errText,
		Details:      detailText,
		UserID:       event.UserID,
	}

	ra.mu.Lock()
	ra.logs = append(ra.logs, rec)
	ra.addMetricsFromRecordLocked(rec)
	ra.appendRecordLocked(rec)

	const pruneIntervalMs = int64(time.Minute / time.Millisecond)
	if ts-ra.lastPrune >= pruneIntervalMs {
		if ra.pruneLocked(time.UnixMilli(ts)) {
			_ = ra.compactLocked()
		}
		ra.lastPrune = ts
	}
	ra.mu.Unlock()
}

func (ra *RequestAnalytics) pruneLocked(now time.Time) bool {
	cutoff := now.Add(-ra.retention).UnixMilli()
	if cutoff <= 0 {
		return false
	}

	changed := false
	if len(ra.logs) > 0 {
		dst := ra.logs[:0]
		for _, rec := range ra.logs {
			if rec.Ts >= cutoff {
				dst = append(dst, rec)
			}
		}
		if len(dst) != len(ra.logs) {
			changed = true
		}
		if len(dst) == 0 && len(ra.logs) > 0 {
			ra.logs = ra.logs[:0]
		} else {
			ra.logs = append([]requestLogRecord(nil), dst...)
		}
	}

	cutoffMinute := floorMinuteUnixMilli(cutoff)
	for k, b := range ra.metrics {
		if b.MinuteTs < cutoffMinute {
			delete(ra.metrics, k)
			changed = true
		}
	}
	return changed
}

func addBucket(agg *requestAgg, bucket *requestMinuteBucket) {
	agg.Count += bucket.Count
	agg.ErrorCount += bucket.ErrorCount
	agg.TotalMs += bucket.TotalMs
	for i := range agg.Latency {
		agg.Latency[i] += bucket.LatencyCount[i]
	}
}

func (ra *RequestAnalytics) addMetricsFromRecordLocked(rec requestLogRecord) {
	minuteTs := floorMinuteUnixMilli(rec.Ts)
	routeType := rec.RouteType
	if routeType == "" {
		routeType = "request"
	}
	routeName := rec.RouteName
	key := fmt.Sprintf("%d|%s|%s", minuteTs, routeType, routeName)

	b := ra.metrics[key]
	if b == nil {
		b = &requestMinuteBucket{
			MinuteTs:  minuteTs,
			RouteType: routeType,
			RouteName: routeName,
		}
		ra.metrics[key] = b
	}
	b.Count++

	if !rec.OK || strings.EqualFold(rec.Status, "error") {
		b.ErrorCount++
	}

	duration := rec.DurationMs
	if duration < 0 {
		duration = 0
	}
	b.TotalMs += duration
	b.LatencyCount[latencyBucket(duration)]++
}

func (ra *RequestAnalytics) appendRecordLocked(rec requestLogRecord) {
	if ra.storage == "" {
		return
	}
	if err := ra.ensureAppendFileLocked(); err != nil {
		return
	}
	payload, err := jsonx.Marshal(requestLogDiskRecord{Ts: rec.Ts, Row: rec.toRow()})
	if err != nil {
		return
	}
	payload = append(payload, '\n')
	if _, err := ra.appendf.Write(payload); err != nil {
		_ = ra.appendf.Close()
		ra.appendf = nil
	}
}

func (ra *RequestAnalytics) ensureAppendFileLocked() error {
	if ra.storage == "" {
		return fmt.Errorf("analytics storage disabled")
	}
	if ra.appendf != nil {
		return nil
	}
	f, err := os.OpenFile(ra.storage, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	ra.appendf = f
	return nil
}

func (ra *RequestAnalytics) compactLocked() error {
	if ra.storage == "" {
		return nil
	}
	tmp := ra.storage + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	for _, rec := range ra.logs {
		payload, err := jsonx.Marshal(requestLogDiskRecord{Ts: rec.Ts, Row: rec.toRow()})
		if err != nil {
			continue
		}
		payload = append(payload, '\n')
		if _, err := out.Write(payload); err != nil {
			_ = out.Close()
			_ = os.Remove(tmp)
			return err
		}
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if ra.appendf != nil {
		_ = ra.appendf.Close()
		ra.appendf = nil
	}
	if err := os.Rename(tmp, ra.storage); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return ra.ensureAppendFileLocked()
}

func matchesSearchRecord(rec requestLogRecord, search string) bool {
	fields := []string{
		rec.RouteType,
		rec.RouteName,
		rec.Method,
		rec.Status,
		rec.Path,
		rec.ErrorMessage,
		rec.Details,
		rec.UserID,
	}
	for _, f := range fields {
		if strings.Contains(strings.ToLower(f), search) {
			return true
		}
	}
	return false
}

func routeValue(v interface{}) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func recordFromRow(ts int64, row map[string]interface{}) requestLogRecord {
	return requestLogRecord{
		Ts:           ts,
		ID:           routeValue(row["id"]),
		Time:         routeValue(row["time"]),
		RouteType:    routeValue(row["routeType"]),
		RouteName:    routeValue(row["routeName"]),
		Method:       routeValue(row["method"]),
		Path:         routeValue(row["path"]),
		Transport:    routeValue(row["transport"]),
		Status:       routeValue(row["status"]),
		OK:           toBoolValue(row["ok"]),
		StatusCode:   int(toFloatValue(row["statusCode"])),
		DurationMs:   toFloatValue(row["durationMs"]),
		ErrorMessage: routeValue(row["errorMessage"]),
		Details:      routeValue(row["details"]),
		UserID:       routeValue(row["userId"]),
	}
}

func (r requestLogRecord) toRow() map[string]interface{} {
	return map[string]interface{}{
		"id":           r.ID,
		"ts":           r.Ts,
		"time":         r.Time,
		"routeType":    r.RouteType,
		"routeName":    r.RouteName,
		"method":       r.Method,
		"path":         r.Path,
		"transport":    r.Transport,
		"status":       r.Status,
		"ok":           r.OK,
		"statusCode":   r.StatusCode,
		"durationMs":   r.DurationMs,
		"errorMessage": r.ErrorMessage,
		"details":      r.Details,
		"userId":       r.UserID,
	}
}

func detailsToText(details map[string]interface{}) string {
	if len(details) == 0 {
		return ""
	}
	raw, err := jsonx.Marshal(details)
	if err != nil {
		return ""
	}
	return truncateText(string(raw), requestMaxDetailBytes)
}

func truncateText(v string, maxBytes int) string {
	if maxBytes <= 0 || len(v) <= maxBytes {
		return v
	}
	if maxBytes <= 3 {
		return v[:maxBytes]
	}
	return v[:maxBytes-3] + "..."
}

func floorMinuteUnixMilli(ts int64) int64 {
	const minuteMs = int64(time.Minute / time.Millisecond)
	if ts <= 0 {
		return 0
	}
	return (ts / minuteMs) * minuteMs
}

func latencyBucket(durationMs float64) int {
	for i, upper := range latencyUpperBoundsMs {
		if durationMs <= upper {
			return i
		}
	}
	return len(latencyUpperBoundsMs)
}

func p95FromLatency(latency [len(latencyUpperBoundsMs) + 1]int, total int) float64 {
	if total <= 0 {
		return 0
	}
	target := int(float64(total) * 0.95)
	if target <= 0 {
		target = 1
	}
	acc := 0
	for i, c := range latency {
		acc += c
		if acc >= target {
			if i < len(latencyUpperBoundsMs) {
				return latencyUpperBoundsMs[i]
			}
			return latencyUpperBoundsMs[len(latencyUpperBoundsMs)-1]
		}
	}
	return latencyUpperBoundsMs[len(latencyUpperBoundsMs)-1]
}

func averageMs(count int, total float64) float64 {
	if count <= 0 {
		return 0
	}
	return total / float64(count)
}

func ratio(num, den int) float64 {
	if den <= 0 {
		return 0
	}
	return (float64(num) / float64(den)) * 100
}

func toFloatValue(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case int32:
		return float64(n)
	case uint:
		return float64(n)
	case uint64:
		return float64(n)
	case uint32:
		return float64(n)
	case jsonx.Number:
		f, _ := n.Float64()
		return f
	case string:
		if f, err := strconv.ParseFloat(strings.TrimSpace(n), 64); err == nil {
			return f
		}
	}
	return 0
}

func toBoolValue(v interface{}) bool {
	switch b := v.(type) {
	case bool:
		return b
	case string:
		return strings.EqualFold(strings.TrimSpace(b), "true")
	}
	return false
}
