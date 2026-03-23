package flop

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/marcisbee/flop/internal/jsonx"
)

func TestBuiltInSuperadminSetupLoginAndAdminVisibility(t *testing.T) {
	app := New(Config{DataDir: t.TempDir(), SyncMode: "normal"})

	db, err := app.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if db.Table(systemSuperadminTableName) == nil {
		t.Fatalf("expected %s table to be available", systemSuperadminTableName)
	}
	if db.superadminService == nil {
		t.Fatal("expected built-in superadmin service")
	}
	if db.authService != nil {
		t.Fatal("did not expect app auth service when no auth table is defined")
	}
	for _, name := range db.tableNames {
		if name == systemSuperadminTableName {
			t.Fatalf("did not expect %s in public tableNames", systemSuperadminTableName)
		}
	}

	provider := &EngineAdminProvider{DB: db}
	tables, err := provider.AdminTables()
	if err != nil {
		t.Fatalf("admin tables: %v", err)
	}
	foundSystemTable := false
	for _, table := range tables {
		if table.Name == systemSuperadminTableName {
			foundSystemTable = true
			if !strings.Contains(string(table.Schema), "\"password\"") {
				t.Fatalf("expected %s schema to include password field", systemSuperadminTableName)
			}
		}
	}
	if !foundSystemTable {
		t.Fatalf("expected %s in admin table list", systemSuperadminTableName)
	}

	mux := http.NewServeMux()
	cfg := MountDefaultAdmin(mux, provider)
	if cfg == nil || cfg.SetupToken == "" {
		t.Fatal("expected setup token when no superadmin exists")
	}

	setupBody, _ := jsonx.Marshal(map[string]any{
		"token":    cfg.SetupToken,
		"email":    "Admin@Example.com",
		"password": "secret123",
	})
	setupReq := httptest.NewRequest(http.MethodPost, "/_/api/setup", bytes.NewReader(setupBody))
	setupReq.Header.Set("Content-Type", "application/json")
	setupResp := httptest.NewRecorder()
	mux.ServeHTTP(setupResp, setupReq)
	if setupResp.Code != http.StatusOK {
		t.Fatalf("setup status=%d body=%s", setupResp.Code, setupResp.Body.String())
	}

	if !provider.AdminHasSuperadmin() {
		t.Fatal("expected superadmin to exist after setup")
	}

	loginBody, _ := jsonx.Marshal(map[string]any{
		"email":    "admin@example.com",
		"password": "secret123",
	})
	loginReq := httptest.NewRequest(http.MethodPost, "/_/api/login", bytes.NewReader(loginBody))
	loginReq.Header.Set("Content-Type", "application/json")
	loginResp := httptest.NewRecorder()
	mux.ServeHTTP(loginResp, loginReq)
	if loginResp.Code != http.StatusOK {
		t.Fatalf("login status=%d body=%s", loginResp.Code, loginResp.Body.String())
	}

	rowsPage, found, err := provider.AdminRows(systemSuperadminTableName, 10, 0)
	if err != nil {
		t.Fatalf("admin rows: %v", err)
	}
	if !found {
		t.Fatalf("expected %s rows to be found", systemSuperadminTableName)
	}
	if rowsPage.Total != 1 || len(rowsPage.Rows) != 1 {
		t.Fatalf("expected one superadmin row, got total=%d len=%d", rowsPage.Total, len(rowsPage.Rows))
	}
	if rowsPage.Rows[0]["email"] != "admin@example.com" {
		t.Fatalf("expected normalized email, got %#v", rowsPage.Rows[0]["email"])
	}
	if rowsPage.Rows[0]["password"] != "[REDACTED]" {
		t.Fatalf("expected redacted password, got %#v", rowsPage.Rows[0]["password"])
	}
}
