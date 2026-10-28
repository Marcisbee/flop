package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDemoLoginExposure(t *testing.T) {
	for _, tc := range []struct {
		name, flag, email, password, method, path string
		status                                    int
		exposed                                   bool
	}{
		{"default off", "", "demo@test", "demo", "GET", "/_/login", 200, false},
		{"missing password", "1", "demo@test", "", "GET", "/_/login", 200, false},
		{"enabled", "1", "demo@test", "demo", "GET", "/_/login", 200, true},
		{"trailing slash", "1", "demo@test", "demo", "GET", "/_/login/", 200, true},
		{"other page", "1", "demo@test", "demo", "GET", "/_/setup", 200, false},
		{"post", "1", "demo@test", "demo", "POST", "/_/login", 405, false},
		{"redirect", "1", "demo@test", "demo", "GET", "/_/login", 302, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("FLOP_DEMO_LOGIN", tc.flag)
			t.Setenv("FLOP_DEMO_EMAIL", tc.email)
			t.Setenv("FLOP_DEMO_PASSWORD", tc.password)
			handler := withDemoLogin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(tc.status)
				w.Write([]byte("<html><body>Login</body></html>"))
			}))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(tc.method, tc.path, nil))
			if response.Code != tc.status {
				t.Fatalf("status = %d", response.Code)
			}
			exposed := strings.Contains(response.Body.String(), "demo@test")
			if exposed != tc.exposed {
				t.Fatalf("demo credentials exposed = %v, want %v", exposed, tc.exposed)
			}
		})
	}
}
