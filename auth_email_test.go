package flop

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAuthEmailRequestsDeliverWithoutDisclosingTokens(t *testing.T) {
	smtp := newTestSMTPServer(t, false)
	app, db := openAuthEmailTestDB(t, smtp.config("https://strike.example"), true)
	handler := app.APIHandler(db)

	authToken := registerAuthEmailTestUser(t, handler, "known@example.com")
	cases := []struct {
		name       string
		path       string
		body       string
		bearer     string
		wantStatus int
		wantURL    string
	}{
		{
			name:       "verification",
			path:       "/api/auth/request-verification",
			body:       `{}`,
			bearer:     authToken,
			wantStatus: http.StatusAccepted,
			wantURL:    "https://strike.example/verify?token=",
		},
		{
			name:       "email change",
			path:       "/api/auth/request-email-change",
			body:       `{"newEmail":"next@example.com","password":"password123"}`,
			bearer:     authToken,
			wantStatus: http.StatusAccepted,
			wantURL:    "https://strike.example/confirm-email-change?token=",
		},
		{
			name:       "password reset",
			path:       "/api/auth/request-password-reset",
			body:       `{"email":"known@example.com"}`,
			wantStatus: http.StatusAccepted,
			wantURL:    "https://strike.example/reset-password?token=",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := authEmailRequest(handler, tc.path, tc.body, tc.bearer)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			assertGenericAuthEmailResponse(t, rec.Body.String())
			message := smtp.nextMessage(t)
			if !strings.Contains(message, tc.wantURL) {
				t.Fatalf("email missing canonical action URL %q:\n%s", tc.wantURL, message)
			}
		})
	}
}

func TestPasswordResetResponseIsEnumerationSafeOnDeliveryFailure(t *testing.T) {
	smtp := newTestSMTPServer(t, true)
	app, db := openAuthEmailTestDB(t, smtp.config("https://strike.example"), true)
	handler := app.APIHandler(db)
	registerAuthEmailTestUser(t, handler, "known@example.com")

	known := authEmailRequest(
		handler,
		"/api/auth/request-password-reset",
		`{"email":"known@example.com"}`,
		"",
	)
	missing := authEmailRequest(
		handler,
		"/api/auth/request-password-reset",
		`{"email":"missing@example.com"}`,
		"",
	)
	if known.Code != http.StatusAccepted || missing.Code != http.StatusAccepted {
		t.Fatalf("known/missing statuses = %d/%d, want %d/%d", known.Code, missing.Code, http.StatusAccepted, http.StatusAccepted)
	}
	if known.Body.String() != missing.Body.String() {
		t.Fatalf("known and missing responses differ: %q != %q", known.Body.String(), missing.Body.String())
	}
	assertGenericAuthEmailResponse(t, known.Body.String())

	deadline := time.Now().Add(time.Second)
	for {
		rows, total, err := db.RequestAnalytics().QueryLogs(1, 20, "", "", "auth_email")
		if err != nil {
			t.Fatalf("query delivery analytics: %v", err)
		}
		if total > 0 {
			if got := fmt.Sprint(rows[0]["statusCode"]); got != "502" {
				t.Fatalf("delivery analytics status = %s, want 502; row=%#v", got, rows[0])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("SMTP delivery failure was not recorded in analytics")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAuthenticatedAuthEmailFailureIsGeneric(t *testing.T) {
	smtp := newTestSMTPServer(t, true)
	app, db := openAuthEmailTestDB(t, smtp.config("https://strike.example"), true)
	handler := app.APIHandler(db)
	authToken := registerAuthEmailTestUser(t, handler, "known@example.com")

	rec := authEmailRequest(handler, "/api/auth/request-verification", `{}`, authToken)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	if got := rec.Body.String(); got != "{\"error\":\"email delivery unavailable\"}\n" {
		t.Fatalf("unexpected public failure response: %q", got)
	}
}

func TestRequiredAuthEmailRejectsIncompleteDeployment(t *testing.T) {
	t.Run("SMTP disabled", func(t *testing.T) {
		app, _ := authEmailTestApp(Config{
			DataDir:          t.TempDir(),
			SyncMode:         "normal",
			RequireAuthEmail: true,
		})
		if db, err := app.Open(); err == nil {
			_ = db.Close()
			t.Fatal("Open succeeded without required SMTP")
		} else if !strings.Contains(err.Error(), "smtp is disabled") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("canonical URL missing", func(t *testing.T) {
		smtp := newTestSMTPServer(t, false)
		cfg := smtp.config("")
		app, _ := authEmailTestApp(Config{
			DataDir:          t.TempDir(),
			SyncMode:         "normal",
			SMTP:             cfg,
			RequireAuthEmail: true,
		})
		if db, err := app.Open(); err == nil {
			_ = db.Close()
			t.Fatal("Open succeeded without a canonical app URL")
		} else if !strings.Contains(err.Error(), "canonical app URL is required") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func openAuthEmailTestDB(t *testing.T, smtp *SMTPConfig, required bool) (*App, *Database) {
	t.Helper()
	app, _ := authEmailTestApp(Config{
		DataDir:          t.TempDir(),
		SyncMode:         "normal",
		SMTP:             smtp,
		RequireAuthEmail: required,
	})
	db, err := app.Open()
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return app, db
}

func authEmailTestApp(config Config) (*App, *Table[map[string]any]) {
	app := New(config)
	users := Define(app, "users", func(s *SchemaBuilder) {
		s.String("id").Primary("uuidv7")
		s.String("email").Required().Unique().Email()
		s.Bcrypt("password", 4).Required()
		s.Boolean("verified").Default(false)
		s.String("default_role").Default("unverified")
		s.Roles("roles")
	})
	return app, users
}

func registerAuthEmailTestUser(t *testing.T, handler http.Handler, email string) string {
	t.Helper()
	rec := authEmailRequest(
		handler,
		"/api/auth/register",
		fmt.Sprintf(`{"email":%q,"password":"password123"}`, email),
		"",
	)
	if rec.Code != http.StatusOK {
		t.Fatalf("register status = %d; body=%s", rec.Code, rec.Body.String())
	}
	var response struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode register response: %v", err)
	}
	if response.Token == "" {
		t.Fatal("register response missing auth token")
	}
	return response.Token
}

func authEmailRequest(handler http.Handler, path, body, bearer string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	handler.ServeHTTP(rec, req)
	return rec
}

func assertGenericAuthEmailResponse(t *testing.T, body string) {
	t.Helper()
	var response map[string]any
	if err := json.Unmarshal([]byte(body), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response) != 1 || response["ok"] != true {
		t.Fatalf("response is not the generic success payload: %#v", response)
	}
	if strings.Contains(strings.ToLower(body), "token") {
		t.Fatalf("response disclosed token material: %s", body)
	}
}

type testSMTPServer struct {
	listener net.Listener
	failRCPT bool
	messages chan string
	done     chan struct{}
	once     sync.Once
}

func newTestSMTPServer(t *testing.T, failRCPT bool) *testSMTPServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen SMTP: %v", err)
	}
	server := &testSMTPServer{
		listener: listener,
		failRCPT: failRCPT,
		messages: make(chan string, 10),
		done:     make(chan struct{}),
	}
	go server.serve()
	t.Cleanup(server.close)
	return server
}

func (s *testSMTPServer) config(appURL string) *SMTPConfig {
	port := s.listener.Addr().(*net.TCPAddr).Port
	return &SMTPConfig{
		AppName:       "Strike",
		AppURL:        appURL,
		Host:          "127.0.0.1",
		Port:          port,
		SenderAddress: "no-reply@strike.example",
		TLS:           "none",
		AuthMethod:    "none",
	}
}

func (s *testSMTPServer) nextMessage(t *testing.T) string {
	t.Helper()
	select {
	case message := <-s.messages:
		return message
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for SMTP message")
		return ""
	}
}

func (s *testSMTPServer) close() {
	s.once.Do(func() {
		close(s.done)
		_ = s.listener.Close()
	})
}

func (s *testSMTPServer) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *testSMTPServer) handle(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	writeSMTPLine(writer, "220 test-smtp ESMTP")

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		command := strings.ToUpper(strings.TrimSpace(line))
		switch {
		case strings.HasPrefix(command, "EHLO"), strings.HasPrefix(command, "HELO"):
			writeSMTPLine(writer, "250 test-smtp")
		case strings.HasPrefix(command, "MAIL FROM"):
			writeSMTPLine(writer, "250 ok")
		case strings.HasPrefix(command, "RCPT TO"):
			if s.failRCPT {
				writeSMTPLine(writer, "550 rejected")
			} else {
				writeSMTPLine(writer, "250 ok")
			}
		case command == "DATA":
			writeSMTPLine(writer, "354 end with <CRLF>.<CRLF>")
			var message strings.Builder
			for {
				dataLine, readErr := reader.ReadString('\n')
				if readErr != nil {
					return
				}
				if strings.TrimRight(dataLine, "\r\n") == "." {
					break
				}
				message.WriteString(dataLine)
			}
			s.messages <- message.String()
			writeSMTPLine(writer, "250 queued")
		case command == "QUIT":
			writeSMTPLine(writer, "221 bye")
			return
		default:
			writeSMTPLine(writer, "250 ok")
		}
	}
}

func writeSMTPLine(writer *bufio.Writer, line string) {
	_, _ = writer.WriteString(line + "\r\n")
	_ = writer.Flush()
}
