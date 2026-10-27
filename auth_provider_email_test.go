package flop

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestProviderSignupEmailContinuation(t *testing.T) {
	smtp := newTestSMTPServer(t, false)
	adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{"signup": {Provider: "fake", Issuer: "https://issuer.example", Subject: "new"}}}
	app, db, handler := providerTestApp(t, map[string]AuthProviderConfig{"fake": fakeProviderConfig(adapter, "fake", "https://issuer.example")})
	if _, err := db.updateEmailSettings(defaultEmailSettings(smtp.config("https://app.example"))); err != nil {
		t.Fatal(err)
	}
	state, _ := startProviderFlow(t, handler, "fake", "sign_up", "", "")
	code := callbackProviderFlow(t, handler, "fake", state, "signup")
	response := signupEmailRequest(t, handler, code, "new@example.com")
	token := signupEmailToken(t, smtp)
	if strings.Contains(response, token) {
		t.Fatal("token disclosed in HTTP response")
	}
	if db.db.GetAuthTable().Count() != 0 || db.Table(systemAuthIdentityTableName).Count() != 0 || db.Table(systemAuthSessionTableName).Count() != 0 {
		t.Fatal("email request created account state")
	}
	// A pending challenge survives restart without persisting its plaintext token.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	db, err = app.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	handler = app.APIHandler(db)
	body, _ := json.Marshal(map[string]any{"completionCode": code, "emailVerificationToken": token})
	rec := providerRequest(t, handler, "POST", "/api/auth/provider/complete", string(body), "")
	if rec.Code != 200 {
		t.Fatalf("complete: %d %s", rec.Code, rec.Body.String())
	}
	out := decodeProviderResponse(t, rec)
	if _, err := db.ValidateAccessToken(toString(out["token"])); err != nil {
		t.Fatal(err)
	}
	id := toString(decodeJWTClaims(t, toString(out["token"]))["sub"])
	user, _ := db.Table("users").Get(id)
	if user["email"] != "new@example.com" || !providerBool(user["verified"]) {
		t.Fatalf("account email not verified: %v", user)
	}
	identity := out["identity"].(map[string]any)
	if providerBool(identity["emailVerified"]) || toString(identity["email"]) != "" {
		t.Fatal("application email falsely attributed to provider")
	}
	if replay := providerRequest(t, handler, "POST", "/api/auth/provider/complete", string(body), ""); replay.Code != 410 {
		t.Fatalf("replay: %d", replay.Code)
	}
}

func signupEmailRequest(t *testing.T, handler http.Handler, code, email string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"completionCode": code, "email": email})
	rec := providerRequest(t, handler, "POST", "/api/auth/provider/signup-email", string(body), "")
	if rec.Code != 202 {
		t.Fatalf("email request: %d %s", rec.Code, rec.Body.String())
	}
	return rec.Body.String()
}

func signupEmailToken(t *testing.T, smtp *testSMTPServer) string {
	t.Helper()
	message := smtp.nextMessage(t)
	match := regexp.MustCompile(`<code>([A-Za-z0-9_-]+)</code>`).FindStringSubmatch(message)
	if len(match) != 2 {
		t.Fatalf("missing signup token in mail: %s", message)
	}
	return strings.TrimSpace(match[1])
}

func TestProviderSignupEmailGuards(t *testing.T) {
	for _, scenario := range []string{"wrong token", "another flow", "another browser", "expired", "resend", "email collision", "profile retry", "SMTP failure", "sign in", "already verified"} {
		t.Run(scenario, func(t *testing.T) {
			smtp := newTestSMTPServer(t, scenario == "SMTP failure")
			identity := AuthProviderIdentity{Provider: "fake", Issuer: "https://issuer.example", Subject: "new", Email: "untrusted@example.com"}
			if scenario == "already verified" {
				identity.EmailVerified = true
			}
			adapter := &fakeAuthProvider{identities: map[string]AuthProviderIdentity{"signup": identity}}
			app, db, handler := providerTestApp(t, map[string]AuthProviderConfig{"fake": fakeProviderConfig(adapter, "fake", "https://issuer.example")})
			if _, err := db.updateEmailSettings(defaultEmailSettings(smtp.config("https://app.example"))); err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			db.providerAuth.now = func() time.Time { return now }
			email := "chosen@example.com"
			if scenario == "email collision" {
				registerProviderTestUser(t, handler, email)
			}
			accounts, identities, sessions := db.db.GetAuthTable().Count(), db.Table(systemAuthIdentityTableName).Count(), db.Table(systemAuthSessionTableName).Count()
			intent := "sign_up"
			if scenario == "sign in" {
				intent = "sign_in"
			}
			start := func() string {
				state, _ := startProviderFlow(t, handler, "fake", intent, "", "")
				return callbackProviderFlow(t, handler, "fake", state, "signup")
			}
			code := start()
			requestBody := func() string {
				b, _ := json.Marshal(map[string]any{"completionCode": code, "email": email})
				return string(b)
			}
			request := func() *httptest.ResponseRecorder {
				return providerRequest(t, handler, "POST", "/api/auth/provider/signup-email", requestBody(), "")
			}
			if scenario == "sign in" || scenario == "already verified" || scenario == "SMTP failure" {
				rec := request()
				want := 400
				if scenario == "SMTP failure" {
					want = 502
				}
				if rec.Code != want {
					t.Fatalf("request: %d %s", rec.Code, rec.Body.String())
				}
				if scenario == "SMTP failure" {
					if rec := completeProviderFlow(t, handler, code, "", false); decodeProviderResponse(t, rec)["code"] != "verified_email_required" {
						t.Fatal(rec.Body.String())
					}
				}
				if accounts != db.db.GetAuthTable().Count() || identities != db.Table(systemAuthIdentityTableName).Count() || sessions != db.Table(systemAuthSessionTableName).Count() {
					t.Fatal("rejected request changed account state")
				}
				return
			}
			if scenario == "another browser" {
				req := httptest.NewRequest("POST", "/api/auth/provider/signup-email", strings.NewReader(requestBody()))
				req.AddCookie(&http.Cookie{Name: "__Host-flop-provider", Value: strings.Repeat("x", 43)})
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				if rec.Code != 403 {
					t.Fatalf("foreign request: %d %s", rec.Code, rec.Body.String())
				}
			}
			signupEmailRequest(t, handler, code, email)
			token := signupEmailToken(t, smtp)
			expected := "email_verification_invalid"
			switch scenario {
			case "wrong token":
				token = "wrong"
			case "another flow":
				code = start()
			case "another browser":
				expected = "provider_browser_mismatch"
			case "expired":
				now = now.Add(providerCompletionTTL)
				expected = "completion_expired"
				if rec := request(); rec.Code != 410 {
					t.Fatalf("expired email request: %d", rec.Code)
				}
			case "resend":
				if rec := request(); rec.Code != 429 {
					t.Fatalf("resend throttle: %d", rec.Code)
				}
				now = now.Add(time.Minute)
				email = "replacement@example.com"
				signupEmailRequest(t, handler, code, email)
			case "email collision":
				expected = "link_required"
			case "profile retry":
				expected = "invalid_signup_fields"
				app.config.ProviderSignup = func(req AuthProviderSignupRequest) (map[string]any, error) {
					if req.Email != email || req.Identity.Email != "untrusted@example.com" || req.Identity.EmailVerified {
						t.Fatal("verified account email or provider claims incorrect")
					}
					if req.Fields["name"] != "Chosen" {
						return nil, fmt.Errorf("name required")
					}
					return map[string]any{"name": "Chosen"}, nil
				}
			}
			complete := func(token string, fields map[string]any, foreign bool) *httptest.ResponseRecorder {
				body, _ := json.Marshal(map[string]any{"completionCode": code, "emailVerificationToken": token, "signupFields": fields})
				if !foreign {
					return providerRequest(t, handler, "POST", "/api/auth/provider/complete", string(body), "")
				}
				req := httptest.NewRequest("POST", "/api/auth/provider/complete", strings.NewReader(string(body)))
				req.AddCookie(&http.Cookie{Name: "__Host-flop-provider", Value: strings.Repeat("x", 43)})
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				return rec
			}
			rec := complete(token, nil, scenario == "another browser")
			if rec.Code < 400 || decodeProviderResponse(t, rec)["code"] != expected {
				t.Fatalf("guard: %d %s; want %s", rec.Code, rec.Body.String(), expected)
			}
			if accounts != db.db.GetAuthTable().Count() || identities != db.Table(systemAuthIdentityTableName).Count() || sessions != db.Table(systemAuthSessionTableName).Count() {
				t.Fatal("guard failure mutated account state")
			}
			if scenario == "resend" || scenario == "profile retry" || scenario == "another browser" {
				if scenario == "resend" {
					token = signupEmailToken(t, smtp)
				}
				rec := complete(token, map[string]any(nil), false)
				if scenario == "profile retry" {
					rec = complete(token, map[string]any{"name": "Chosen"}, false)
				}
				if rec.Code != 200 {
					t.Fatalf("corrected completion: %d %s", rec.Code, rec.Body.String())
				}
				id := toString(decodeJWTClaims(t, toString(decodeProviderResponse(t, rec)["token"]))["sub"])
				user, _ := db.Table("users").Get(id)
				if user["email"] != email {
					t.Fatalf("wrong email: %v", user["email"])
				}
			}
		})
	}
}
