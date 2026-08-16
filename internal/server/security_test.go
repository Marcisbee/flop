package server

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A token verified under one secret must never be replayed from the
// verification cache under a different secret (e.g. two Database instances
// in one process).
func TestVerifyJWTCacheIsScopedBySecret(t *testing.T) {
	token := CreateJWT(&JWTPayload{
		Sub: "u1",
		Iat: time.Now().Unix(),
		Exp: time.Now().Add(time.Hour).Unix(),
	}, "secret-a")

	if payload := VerifyJWT(token, "secret-a"); payload == nil || payload.Sub != "u1" {
		t.Fatal("token did not verify under its own secret")
	}
	if payload := VerifyJWT(token, "secret-b"); payload != nil {
		t.Fatalf("token verified under a different secret from cache: %#v", payload)
	}
}

func TestRateLimiterFixedWindow(t *testing.T) {
	limiter := NewRateLimiter(3, time.Minute)
	for i := 0; i < 3; i++ {
		if !limiter.Allow("k") {
			t.Fatalf("attempt %d rejected within limit", i+1)
		}
	}
	if limiter.Allow("k") {
		t.Fatal("attempt beyond limit was allowed")
	}
	// A different key has its own bucket.
	if !limiter.Allow("other") {
		t.Fatal("unrelated key was throttled")
	}
}

// With TLS "auto" and credentials configured, a server without STARTTLS must
// not receive plaintext credentials.
func TestSMTPAutoTLSRefusesPlaintextCredentials(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer func() { _ = conn.Close() }()
				reader := bufio.NewReader(conn)
				writer := bufio.NewWriter(conn)
				write := func(s string) {
					_, _ = writer.WriteString(s + "\r\n")
					_ = writer.Flush()
				}
				write("220 test-smtp ESMTP")
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					cmd := strings.ToUpper(strings.TrimSpace(line))
					switch {
					case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
						// Deliberately no STARTTLS extension.
						write("250-test-smtp")
						write("250 OK")
					default:
						write("250 ok")
					}
				}
			}(conn)
		}
	}()

	port := listener.Addr().(*net.TCPAddr).Port
	_, _, err = newSMTPClient(SMTPConfig{
		Host:       "127.0.0.1",
		Port:       port,
		TLS:        "auto",
		Username:   "user",
		Password:   "secret",
		AuthMethod: "plain",
	})
	if err == nil || !strings.Contains(err.Error(), "STARTTLS") {
		t.Fatalf("expected STARTTLS refusal, got %v", err)
	}

	// Without credentials, plaintext relaying stays allowed.
	client, conn, err := newSMTPClient(SMTPConfig{
		Host:       "127.0.0.1",
		Port:       port,
		TLS:        "auto",
		AuthMethod: "none",
	})
	if err != nil {
		t.Fatalf("credential-free relay must still work: %v", err)
	}
	_ = client.Close()
	_ = conn.Close()
}

func TestClientIP(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "203.0.113.7:1234"
	if got := ClientIP(r); got != "203.0.113.7" {
		t.Fatalf("ClientIP = %q", got)
	}
	r.RemoteAddr = "203.0.113.9"
	if got := ClientIP(r); got != "203.0.113.9" {
		t.Fatalf("ClientIP without port = %q", got)
	}
}
