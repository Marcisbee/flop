package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"strings"
)

// Demo credentials are intentionally public when this preview-only mode is enabled.
// Normal example runs and the Flop library do not expose this shortcut.
func withDemoLogin(next http.Handler) http.Handler {
	email, password := os.Getenv("FLOP_DEMO_EMAIL"), os.Getenv("FLOP_DEMO_PASSWORD")
	if os.Getenv("FLOP_DEMO_LOGIN") != "1" || email == "" || password == "" {
		return next
	}
	credentials, _ := json.Marshal(map[string]string{"email": email, "password": password})
	script := []byte(`<script>
(() => {
 const form = document.getElementById('login-form');
 if (!form) return;
 const box = document.createElement('div');
 box.style.cssText = 'margin-bottom:24px;padding-bottom:24px;border-bottom:1px solid var(--border)';
 const note = document.createElement('p');
 note.textContent = 'Development preview · Sample data';
 note.style.cssText = 'margin-bottom:10px;color:var(--text-muted);font-size:14px';
 const button = document.createElement('button');
 button.type = 'button';
 button.textContent = 'Log in to demo';
 button.style.cssText = 'width:100%;min-height:44px;padding:10px 16px;border:1px solid var(--border-input);border-radius:9px;background:var(--bg-input);color:var(--text-heading);font:inherit;font-size:16px;cursor:pointer';
 box.append(note, button);
 form.before(box);
 button.addEventListener('click', async () => {
  const error = document.getElementById('error');
  const submit = form.querySelector('button[type=submit]');
  error.classList.add('hidden');
  button.disabled = true;
  submit.disabled = true;
  button.textContent = 'Signing in…';
  try {
   const response = await fetch('/_/api/login', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body:JSON.stringify(` + string(credentials) + `)
   });
   const data = await response.json();
   if (!response.ok) throw new Error(data.error || 'Demo login failed');
   localStorage.setItem('flop_admin_token', data.token);
   if (data.refreshToken) localStorage.setItem('flop_admin_refresh', data.refreshToken);
   window.location.href = '/_';
  } catch (err) {
   error.textContent = err.message;
   error.classList.remove('hidden');
   button.disabled = false;
   submit.disabled = false;
   button.textContent = 'Log in to demo';
  }
 });
})();
</script></body>`)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || strings.TrimSuffix(r.URL.Path, "/") != "/_/login" {
			next.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(&demoLoginWriter{ResponseWriter: w, script: script}, r)
	})
}

type demoLoginWriter struct {
	http.ResponseWriter
	script []byte
	status int
}

func (w *demoLoginWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *demoLoginWriter) Write(body []byte) (int, error) {
	if (w.status == 0 || w.status == http.StatusOK) && strings.HasPrefix(w.Header().Get("Content-Type"), "text/html") && bytes.Contains(body, []byte("</body>")) {
		w.Header().Del("Content-Length")
		modified := bytes.Replace(body, []byte("</body>"), w.script, 1)
		_, err := w.ResponseWriter.Write(modified)
		if err != nil {
			return 0, err
		}
		return len(body), nil
	}
	return w.ResponseWriter.Write(body)
}
