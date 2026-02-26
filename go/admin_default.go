package flop

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type AdminTable struct {
	Name     string `json:"name"`
	RowCount int    `json:"rowCount"`
}

type AdminRowsPage struct {
	Table  string           `json:"table"`
	Rows   []map[string]any `json:"rows"`
	Total  int              `json:"total"`
	Offset int              `json:"offset"`
	Limit  int              `json:"limit"`
}

type AdminProvider interface {
	AdminTables() ([]AdminTable, error)
	AdminRows(table string, limit, offset int) (AdminRowsPage, bool, error)
}

type AdminAuthProvider interface {
	AdminLogin(email, password string) (token, refreshToken string, err error)
	AdminRefresh(refreshToken string) (token string, err error)
	AdminIsAuthorized(token string) bool
}

func MountDefaultAdmin(mux *http.ServeMux, provider AdminProvider) {
	if mux == nil {
		panic("flop: admin mux is nil")
	}
	h := DefaultAdminHandler(provider)
	mux.Handle("/_", h)
	mux.Handle("/_/", h)
}

func DefaultAdminHandler(provider AdminProvider) http.Handler {
	authProvider, authEnabled := provider.(AdminAuthProvider)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		if authEnabled {
			switch path {
			case "/_/login", "/_/login/":
				if r.Method != http.MethodGet {
					http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
					return
				}
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				_, _ = w.Write([]byte(defaultAdminLoginHTML))
				return
			case "/_/api/login":
				if r.Method != http.MethodPost {
					jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
					return
				}
				var body struct {
					Email    string `json:"email"`
					Password string `json:"password"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					jsonError(w, "invalid json", http.StatusBadRequest)
					return
				}
				token, refresh, err := authProvider.AdminLogin(body.Email, body.Password)
				if err != nil {
					jsonError(w, err.Error(), http.StatusUnauthorized)
					return
				}
				jsonResp(w, http.StatusOK, map[string]any{
					"ok":           true,
					"token":        token,
					"refreshToken": refresh,
				})
				return
			case "/_/api/refresh":
				if r.Method != http.MethodPost {
					jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
					return
				}
				var body struct {
					RefreshToken string `json:"refreshToken"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					jsonError(w, "invalid json", http.StatusBadRequest)
					return
				}
				token, err := authProvider.AdminRefresh(body.RefreshToken)
				if err != nil {
					jsonError(w, err.Error(), http.StatusUnauthorized)
					return
				}
				jsonResp(w, http.StatusOK, map[string]any{"ok": true, "token": token})
				return
			}
		}

		if path == "/_" || path == "/_/" {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			if authEnabled {
				_, _ = w.Write([]byte(defaultAdminAuthedHTML))
			} else {
				_, _ = w.Write([]byte(defaultAdminHTML))
			}
			return
		}

		if path == "/_/api/tables" {
			if r.Method != http.MethodGet {
				jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if authEnabled && !isAuthorizedRequest(r, authProvider) {
				jsonError(w, "authentication required", http.StatusUnauthorized)
				return
			}
			tables, err := provider.AdminTables()
			if err != nil {
				jsonError(w, err.Error(), http.StatusBadRequest)
				return
			}
			jsonResp(w, http.StatusOK, map[string]any{
				"ok":     true,
				"tables": tables,
			})
			return
		}

		if strings.HasPrefix(path, "/_/api/tables/") {
			if r.Method != http.MethodGet {
				jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if authEnabled && !isAuthorizedRequest(r, authProvider) {
				jsonError(w, "authentication required", http.StatusUnauthorized)
				return
			}
			tableName, ok := parseRowsPath(path)
			if !ok {
				jsonError(w, "not found", http.StatusNotFound)
				return
			}
			limit := clampInt(parseIntOr(r.URL.Query().Get("limit"), 100), 1, 1000)
			offset := parseIntOr(r.URL.Query().Get("offset"), 0)
			if offset < 0 {
				offset = 0
			}
			page, found, err := provider.AdminRows(tableName, limit, offset)
			if err != nil {
				jsonError(w, err.Error(), http.StatusBadRequest)
				return
			}
			if !found {
				jsonError(w, "not found", http.StatusNotFound)
				return
			}
			jsonResp(w, http.StatusOK, map[string]any{"ok": true, "data": page})
			return
		}

		jsonError(w, "not found", http.StatusNotFound)
	})
}

func isAuthorizedRequest(r *http.Request, authProvider AdminAuthProvider) bool {
	token := extractBearerToken(r.Header.Get("Authorization"), r.URL.Query().Get("_token"))
	if token == "" {
		return false
	}
	return authProvider.AdminIsAuthorized(token)
}

func extractBearerToken(headerValue, queryToken string) string {
	h := strings.TrimSpace(headerValue)
	if h != "" {
		if strings.HasPrefix(strings.ToLower(h), "bearer ") {
			return strings.TrimSpace(h[7:])
		}
		return h
	}
	return strings.TrimSpace(queryToken)
}

func parseRowsPath(path string) (string, bool) {
	rest := strings.TrimPrefix(path, "/_/api/tables/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) != 2 || parts[1] != "rows" || parts[0] == "" {
		return "", false
	}
	name, err := url.PathUnescape(parts[0])
	if err != nil || name == "" {
		return "", false
	}
	return name, true
}

func parseIntOr(raw string, fallback int) int {
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return n
}

func clampInt(v, minV, maxV int) int {
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}

func jsonResp(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func jsonError(w http.ResponseWriter, message string, status int) {
	jsonResp(w, status, map[string]any{"error": message})
}

const defaultAdminCSS = `
      * { box-sizing: border-box; }
      body {
        margin: 0;
        font-family: "IBM Plex Sans", "Helvetica Neue", Arial, sans-serif;
        background: linear-gradient(140deg, #f4f9ff, #fef8f2);
        color: #162132;
      }
      header {
        padding: 22px;
        border-bottom: 1px solid #d6deea;
        background: #ffffffd9;
      }
      header h1 { margin: 0 0 6px; }
      header p { margin: 0; color: #4b5a70; }
      main {
        padding: 16px;
        display: grid;
        gap: 16px;
      }
      section {
        background: #fff;
        border: 1px solid #d6deea;
        border-radius: 12px;
        padding: 12px;
      }
      .cards {
        display: grid;
        gap: 10px;
        grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      }
      .card {
        border: 1px solid #c8d3e4;
        border-radius: 10px;
        padding: 10px;
        background: #f9fbff;
        cursor: pointer;
        text-align: left;
      }
      .card:hover { background: #eef4ff; }
      .muted { color: #6a7d98; }
      table {
        width: 100%;
        border-collapse: collapse;
        font-size: 14px;
      }
      th, td {
        border-bottom: 1px solid #d6deea;
        text-align: left;
        padding: 7px;
        vertical-align: top;
      }
      th { color: #4b5a70; font-weight: 600; }
      nav a, button.linkish {
        color: #0b4da2;
        text-decoration: none;
        font-weight: 600;
        background: none;
        border: 0;
        padding: 0;
        cursor: pointer;
      }
      .card-login {
        max-width: 380px;
        margin: 56px auto;
        background: #fff;
        border: 1px solid #d6deea;
        border-radius: 12px;
        padding: 16px;
      }
      .card-login h1 { margin: 0 0 10px; }
      .card-login input {
        width: 100%;
        border: 1px solid #c8d3e4;
        border-radius: 8px;
        padding: 10px;
        margin: 0 0 10px;
      }
      .card-login button {
        width: 100%;
        border: 1px solid #c8d3e4;
        border-radius: 8px;
        padding: 10px;
        background: #f9fbff;
        cursor: pointer;
      }
      .error { color: #b42318; }
`

const defaultAdminJS = `
      function esc(v) {
        if (v === null || v === undefined) return "<span class='muted'>null</span>";
        if (typeof v === "object") return JSON.stringify(v);
        return String(v)
          .replaceAll("&", "&amp;")
          .replaceAll("<", "&lt;")
          .replaceAll(">", "&gt;")
          .replaceAll('"', "&quot;")
          .replaceAll("'", "&#039;");
      }

      async function loadTables(api) {
        const result = await api("/_/api/tables");
        const host = document.getElementById("tables");
        host.innerHTML = "";
        const tables = result.tables || [];
        for (const t of tables) {
          const b = document.createElement("button");
          b.type = "button";
          b.className = "card";
          b.innerHTML = "<strong>" + esc(t.name) + "</strong><div class='muted'>" + esc(t.rowCount) + " rows</div>";
          b.onclick = function () { loadRows(api, t.name); };
          host.appendChild(b);
        }
        if (tables.length > 0) {
          await loadRows(api, tables[0].name);
        }
      }

      async function loadRows(api, name) {
        const result = await api("/_/api/tables/" + encodeURIComponent(name) + "/rows?limit=100&offset=0");
        const data = result.data || { rows: [], table: name, total: 0 };
        document.getElementById("rowsTitle").textContent = data.table + " (" + data.total + " rows)";
        const host = document.getElementById("rows");

        const rows = data.rows || [];
        if (rows.length === 0) {
          host.innerHTML = "<p class='muted'>No rows.</p>";
          return;
        }

        const fields = Object.keys(rows[0]);
        let html = "<table><thead><tr>";
        for (const f of fields) html += "<th>" + esc(f) + "</th>";
        html += "</tr></thead><tbody>";
        for (const row of rows) {
          html += "<tr>";
          for (const f of fields) html += "<td>" + esc(row[f]) + "</td>";
          html += "</tr>";
        }
        html += "</tbody></table>";
        host.innerHTML = html;
      }
`

const defaultAdminHTML = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Flop Admin</title>
    <style>` + defaultAdminCSS + `</style>
  </head>
  <body>
    <header>
      <h1>Flop Admin</h1>
      <p>Default admin panel (read-only).</p>
      <nav><a href="/">Back</a></nav>
    </header>
    <main>
      <section>
        <h2>Tables</h2>
        <div class="cards" id="tables"></div>
      </section>
      <section>
        <h2 id="rowsTitle">Rows</h2>
        <div id="rows"></div>
      </section>
    </main>
    <script>
      ` + defaultAdminJS + `
      async function api(path) {
        const res = await fetch(path);
        if (!res.ok) throw new Error("HTTP " + res.status);
        return await res.json();
      }
      loadTables(api).catch(function (err) {
        document.getElementById("rows").innerHTML = "<p>Failed to load admin panel: " + esc(err.message) + "</p>";
      });
    </script>
  </body>
</html>`

const defaultAdminAuthedHTML = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Flop Admin</title>
    <style>` + defaultAdminCSS + `</style>
  </head>
  <body>
    <header>
      <h1>Flop Admin</h1>
      <p>Authenticated admin panel.</p>
      <nav>
        <a href="/">Back</a>
        <button class="linkish" id="logoutBtn" style="margin-left:12px">Logout</button>
      </nav>
    </header>
    <main>
      <section>
        <h2>Tables</h2>
        <div class="cards" id="tables"></div>
      </section>
      <section>
        <h2 id="rowsTitle">Rows</h2>
        <div id="rows"></div>
      </section>
    </main>
    <script>
      ` + defaultAdminJS + `
      const token = localStorage.getItem("flop_token");
      if (!token) {
        location.href = "/_/login";
      }
      document.getElementById("logoutBtn").onclick = function () {
        localStorage.removeItem("flop_token");
        localStorage.removeItem("flop_refresh");
        location.href = "/_/login";
      };
      async function api(path) {
        const res = await fetch(path, { headers: { Authorization: "Bearer " + token } });
        if (res.status === 401) {
          location.href = "/_/login";
          throw new Error("unauthorized");
        }
        if (!res.ok) throw new Error("HTTP " + res.status);
        return await res.json();
      }
      loadTables(api).catch(function (err) {
        document.getElementById("rows").innerHTML = "<p>Failed to load admin panel: " + esc(err.message) + "</p>";
      });
    </script>
  </body>
</html>`

const defaultAdminLoginHTML = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Flop Admin - Login</title>
    <style>` + defaultAdminCSS + `</style>
  </head>
  <body>
    <div class="card-login">
      <h1>Flop Admin</h1>
      <p class="muted" style="margin:0 0 12px">Sign in with an existing user account.</p>
      <div id="err" class="error" style="display:none; margin-bottom:8px"></div>
      <form id="loginForm">
        <input name="email" type="email" placeholder="Email" required />
        <input name="password" type="password" placeholder="Password" required />
        <button type="submit">Sign in</button>
      </form>
    </div>
    <script>
      const form = document.getElementById("loginForm");
      const err = document.getElementById("err");
      form.onsubmit = async function (e) {
        e.preventDefault();
        const fd = new FormData(form);
        err.style.display = "none";
        try {
          const res = await fetch("/_/api/login", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              email: String(fd.get("email") || ""),
              password: String(fd.get("password") || ""),
            }),
          });
          const data = await res.json();
          if (!res.ok || data.error) {
            throw new Error(data.error || ("HTTP " + res.status));
          }
          localStorage.setItem("flop_token", data.token || "");
          localStorage.setItem("flop_refresh", data.refreshToken || "");
          location.href = "/_";
        } catch (e2) {
          err.textContent = e2.message || "Login failed";
          err.style.display = "block";
        }
      };
    </script>
  </body>
</html>`
