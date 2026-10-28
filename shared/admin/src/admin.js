import { AnalyticsChart, RequestsBarChart } from "./charts.jsx";
import {
  useState,
  useEffect,
  useLayoutEffect,
  useRef,
  useCallback,
  useMemo,
} from "react";
import { createRoot } from "react-dom/client";
import {
  html,
  Banner,
  Action,
  UploadButton,
  FieldInput,
  FieldSelect,
  FieldArea,
  Table,
  Badge,
  LayerCard,
  Dialog,
  Switch,
  Sidebar,
  Tabs,
  Toasty,
  toastManager,
  confirmAction,
  ConfirmDialog,
  ThemeControl,
  AppNavigation,
  TableLibrary,
  PageFrame,
} from "./ui.jsx";
import "@cloudflare/kumo/styles/standalone";
import "./layout.css";

// ── Auth ──
let TOKEN = localStorage.getItem("flop_admin_token");
if (!TOKEN) window.location.href = "/_/login";

let _refreshing = null;
async function refreshToken() {
  if (_refreshing) return _refreshing;
  const rt = localStorage.getItem("flop_admin_refresh");
  if (!rt) {
    logout();
    throw new Error("No refresh token");
  }
  _refreshing = fetch("/_/api/refresh", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ refreshToken: rt }),
  })
    .then(async (r) => {
      _refreshing = null;
      if (!r.ok) {
        logout();
        throw new Error("Refresh failed");
      }
      const data = await r.json();
      TOKEN = data.token;
      localStorage.setItem("flop_admin_token", data.token);
      return data.token;
    })
    .catch((err) => {
      _refreshing = null;
      throw err;
    });
  return _refreshing;
}

async function api(path, opts = {}) {
  const doFetch = () =>
    fetch("/_/api" + path, {
      ...opts,
      headers: {
        Authorization: "Bearer " + TOKEN,
        "Content-Type": "application/json",
        ...opts.headers,
      },
    });
  let r = await doFetch();
  if (r.status === 401) {
    try {
      await refreshToken();
      r = await doFetch();
    } catch {
      return;
    }
  }
  if (r.status === 401 || r.status === 403) {
    logout();
    throw new Error("Unauthorized");
  }
  const isJSON = r.headers.get("content-type")?.includes("json");
  if (!r.ok) {
    if (isJSON) {
      const data = await r.json().catch(() => ({}));
      throw new Error(data.error || data.message || "HTTP " + r.status);
    }
    throw new Error("HTTP " + r.status);
  }
  if (isJSON) return r.json();
  return r;
}

async function apiUpload(path, file) {
  const form = new FormData();
  form.append("file", file);
  const doFetch = () =>
    fetch("/_/api" + path, {
      method: "POST",
      headers: { Authorization: "Bearer " + TOKEN },
      body: form,
    });
  let r = await doFetch();
  if (r.status === 401) {
    try {
      await refreshToken();
      r = await doFetch();
    } catch {
      return;
    }
  }
  if (r.status === 401 || r.status === 403) {
    logout();
    throw new Error("Unauthorized");
  }
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(data.error || data.message || "HTTP " + r.status);
  return data;
}

async function apiDelete(path) {
  const doFetch = () =>
    fetch("/_/api" + path, {
      method: "DELETE",
      headers: { Authorization: "Bearer " + TOKEN },
    });
  let r = await doFetch();
  if (r.status === 401) {
    try {
      await refreshToken();
      r = await doFetch();
    } catch {
      return;
    }
  }
  if (r.status === 401 || r.status === 403) {
    logout();
    throw new Error("Unauthorized");
  }
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(data.error || data.message || "HTTP " + r.status);
  return data;
}

function logout() {
  localStorage.removeItem("flop_admin_token");
  localStorage.removeItem("flop_admin_refresh");
  window.location.href = "/_/login";
}

// ── Ref data cache ──
const refCache = {};
async function fetchRefOptions(tableName) {
  if (refCache[tableName]) return refCache[tableName];
  try {
    const data = await api("/tables/" + tableName + "/rows?limit=500");
    const rows = data.rows || [];
    const options = rows.map((r) => {
      const keys = Object.keys(r);
      const id = r[keys[0]];
      const label = keys.length > 1 ? r[keys[1]] || id : id;
      return { id: String(id), label: String(label) };
    });
    refCache[tableName] = options;
    return options;
  } catch {
    return [];
  }
}

// ── Icons (inline SVG) ──
const icons = {
  x: html`<svg
    width="18"
    height="18"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    stroke-width="1.8"
  >
    <path d="m6 6 12 12M6 18 18 6" />
  </svg>`,
  table: html`<svg
    width="16"
    height="16"
    viewBox="0 0 16 16"
    fill="currentColor"
  >
    <path
      d="M14 1H2a1 1 0 00-1 1v12a1 1 0 001 1h12a1 1 0 001-1V2a1 1 0 00-1-1zM2 2h12v3H2V2zm0 4h5v4H2V6zm0 5h5v3H2v-3zm12 3H8v-3h6v3zm0-4H8V6h6v4z"
    />
  </svg>`,
  plus: html`<svg
    width="14"
    height="14"
    viewBox="0 0 16 16"
    fill="currentColor"
  >
    <path d="M14 7v1H8v6H7V8H1V7h6V1h1v6h6z" />
  </svg>`,
  edit: html`<svg
    width="14"
    height="14"
    viewBox="0 0 16 16"
    fill="currentColor"
  >
    <path
      d="M11.854 1.146a.5.5 0 00-.708 0l-8.5 8.5a.5.5 0 00-.132.235l-.5 2a.5.5 0 00.606.606l2-.5a.5.5 0 00.235-.132l8.5-8.5a.5.5 0 000-.708l-1.5-1.5zM3.474 10.257l8.026-8.026.793.793-8.026 8.026-1.118.28.325-1.073z"
    />
    <path d="M2 14h12v1H2z" />
  </svg>`,
  trash: html`<svg
    width="14"
    height="14"
    viewBox="0 0 16 16"
    fill="currentColor"
  >
    <path
      d="M10 3h3v1h-1v9a1 1 0 01-1 1H5a1 1 0 01-1-1V4H3V3h3V2a1 1 0 011-1h2a1 1 0 011 1v1zM5 4v9h6V4H5zm1 1h1v7H6V5zm3 0h1v7H9V5zM7 2v1h2V2H7z"
    />
  </svg>`,
  download: html`<svg
    width="14"
    height="14"
    viewBox="0 0 16 16"
    fill="currentColor"
  >
    <path
      d="M4.5 13h7a.5.5 0 010 1h-7a.5.5 0 010-1zM7.5 2v7.793L5.354 7.646a.5.5 0 10-.708.708l3 3a.5.5 0 00.708 0l3-3a.5.5 0 00-.708-.708L8.5 9.793V2a.5.5 0 00-1 0z"
    />
  </svg>`,
  upload: html`<svg
    width="14"
    height="14"
    viewBox="0 0 16 16"
    fill="currentColor"
  >
    <path
      d="M4.5 13h7a.5.5 0 010 1h-7a.5.5 0 010-1zM7.5 11V3.207L5.354 5.354a.5.5 0 11-.708-.708l3-3a.5.5 0 01.708 0l3 3a.5.5 0 01-.708.708L8.5 3.207V11a.5.5 0 01-1 0z"
    />
  </svg>`,
  signout: html`<svg
    width="14"
    height="14"
    viewBox="0 0 16 16"
    fill="currentColor"
  >
    <path
      d="M11.854 8.354a.5.5 0 000-.708l-3-3a.5.5 0 10-.708.708L10.293 7.5H4a.5.5 0 000 1h6.293l-2.147 2.146a.5.5 0 00.708.708l3-3z"
    />
    <path d="M14.5 1a.5.5 0 01.5.5v13a.5.5 0 01-1 0v-13a.5.5 0 01.5-.5z" />
  </svg>`,
  search: html`<svg
    width="14"
    height="14"
    viewBox="0 0 16 16"
    fill="currentColor"
  >
    <path
      d="M11.742 10.344a6.5 6.5 0 10-1.397 1.398h-.001c.03.04.062.078.098.115l3.85 3.85a1 1 0 001.415-1.414l-3.85-3.85a1.007 1.007 0 00-.115-.1zM12 6.5a5.5 5.5 0 11-11 0 5.5 5.5 0 0111 0z"
    />
  </svg>`,
  chevLeft: html`<svg
    width="14"
    height="14"
    viewBox="0 0 16 16"
    fill="currentColor"
  >
    <path
      fill-rule="evenodd"
      d="M11.354 1.646a.5.5 0 010 .708L5.707 8l5.647 5.646a.5.5 0 01-.708.708l-6-6a.5.5 0 010-.708l6-6a.5.5 0 01.708 0z"
    />
  </svg>`,
  chevRight: html`<svg
    width="14"
    height="14"
    viewBox="0 0 16 16"
    fill="currentColor"
  >
    <path
      fill-rule="evenodd"
      d="M4.646 1.646a.5.5 0 01.708 0l6 6a.5.5 0 010 .708l-6 6a.5.5 0 01-.708-.708L10.293 8 4.646 2.354a.5.5 0 010-.708z"
    />
  </svg>`,
  refresh: html`<svg
    width="14"
    height="14"
    viewBox="0 0 16 16"
    fill="currentColor"
  >
    <path
      d="M8 2a6 6 0 015.196 3H11.5a.5.5 0 000 1H15a.5.5 0 00.5-.5V2a.5.5 0 00-1 0v2.078A7 7 0 1015 8a.5.5 0 00-1 0A6 6 0 118 2z"
    />
  </svg>`,
  db: html`<svg width="18" height="18" viewBox="0 0 16 16" fill="currentColor">
    <ellipse cx="8" cy="3.5" rx="5.5" ry="2" />
    <path d="M13.5 5.5c0 1.1-2.46 2-5.5 2s-5.5-.9-5.5-2" />
    <path
      d="M2.5 3.5v9c0 1.1 2.46 2 5.5 2s5.5-.9 5.5-2v-9"
      fill="none"
      stroke="currentColor"
      stroke-width="1"
    />
    <path
      d="M13.5 8.5c0 1.1-2.46 2-5.5 2s-5.5-.9-5.5-2"
      fill="none"
      stroke="currentColor"
      stroke-width="1"
    />
  </svg>`,
  chart: html`<svg
    width="16"
    height="16"
    viewBox="0 0 16 16"
    fill="currentColor"
  >
    <path d="M1 14h14v1H0V1h1v13z" />
    <path d="M3 10.5l2.3-2.3 2.2 1.5 3.7-4.2.8.7-4.3 4.9-2.3-1.6L3.7 11.2z" />
  </svg>`,
  pulse: html`<svg
    width="16"
    height="16"
    viewBox="0 0 16 16"
    fill="currentColor"
  >
    <path
      d="M1 8h2.7l1.2-3.1a.5.5 0 01.94.02L7.6 11.2l1.1-2.2a.5.5 0 01.45-.28H15v1H9.46l-1.5 3a.5.5 0 01-.93-.07L5.3 6.74 4.45 8.9A.5.5 0 013.98 9H1V8z"
    />
  </svg>`,
  image: html`<svg
    width="16"
    height="16"
    viewBox="0 0 16 16"
    fill="currentColor"
  >
    <path
      d="M2 3.5A1.5 1.5 0 013.5 2h9A1.5 1.5 0 0114 3.5v9a1.5 1.5 0 01-1.5 1.5h-9A1.5 1.5 0 012 12.5v-9zm1.5-.5a.5.5 0 00-.5.5v6.293l2.146-2.147a.5.5 0 01.708 0L8.5 10.293l1.646-1.647a.5.5 0 01.708 0L13 10.793V3.5a.5.5 0 00-.5-.5h-9zM13 12.5v-.293l-2.5-2.5-1.646 1.647a.5.5 0 01-.708 0L5.5 8.707 3 11.207v1.293a.5.5 0 00.5.5h9a.5.5 0 00.5-.5z"
    />
    <path d="M6 6a1.25 1.25 0 100-2.5A1.25 1.25 0 006 6z" />
  </svg>`,
  clock: html`<svg
    width="16"
    height="16"
    viewBox="0 0 16 16"
    fill="currentColor"
  >
    <path
      d="M8 1.5A6.5 6.5 0 1014.5 8 6.5 6.5 0 008 1.5zm0 12A5.5 5.5 0 118 2.5a5.5 5.5 0 010 11zm.5-8v2.793l1.854 1.853-.708.708L7.5 8.707V5.5h1z"
    />
  </svg>`,
  mail: html`<svg
    width="16"
    height="16"
    viewBox="0 0 16 16"
    fill="currentColor"
  >
    <path
      d="M1.75 3A1.75 1.75 0 000 4.75v6.5C0 12.216.784 13 1.75 13h12.5A1.75 1.75 0 0016 11.25v-6.5A1.75 1.75 0 0014.25 3H1.75zm0 1h12.5c.207 0 .4.08.544.223L8 8.44 1.206 4.223A.75.75 0 011.75 4zm-.75 1.39l4.94 3.067L1 11.932V5.39zm1.132 6.61l4.64-3.27.832.516a.75.75 0 00.792 0l.832-.516 4.64 3.27H2.132zM15 11.932l-4.94-3.475L15 5.39v6.542z"
    />
  </svg>`,
  check: html`<svg
    width="14"
    height="14"
    viewBox="0 0 16 16"
    fill="currentColor"
  >
    <path
      d="M13.854 3.646a.5.5 0 010 .708l-7 7a.5.5 0 01-.708 0l-3-3a.5.5 0 11.708-.708L6.5 10.293l6.646-6.647a.5.5 0 01.708 0z"
    />
  </svg>`,
  chevDown: html`<svg
    width="14"
    height="14"
    viewBox="0 0 16 16"
    fill="currentColor"
  >
    <path
      fill-rule="evenodd"
      d="M3.646 5.646a.5.5 0 01.708 0L8 9.293l3.646-3.647a.5.5 0 11.708.708l-4 4a.5.5 0 01-.708 0l-4-4a.5.5 0 010-.708z"
    />
  </svg>`,
  chevUp: html`<svg
    width="14"
    height="14"
    viewBox="0 0 16 16"
    fill="currentColor"
  >
    <path
      fill-rule="evenodd"
      d="M8 5.293 4.354 8.94a.5.5 0 11-.708-.708l4-4a.5.5 0 01.708 0l4 4a.5.5 0 11-.708.708L8 5.293z"
    />
  </svg>`,
};

// ── Helpers ──
function isAutoField(schema, col) {
  if (!schema) return false;
  return Object.keys(schema)[0] === col;
}
function isReadOnly(schema, col) {
  if (!schema || !schema[col]) return false;
  return schema[col].type === "bcrypt" || schema[col].cached === true;
}
function truncate(s, n = 60) {
  if (s == null) return "";
  const str = String(s);
  return str.length > n ? str.slice(0, n) + "..." : str;
}

function formatBytes(bytes) {
  const num = Number(bytes || 0);
  if (num < 1024) return num + " B";
  if (num < 1024 * 1024)
    return (num / 1024).toFixed(2).replace(/\.00$/, "") + " KB";
  if (num < 1024 * 1024 * 1024)
    return (num / (1024 * 1024)).toFixed(2).replace(/\.00$/, "") + " MB";
  return (num / (1024 * 1024 * 1024)).toFixed(2).replace(/\.00$/, "") + " GB";
}

function formatDateTime(value) {
  if (!value) return "";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return "";
  return dt.toLocaleString();
}

function titleCaseToken(token) {
  return String(token || "")
    .split(/[_-]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function routeTypeLabel(routeType, plural = true) {
  const normalized = String(routeType || "request");
  if (normalized === "request") return plural ? "Requests" : "Request";
  if (normalized === "view") return plural ? "Views" : "View";
  if (normalized === "reducer") return plural ? "Reducers" : "Reducer";
  const label = titleCaseToken(normalized);
  return plural ? label + " Events" : label + " Event";
}

function showToast(message, type = "success") {
  toastManager.add({ title: message, variant: type });
}

function valueToEditorInput(field, value) {
  if (value == null) return "";
  if (!field) return String(value);
  if (field.type === "boolean") return value ? "true" : "false";
  if (
    ["json", "vector", "roles", "set", "refMulti", "fileMulti"].includes(
      field.type,
    )
  ) {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
}

function parseEditorInput(field, input) {
  if (!field) return input;
  if (
    field.type === "number" ||
    field.type === "integer" ||
    field.type === "timestamp"
  ) {
    return input === "" ? null : Number(input);
  }
  if (field.type === "boolean") return input === "" ? null : input === "true";
  if (
    ["json", "vector", "roles", "set", "refMulti", "fileMulti"].includes(
      field.type,
    )
  ) {
    try {
      return JSON.parse(input);
    } catch {
      return input;
    }
  }
  return input;
}

function RowEditor({
  open,
  mode = "edit",
  tableName,
  schema,
  cols,
  row,
  createError,
  onClose,
  onSave,
  onCreate,
  onReload,
  readOnly = false,
}) {
  const isCreate = mode === "create";
  const pkField = cols[0];
  const pk = row ? row[pkField] : "";
  const [form, setForm] = useState({});
  const [pendingFiles, setPendingFiles] = useState({});
  const [refOptions, setRefOptions] = useState({});
  const [saving, setSaving] = useState(false);
  const [uploadingCol, setUploadingCol] = useState("");

  // Keep an in-progress draft when live updates refresh the same row.
  useLayoutEffect(() => {
    if (!schema) return;
    const next = {};
    for (const col of cols) {
      next[col] = isCreate
        ? ""
        : valueToEditorInput(schema[col], row ? row[col] : null);
    }
    setForm(next);
    setPendingFiles({});
  }, [pk, tableName, isCreate, open]);

  useEffect(() => {
    if (!schema) return;
    const refs = Object.entries(schema)
      .filter(
        ([, f]) =>
          f && (f.type === "ref" || f.type === "refMulti") && f.refTable,
      )
      .map(([col, f]) => ({ col, table: f.refTable }));
    let alive = true;
    Promise.all(refs.map((r) => fetchRefOptions(r.table))).then((all) => {
      if (!alive) return;
      const map = {};
      refs.forEach((r, i) => {
        map[r.col] = all[i];
      });
      setRefOptions(map);
    });
    return () => {
      alive = false;
    };
  }, [schema]);

  const setCol = useCallback((col, value) => {
    setForm((prev) => ({ ...prev, [col]: value }));
  }, []);

  const doSave = useCallback(async () => {
    if (!schema) return;

    if (isCreate) {
      const data = {};
      for (const col of cols) {
        if (isAutoField(schema, col)) continue;
        const f = schema[col];
        if (f && f.cached) continue;
        if (f && (f.type === "fileSingle" || f.type === "fileMulti")) continue;
        const raw = form[col] ?? "";
        if (raw === "") continue;
        data[col] = parseEditorInput(f, raw);
      }
      setSaving(true);
      try {
        await onCreate(data, pendingFiles);
      } catch (err) {
        // Errors are handled by submitCreate (toast + inline message).
      } finally {
        setSaving(false);
      }
      return;
    }

    if (!row) return;
    const payload = {};
    for (const col of cols) {
      if (isAutoField(schema, col) || isReadOnly(schema, col)) continue;
      const f = schema[col];
      if (f && (f.type === "fileSingle" || f.type === "fileMulti")) continue;
      const before = valueToEditorInput(f, row[col]);
      const after = form[col] ?? "";
      if (before !== after) payload[col] = parseEditorInput(f, after);
    }
    if (Object.keys(payload).length === 0) {
      showToast("No changes");
      return;
    }
    setSaving(true);
    try {
      await onSave(String(pk), payload);
    } catch (err) {
      showToast(err.message || "Update failed", "error");
    } finally {
      setSaving(false);
    }
  }, [schema, row, cols, form, pk, onSave, isCreate, onCreate, pendingFiles]);

  const onUploadFile = useCallback(
    async (col, e) => {
      if (readOnly) return;
      const field = schema ? schema[col] : null;
      const files = Array.from(e.target.files || []);
      if (isCreate) {
        if (field && field.type === "fileMulti") {
          if (files.length === 0) return;
          setPendingFiles((prev) => ({
            ...prev,
            [col]: [...normalizeFileItems(prev[col]), ...files],
          }));
        } else {
          const f = files[0];
          if (!f) return;
          setPendingFiles((prev) => ({ ...prev, [col]: f }));
          setCol(col, f.name);
        }
        if (e.target) e.target.value = "";
        return;
      }
      const f = files[0];
      if (!f) return;
      setUploadingCol(col);
      try {
        const res = await apiUpload(
          "/tables/" + tableName + "/rows/" + pk + "/files/" + col,
          f,
        );
        if (res && res.error) showToast(res.error, "error");
        else showToast("File uploaded");
        await onReload();
      } catch (err) {
        showToast(err.message || "Upload failed", "error");
      } finally {
        setUploadingCol("");
        if (e.target) e.target.value = "";
      }
    },
    [tableName, pk, onReload, isCreate, setCol, readOnly, schema],
  );

  const onRemoveFile = useCallback(
    async (col, fileKey = "", pendingIndex = -1) => {
      if (readOnly) return;
      const field = schema ? schema[col] : null;
      if (isCreate) {
        if (field && field.type === "fileMulti" && pendingIndex >= 0) {
          setPendingFiles((prev) => {
            const next = { ...prev };
            next[col] = normalizeFileItems(prev[col]).filter(
              (_, idx) => idx !== pendingIndex,
            );
            if (next[col].length === 0) delete next[col];
            return next;
          });
        } else {
          setPendingFiles((prev) => {
            const next = { ...prev };
            delete next[col];
            return next;
          });
          setCol(col, "");
        }
        return;
      }
      try {
        const suffix = fileKey ? "?path=" + encodeURIComponent(fileKey) : "";
        await apiDelete(
          "/tables/" + tableName + "/rows/" + pk + "/files/" + col + suffix,
        );
        showToast("File removed");
        await onReload();
      } catch (err) {
        showToast(err.message || "Remove failed", "error");
      }
    },
    [tableName, pk, onReload, isCreate, setCol, readOnly, schema],
  );

  return html`
    <${Dialog.Root} open=${open} onOpenChange=${(visible) => {
      if (!visible) onClose();
    }}><${Dialog} size="xl" className="record-dialog"><div class="row-editor">
      <div class="row-editor-header">
        <div>
          <${Dialog.Title}>${isCreate ? "New record" : "Edit record"}</${Dialog.Title}>
          <${Dialog.Description}>${tableName} · ${isCreate ? "New record" : String(pk)}</${Dialog.Description}>
        </div>
        <${Action} class="btn-icon" aria-label="Close row editor" onClick=${onClose}>${icons.x}</${Action}>
      </div>
      <div class="row-editor-body">
        ${cols.map((col) => {
          const field = schema ? schema[col] : null;
          const val = row ? row[col] : null;
          const ro =
            readOnly ||
            (isCreate
              ? isAutoField(schema, col) || (field && field.cached === true)
              : isAutoField(schema, col) ||
                isReadOnly(schema, col) ||
                val === "[REDACTED]");
          const typeLabel = field
            ? field.type + (field.refTable ? " -> " + field.refTable : "")
            : "";
          const inputVal = form[col] ?? "";

          if (
            field &&
            (field.type === "fileSingle" || field.type === "fileMulti")
          ) {
            const accept = field.mimeTypes ? field.mimeTypes.join(",") : "";
            const singleFile = isCreate
              ? pendingFiles[col]
              : field.type === "fileSingle"
                ? val
                : null;
            const multiFiles =
              field.type === "fileMulti"
                ? isCreate
                  ? normalizeFileItems(pendingFiles[col])
                  : normalizeFileItems(val)
                : [];
            const fileCount =
              field.type === "fileMulti"
                ? multiFiles.length
                : singleFile
                  ? 1
                  : 0;
            return html`<div key=${col} class="row-editor-field">
              <label>${col} <span class="type-badge">${typeLabel}</span></label>
              <div class="row-editor-readonly">
                ${field.type === "fileSingle"
                  ? singleFile && fileDisplayName(singleFile)
                    ? fileDisplayName(singleFile)
                    : html`<span class="null">null</span>`
                  : fileCount > 0
                    ? fileCount + " files"
                    : html`<span class="null">null</span>`}
              </div>
              ${field.type === "fileMulti" &&
              html`
                <div class="file-list">
                  ${multiFiles.length > 0
                    ? multiFiles.map((file, idx) => {
                        const fileName =
                          fileDisplayName(file) || "file " + (idx + 1);
                        const itemKey =
                          file.path || file.url || fileName + ":" + idx;
                        return html`<div key=${itemKey} class="file-item">
                          <span class="file-item-main">
                            ${isImageFile(file) && file.url
                              ? html`<img
                                  class="file-thumb"
                                  src=${file.url}
                                  alt=${fileName}
                                />`
                              : ""}
                            ${file.url
                              ? html`<a
                                  href=${file.url}
                                  target="_blank"
                                  class="file-link file-item-name"
                                  onClick=${(e) => e.stopPropagation()}
                                  >${fileName}</a
                                >`
                              : html`<span class="file-item-name"
                                  >${fileName}</span
                                >`}
                          </span>
                          ${!readOnly &&
                          html`<${Action} class="btn btn-sm file-remove" onClick=${() => onRemoveFile(col, isCreate ? fileName : file.path || "", isCreate ? idx : -1)}>${icons.trash} Remove</${Action}>`}
                        </div>`;
                      })
                    : html`<span class="null">null</span>`}
                </div>
              `}
              <div class="create-actions" style="margin-top:6px">
                ${!readOnly &&
                html`
                <${UploadButton} disabled=${uploadingCol === col}>
                  ${uploadingCol === col ? "Uploading..." : html`${icons.upload} Upload`}
                  <${FieldInput} aria-label=${col} type="file" accept=${accept} multiple=${field.type === "fileMulti"} onChange=${(e) => onUploadFile(col, e)} hidden />
                </${UploadButton}>
                ${field.type === "fileSingle" && html`<${Action} class="btn btn-sm file-remove" onClick=${() => onRemoveFile(col)}>${icons.trash} Remove</${Action}>`}
                `}
              </div>
            </div>`;
          }

          if (ro) {
            const display = isCreate
              ? html`<span class="null">auto</span>`
              : val == null
                ? html`<span class="null">null</span>`
                : typeof val === "object"
                  ? JSON.stringify(val)
                  : String(val);
            return html`<div key=${col} class="row-editor-field">
              <label
                >${col}
                <span class="type-badge"
                  >${typeLabel}${typeLabel ? " " : ""}(read only)</span
                ></label
              >
              <div class="row-editor-readonly">${display}</div>
            </div>`;
          }

          if (field && field.type === "bcrypt") {
            return html`<div key=${col} class="row-editor-field">
              <label>${col} <span class="type-badge">${typeLabel}</span></label>
              <${FieldInput}
                aria-label=${col}
                type="password"
                value=${inputVal}
                onInput=${(e) => setCol(col, e.target.value)}
              />
            </div>`;
          }

          if (field && field.type === "boolean") {
            return html`<div key=${col} class="row-editor-field">
              <label>${col} <span class="type-badge">${typeLabel}</span></label>
              <${FieldSelect} aria-label=${col} value=${inputVal} disabled=${readOnly} onChange=${(e) => setCol(col, e.target.value)}>
                <${FieldSelect.Option} value="">null</${FieldSelect.Option}>
                <${FieldSelect.Option} value="true">true</${FieldSelect.Option}>
                <${FieldSelect.Option} value="false">false</${FieldSelect.Option}>
              </${FieldSelect}>
            </div>`;
          }

          if (field && field.type === "enum" && field.enumValues) {
            return html`<div key=${col} class="row-editor-field">
              <label>${col} <span class="type-badge">${typeLabel}</span></label>
              <${FieldSelect} aria-label=${col} value=${inputVal} disabled=${readOnly} onChange=${(e) => setCol(col, e.target.value)}>
                <${FieldSelect.Option} value="">--</${FieldSelect.Option}>
                ${field.enumValues.map((v) => html`<${FieldSelect.Option} key=${v} value=${v}>${v}</${FieldSelect.Option}>`)}
              </${FieldSelect}>
            </div>`;
          }

          if (field && field.type === "ref" && field.refTable) {
            const options = refOptions[col] || [];
            return html`<div key=${col} class="row-editor-field">
              <label>${col} <span class="type-badge">${typeLabel}</span></label>
              <${FieldSelect} aria-label=${col} value=${inputVal} onChange=${(e) => setCol(col, e.target.value)}>
                <${FieldSelect.Option} value="">--</${FieldSelect.Option}>
                ${options.map((o) => html`<${FieldSelect.Option} key=${o.id} value=${o.id}>${o.id}${o.label !== o.id ? " - " + o.label : ""}</${FieldSelect.Option}>`)}
              </${FieldSelect}>
            </div>`;
          }

          if (
            field &&
            ["json", "vector", "roles", "set", "refMulti"].includes(field.type)
          ) {
            return html`<div key=${col} class="row-editor-field">
              <label>${col} <span class="type-badge">${typeLabel}</span></label>
              <${FieldArea} aria-label=${col} value=${inputVal} disabled=${readOnly} onInput=${(e) => setCol(col, e.target.value)}></${FieldArea}>
            </div>`;
          }

          const inputType =
            field &&
            (field.type === "number" ||
              field.type === "integer" ||
              field.type === "timestamp")
              ? "number"
              : "text";
          return html`<div key=${col} class="row-editor-field">
            <label>${col} <span class="type-badge">${typeLabel}</span></label>
            <${FieldInput}
              aria-label=${col}
              type=${inputType}
              value=${inputVal}
              disabled=${readOnly}
              onInput=${(e) => setCol(col, e.target.value)}
            />
          </div>`;
        })}
      </div>
      <div class="row-editor-actions">
        ${isCreate && createError && html`<span class="create-error" style="margin-right:auto">${createError}</span>`}
        <${Action} class="btn" onClick=${onClose}>Close</${Action}>
        ${!readOnly && html`<${Action} class="btn btn-primary" disabled=${saving} onClick=${doSave}>${isCreate ? (saving ? "Creating..." : "Create record") : saving ? "Saving..." : "Save changes"}</${Action}>`}
      </div>
    </div></${Dialog}></${Dialog.Root}>
  `;
}

function DataTable({
  tableName,
  tableMeta,
  schema,
  archive = false,
  onReload,
  sseSignal,
}) {
  const [rows, setRows] = useState([]);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [total, setTotal] = useState(0);
  const [selectedPk, setSelectedPk] = useState(null);
  const [showCreate, setShowCreate] = useState(false);
  const [createErr, setCreateErr] = useState("");
  const [search, setSearch] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [refreshingMaterialized, setRefreshingMaterialized] = useState(false);

  const isMaterialized = !!tableMeta?.materialized;
  const isReadOnlyTable = !!tableMeta?.readOnly || archive;
  const rowsBasePath = archive
    ? "/archive/tables/" + tableName + "/rows"
    : "/tables/" + tableName + "/rows";

  const cols = useMemo(
    () =>
      schema
        ? Object.keys(schema)
        : rows.length > 0
          ? Object.keys(rows[0])
          : [],
    [schema, rows],
  );

  // Lead phone summaries with recognizable names instead of internal IDs.
  const mobileCols = useMemo(() => {
    const preferred = [
      "title",
      "name",
      "email",
      "username",
      "slug",
      "excerpt",
      "description",
    ];
    const rank = (c) => {
      const index = preferred.indexOf(c.toLowerCase());
      return index < 0 ? preferred.length : index;
    };
    const fields = cols.filter(
      (c) =>
        !isAutoField(schema, c) &&
        c !== cols[0] &&
        !["ref", "timestamp", "bcrypt", "fileSingle", "fileMulti"].includes(
          schema?.[c]?.type,
        ),
    );
    return fields.length
      ? fields.sort((a, b) => rank(a) - rank(b)).slice(0, 3)
      : cols.slice(0, 1);
  }, [cols, schema]);

  const displayCols = useMemo(
    () => [...mobileCols, ...cols.filter((c) => !mobileCols.includes(c))],
    [cols, mobileCols],
  );

  const loadRows = useCallback(
    async (p, q) => {
      const pg = p || page;
      const sq = q !== undefined ? q : searchQuery;
      const params = new URLSearchParams({ limit: 50 });
      if (archive) params.set("offset", String((pg - 1) * 50));
      else params.set("page", pg);
      if (sq && !archive) {
        const isFilter = /[=<>~]|!=|!~/.test(sq);
        params.set(isFilter ? "filter" : "search", sq);
      }
      try {
        const data = await api(rowsBasePath + "?" + params);
        if (data) {
          setRows(data.rows || []);
          setTotalPages(data.pages);
          setTotal(data.total);
        }
      } catch (err) {
        if (err && err.message) showToast(err.message, "error");
      }
    },
    [tableName, page, searchQuery, archive, rowsBasePath],
  );

  useEffect(() => {
    setPage(1);
    setSearch("");
    setSearchQuery("");
    setShowCreate(false);
    setCreateErr("");
    setSelectedPk(null);
  }, [tableName]);

  useEffect(() => {
    loadRows(page, searchQuery);
  }, [tableName, page, searchQuery]);

  // Debounced reload when SSE signals changes for this table
  const sseReloadTimer = useRef(null);
  useEffect(() => {
    if (!sseSignal) return;
    clearTimeout(sseReloadTimer.current);
    sseReloadTimer.current = setTimeout(() => loadRows(), 500);
    return () => clearTimeout(sseReloadTimer.current);
  }, [sseSignal]);

  const applyFilter = useCallback(() => {
    setSearchQuery(search);
    setPage(1);
  }, [search]);

  const onSearchInput = useCallback((e) => {
    setSearch(e.target.value);
  }, []);

  const onSearchKey = useCallback(
    (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        applyFilter();
      }
    },
    [applyFilter],
  );

  const saveRow = useCallback(
    async (pk, payload) => {
      await api("/tables/" + tableName + "/rows/" + pk, {
        method: "PUT",
        body: JSON.stringify(payload),
      });
      showToast("Row updated");
      loadRows();
    },
    [tableName, loadRows],
  );

  const deleteRow = useCallback(
    async (pk) => {
      if (!(await confirmAction("Delete row " + pk + "?"))) return;
      try {
        await api("/tables/" + tableName + "/rows/" + pk, { method: "DELETE" });
        showToast("Row deleted");
      } catch (err) {
        showToast(err.message || "Delete failed", "error");
      }
      if (selectedPk != null && String(selectedPk) === String(pk))
        setSelectedPk(null);
      loadRows();
    },
    [tableName, loadRows, selectedPk],
  );

  const archiveRow = useCallback(
    async (pk) => {
      if (!(await confirmAction("Archive row " + pk + "?"))) return;
      try {
        await api("/tables/" + tableName + "/rows/" + pk + "/archive", {
          method: "POST",
        });
        showToast("Row archived");
      } catch (err) {
        showToast(err.message || "Archive failed", "error");
      }
      if (selectedPk != null && String(selectedPk) === String(pk))
        setSelectedPk(null);
      loadRows();
      onReload?.();
    },
    [tableName, loadRows, selectedPk, onReload],
  );

  const restoreRow = useCallback(
    async (archiveID) => {
      if (!(await confirmAction("Restore archived row " + archiveID + "?")))
        return;
      try {
        await api(
          "/archive/tables/" + tableName + "/rows/" + archiveID + "/restore",
          { method: "POST" },
        );
        showToast("Archived row restored");
      } catch (err) {
        showToast(err.message || "Restore failed", "error");
      }
      if (selectedPk != null && String(selectedPk) === String(archiveID))
        setSelectedPk(null);
      loadRows();
      onReload?.();
    },
    [tableName, selectedPk, loadRows, onReload],
  );

  const deleteArchivedRow = useCallback(
    async (archiveID) => {
      if (
        !(await confirmAction(
          "Permanently delete archived row " + archiveID + "?",
        ))
      )
        return;
      try {
        await api("/archive/tables/" + tableName + "/rows/" + archiveID, {
          method: "DELETE",
        });
        showToast("Archived row deleted");
      } catch (err) {
        showToast(err.message || "Delete failed", "error");
      }
      if (selectedPk != null && String(selectedPk) === String(archiveID))
        setSelectedPk(null);
      loadRows();
      onReload?.();
    },
    [tableName, selectedPk, loadRows, onReload],
  );

  const refreshMaterialized = useCallback(async () => {
    if (!isMaterialized || refreshingMaterialized) return;
    setRefreshingMaterialized(true);
    try {
      await api("/materialized/" + tableName + "/refresh", { method: "POST" });
      showToast("Materialized table refreshed");
      onReload?.();
      loadRows();
    } catch (err) {
      showToast(err.message || "Refresh failed", "error");
    } finally {
      setRefreshingMaterialized(false);
    }
  }, [isMaterialized, refreshingMaterialized, tableName, onReload, loadRows]);

  const submitCreate = useCallback(
    async (data, pendingFiles) => {
      if (!schema) return;
      setCreateErr("");
      try {
        const res = await api("/tables/" + tableName + "/rows", {
          method: "POST",
          body: JSON.stringify(data),
        });
        // Upload pending files using the new row's primary key
        if (res && res.row) {
          const pk = res.row[Object.keys(schema)[0]];
          for (const [col, value] of Object.entries(pendingFiles)) {
            const files = Array.isArray(value) ? value : [value];
            for (const file of files) {
              if (!file) continue;
              const upRes = await apiUpload(
                "/tables/" + tableName + "/rows/" + pk + "/files/" + col,
                file,
              );
              if (upRes && upRes.error)
                showToast(col + ": " + upRes.error, "error");
            }
          }
        }
        setShowCreate(false);
        setCreateErr("");
        showToast("Row created");
        loadRows();
      } catch (err) {
        showToast(err.message || "Create failed", "error");
        setCreateErr(err.message || "Create failed");
        throw err;
      }
    },
    [schema, tableName, loadRows],
  );

  const selectedRow = useMemo(() => {
    if (selectedPk == null) return null;
    return rows.find((r) => String(r[cols[0]]) === String(selectedPk)) || null;
  }, [rows, cols, selectedPk]);

  return html`
    <div class="panel">
      <div class="panel-toolbar">
        <div class="panel-title">
          <span class="panel-title-icon">${icons.table}</span>
          ${tableName}
          ${isMaterialized && html`<${Badge} variant="secondary">materialized</${Badge}>`}
          <span class="panel-meta">${total} rows</span>
          ${tableMeta?.lastRefreshUnixMilli > 0 ? html`<span class="panel-meta">refreshed ${fmtAgo(tableMeta.lastRefreshUnixMilli)} (${fmtTime(tableMeta.lastRefreshUnixMilli)})</span>` : ""}
        </div>
        <div class="panel-actions">
          <div class="search-box">
            ${icons.search}
            <${FieldInput} aria-label="Filter rows" type="text" placeholder=${archive ? "Archive search not available" : "Search or filter…"} value=${search} disabled=${archive} onInput=${onSearchInput} onKeyDown=${onSearchKey} />
            <span class="filter-help" tabindex="0" aria-label="Filter syntax: equals, contains, comparisons; plain text searches all string fields" title="= equals  != not equal  ~ contains  !~ not contains&#10;> >= < <= comparisons&#10;&& and  || or  () grouping&#10;&#10;Plain text = search all string fields">?</span>
            <${Action} class="btn-filter" aria-label="Apply filter" disabled=${archive} onClick=${applyFilter}>${icons.chevRight}</${Action}>
          </div>
          <${Action} class="btn btn-primary" disabled=${isReadOnlyTable} onClick=${() => {
            setSelectedPk(null);
            setCreateErr("");
            setShowCreate(true);
          }}>
            ${icons.plus} New record
          </${Action}>
        </div>
      </div>

      ${
        isMaterialized &&
        html`<div class=${"materialized-banner" + (tableMeta?.refreshError ? " error" : "")}>
        <div>
          <strong>Materialized table</strong>
          <div class="panel-meta">${tableMeta?.refreshError ? "Last refresh failed" : tableMeta?.lastRefreshUnixMilli > 0 ? "Last refreshed " + fmtAgo(tableMeta.lastRefreshUnixMilli) : "Not refreshed yet"}</div>
          ${tableMeta?.refreshError ? html`<div class="panel-meta" style="color:var(--danger-hover-text)">${tableMeta.refreshError}</div>` : ""}
        </div>
        <${Action} class="btn" disabled=${refreshingMaterialized} onClick=${refreshMaterialized}>${icons.refresh} ${refreshingMaterialized ? "Refreshing..." : "Refresh now"}</${Action}>
      </div>`
      }

      <div class="table-layout">
        <div class="table-container">
          <${Table} class="data-table" aria-label=${tableName + " rows"}>
            <${Table.Header}>
              <${Table.Row}>
                ${displayCols.map((c) => {
                  const f = schema ? schema[c] : null;
                  const typeStr = f
                    ? f.type +
                      (f.refTable ? " -> " + f.refTable : "") +
                      (f.cached ? " (cached)" : "")
                    : "";
                  return html`<${Table.Head} key=${c}>
                    <span class="th-name">${c}</span>
                    ${typeStr && html`<span class="th-type">${typeStr}</span>`}
                  </${Table.Head}>`;
                })}
                <${Table.Head} class="th-actions"></${Table.Head}>
              </${Table.Row}>
            </${Table.Header}>
            <${Table.Body}>
              ${
                rows.length === 0 &&
                html`
                <${Table.Row}><${Table.Cell} colspan=${cols.length + 1} class="empty-row">No rows found</${Table.Cell}></${Table.Row}>
              `
              }
              ${rows.map((row) => {
                const pk = row[cols[0]];
                const isSelected =
                  selectedPk != null && String(selectedPk) === String(pk);
                return html`
                  <${Table.Row} key=${pk} tabindex="0" aria-label=${"Open row " + pk} onKeyDown=${(
                    e,
                  ) => {
                    if (
                      e.target === e.currentTarget &&
                      (e.key === "Enter" || e.key === " ")
                    ) {
                      e.preventDefault();
                      setShowCreate(false);
                      setCreateErr("");
                      setSelectedPk(String(pk));
                    }
                  }} variant=${isSelected ? "selected" : "default"} onClick=${() => {
                    setShowCreate(false);
                    setCreateErr("");
                    setSelectedPk(String(pk));
                  }}>
                    ${displayCols.map((c) => {
                      const val = row[c];
                      const ro =
                        isReadOnly(schema, c) ||
                        isAutoField(schema, c) ||
                        val === "[REDACTED]";
                      const f = schema ? schema[c] : null;
                      const isFile =
                        f &&
                        (f.type === "fileSingle" || f.type === "fileMulti");
                      const isObj = typeof val === "object" && val !== null;
                      let display;
                      if (isFile && f.type === "fileSingle" && val && val.url) {
                        const isImage =
                          val.mime && val.mime.startsWith("image/");
                        const fileName = fileDisplayName(val) || "file";
                        display = html`<span class="file-display"
                          >${isImage
                            ? html`<img
                                class="file-thumb"
                                src=${val.url}
                                alt=${fileName}
                              />`
                            : ""}<a
                            href=${val.url}
                            target="_blank"
                            class="file-link"
                            onClick=${(e) => e.stopPropagation()}
                            >${fileName}</a
                          ></span
                        >`;
                      } else if (
                        isFile &&
                        f.type === "fileMulti" &&
                        Array.isArray(val) &&
                        val.length > 0
                      ) {
                        display =
                          val.length + " file" + (val.length !== 1 ? "s" : "");
                      } else if (isFile) {
                        display = null;
                      } else {
                        display = isObj ? JSON.stringify(val) : val;
                      }
                      const rendered =
                        display == null
                          ? html`<span class="null">null</span>`
                          : typeof display === "object"
                            ? display
                            : truncate(String(display));
                      return html`<${Table.Cell} key=${c} style=${{ order: mobileCols.indexOf(c) }} data-label=${c} class=${(ro ? "td-readonly " : "") + (mobileCols.includes(c) ? "mobile-field" : "mobile-secondary")}>${rendered}</${Table.Cell}>`;
                    })}
                    <${Table.Cell} class="td-actions"><span class="mobile-row-hint">Open row to see all fields ${icons.chevRight}</span>
                      ${
                        archive
                          ? html`
                            <${Action} class="btn-icon" onClick=${(e) => {
                              e.stopPropagation();
                              restoreRow(pk);
                            }} title="Restore">${icons.upload}</${Action}>
                            <${Action} class="btn-icon danger" onClick=${(
                              e,
                            ) => {
                              e.stopPropagation();
                              deleteArchivedRow(pk);
                            }} title="Delete Permanently">${icons.trash}</${Action}>
                          `
                          : html`
                            <${Action} class="btn-icon" disabled=${isReadOnlyTable} onClick=${(
                              e,
                            ) => {
                              e.stopPropagation();
                              archiveRow(pk);
                            }} title="Archive">${icons.download}</${Action}>
                            <${Action} class="btn-icon danger" disabled=${isReadOnlyTable} onClick=${(
                              e,
                            ) => {
                              e.stopPropagation();
                              deleteRow(pk);
                            }} title="Delete">${icons.trash}</${Action}>
                          `
                      }
                    </${Table.Cell}>
                  </${Table.Row}>
                `;
              })}
            </${Table.Body}>
          </${Table}>
        </div>
        <${RowEditor} open=${showCreate || !!selectedRow} mode=${showCreate ? "create" : "edit"} tableName=${tableName} schema=${schema} cols=${cols} row=${selectedRow} createError=${createErr} readOnly=${archive} onClose=${() => {
          setShowCreate(false);
          setSelectedPk(null);
        }} onSave=${saveRow} onCreate=${submitCreate} onReload=${loadRows} />
      </div>

      ${
        totalPages > 1 &&
        html`
        <div class="pagination">
          <${Action} class="btn-icon" aria-label="Previous page" disabled=${page <= 1} onClick=${() => setPage((p) => p - 1)}>${icons.chevLeft}</${Action}>
          <span class="page-info">Page ${page} of ${totalPages}</span>
          <${Action} class="btn-icon" aria-label="Next page" disabled=${page >= totalPages} onClick=${() => setPage((p) => p + 1)}>${icons.chevRight}</${Action}>
        </div>
      `
      }
    </div>
  `;
}

function fmtCount(n) {
  return (Number(n) || 0).toLocaleString();
}

function fmtMs(n) {
  const v = Number(n) || 0;
  if (v >= 1000) return (v / 1000).toFixed(2) + "s";
  return v.toFixed(2) + "ms";
}

function fmtPct(n) {
  return (Number(n) || 0).toFixed(2) + "%";
}

function fmtBytes(n) {
  const v = Number(n) || 0;
  if (v < 1024) return v.toFixed(0) + " B";
  const units = ["KB", "MB", "GB", "TB"];
  let x = v / 1024;
  let i = 0;
  while (x >= 1024 && i < units.length - 1) {
    x /= 1024;
    i++;
  }
  return x.toFixed(2) + " " + units[i];
}

function fmtTime(ts) {
  const n = Number(ts);
  if (!Number.isFinite(n) || n <= 0) return "-";
  return new Date(n).toLocaleString();
}

function fmtAgo(ts) {
  const n = Number(ts);
  if (!Number.isFinite(n) || n <= 0) return "";
  const diff = Math.max(0, Date.now() - n);
  const sec = Math.floor(diff / 1000);
  if (sec < 60) return sec + "s ago";
  const min = Math.floor(sec / 60);
  if (min < 60) return min + "m ago";
  const hr = Math.floor(min / 60);
  if (hr < 24) return hr + "h ago";
  const day = Math.floor(hr / 24);
  return day + "d ago";
}

function parseDetailsObject(raw) {
  if (!raw) return null;
  if (typeof raw === "object") return raw;
  if (typeof raw !== "string") return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function extractTraceEvents(detailsObj) {
  if (!detailsObj || typeof detailsObj !== "object") return [];
  if (Array.isArray(detailsObj.trace)) return detailsObj.trace;
  if (Array.isArray(detailsObj.spans)) return detailsObj.spans;
  return [];
}

function fileDisplayName(file) {
  if (!file || typeof file !== "object") return "";
  if (typeof file.name === "string" && file.name) return file.name;
  if (typeof file.path === "string" && file.path) {
    const parts = file.path.split("/");
    return parts[parts.length - 1] || file.path;
  }
  if (typeof file.url === "string" && file.url) {
    const clean = file.url.split("?")[0];
    const parts = clean.split("/");
    return parts[parts.length - 1] || clean;
  }
  return "";
}

function normalizeFileItems(value) {
  return Array.isArray(value)
    ? value.filter((file) => file && typeof file === "object")
    : [];
}

function isImageFile(file) {
  return !!(
    file &&
    typeof file.mime === "string" &&
    file.mime.startsWith("image/")
  );
}

function toNum(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function buildTraceTimeline(events) {
  const rows = (Array.isArray(events) ? events : []).map((e, i) => {
    const ms = toNum(e && (e.ms != null ? e.ms : e.durationMs), 0);
    const s = toNum(e && e.started, NaN);
    const f = toNum(e && e.finished, NaN);
    return {
      i,
      raw: e || {},
      ms: Math.max(0, ms),
      hasRange: Number.isFinite(s) && Number.isFinite(f) && f >= s,
      start: s,
      finish: f,
    };
  });
  if (rows.length === 0) return { rows: [], totalMs: 0 };

  let cursor = 0;
  for (const r of rows) {
    if (r.hasRange) {
      r.relStart = r.start;
      r.relEnd = r.finish;
    } else {
      r.relStart = cursor;
      r.relEnd = cursor + r.ms;
      cursor = r.relEnd;
    }
  }

  const minStart = Math.min(...rows.map((r) => toNum(r.relStart, 0)));
  const maxEnd = Math.max(...rows.map((r) => toNum(r.relEnd, 0)));
  const totalMs = Math.max(1, maxEnd - minStart);

  for (const r of rows) {
    const startMs = Math.max(0, toNum(r.relStart, 0) - minStart);
    const endMs = Math.max(startMs, toNum(r.relEnd, startMs));
    const dur = Math.max(1, endMs - startMs);
    r.offsetPct = (startMs / totalMs) * 100;
    r.widthPct = (dur / totalMs) * 100;
  }

  return { rows, totalMs };
}

function AnalyticsPanel({ page = "volume" }) {
  const isPerformance = page === "performance";
  const [supported, setSupported] = useState(true);
  const [loaded, setLoaded] = useState(false);
  const [windowSize, setWindowSize] = useState("24h");
  const [config, setConfig] = useState(null);
  const [series, setSeries] = useState({
    points: [],
    summary: { count: 0, errorCount: 0, errorRate: 0, avgMs: 0, p95Ms: 0 },
    routes: [],
    droppedEvents: 0,
    retentionHours: 168,
  });
  const [runtimeStats, setRuntimeStats] = useState(null);
  const [selectedType, setSelectedType] = useState("request");
  const [routeName, setRouteName] = useState("");
  const [logs, setLogs] = useState([]);
  const [logPage, setLogPage] = useState(1);
  const [logPages, setLogPages] = useState(1);
  const [logTotal, setLogTotal] = useState(0);
  const [logSearch, setLogSearch] = useState("");
  const [logSearchQuery, setLogSearchQuery] = useState("");
  const [selectedLog, setSelectedLog] = useState(null);
  const [slowMs, setSlowMs] = useState(250);

  const loadConfig = useCallback(async () => {
    try {
      const data = await api("/analytics/config");
      if (!data || !data.enabled) {
        setSupported(false);
        return;
      }
      setSupported(true);
      setConfig(data);
      const routeTypes =
        Array.isArray(data.routeTypes) && data.routeTypes.length > 0
          ? data.routeTypes
          : ["request"];
      setSelectedType((prev) =>
        routeTypes.includes(prev) ? prev : routeTypes[0],
      );
    } catch {
      setSupported(false);
    }
  }, []);

  const loadSeries = useCallback(async () => {
    if (!supported) return;
    const params = new URLSearchParams({ window: windowSize });
    if (selectedType) params.set("routeType", selectedType);
    if (routeName) params.set("routeName", routeName);
    try {
      const data = await api("/analytics/timeseries?" + params.toString());
      if (data && data.data) {
        setSeries(data.data);
      }
    } catch (err) {
      showToast(err.message || "Failed to load metrics", "error");
    }
  }, [supported, windowSize, selectedType, routeName]);

  const loadLogs = useCallback(
    async (pageArg, queryArg) => {
      if (!supported) return;
      const pg = pageArg || logPage;
      const q = queryArg !== undefined ? queryArg : logSearchQuery;
      const params = new URLSearchParams({ page: String(pg), limit: "50" });
      if (selectedType) params.set("routeType", selectedType);
      if (q) {
        const isFilter = /[=<>~]|!=|!~/.test(q);
        params.set(isFilter ? "filter" : "search", q);
      }
      try {
        const data = await api("/analytics/logs?" + params.toString());
        if (data) {
          setLogs(data.rows || []);
          setLogPages(data.pages || 1);
          setLogTotal(data.total || 0);
        }
      } catch (err) {
        showToast(err.message || "Failed to load request logs", "error");
      }
    },
    [supported, logPage, logSearchQuery, selectedType],
  );

  const loadRuntime = useCallback(async () => {
    if (!supported) return;
    try {
      const data = await api("/analytics/runtime");
      if (data && data.data) setRuntimeStats(data.data);
    } catch {
      // Best effort: runtime stats are informational only.
    }
  }, [supported]);

  useEffect(() => {
    loadConfig().finally(() => setLoaded(true));
  }, []);

  useEffect(() => {
    if (!supported) return;
    loadSeries();
    loadRuntime();
  }, [supported, loadSeries]);

  useEffect(() => {
    setRouteName("");
    setLogPage(1);
    setSelectedLog(null);
  }, [selectedType]);

  useEffect(() => {
    if (!supported) return;
    loadLogs(logPage, logSearchQuery);
  }, [supported, logPage, logSearchQuery, loadLogs]);

  useEffect(() => {
    if (!selectedLog) return;
    const found = logs.find((l) => String(l.id) === String(selectedLog.id));
    if (!found) setSelectedLog(null);
    else if (found !== selectedLog) setSelectedLog(found);
  }, [logs, selectedLog]);

  useEffect(() => {
    if (!supported) return;
    const id = setInterval(() => {
      loadConfig();
      loadSeries();
      loadLogs(logPage, logSearchQuery);
      loadRuntime();
    }, 10000);
    return () => clearInterval(id);
  }, [
    supported,
    loadConfig,
    loadSeries,
    loadLogs,
    loadRuntime,
    logPage,
    logSearchQuery,
  ]);

  const applyLogFilter = useCallback(() => {
    setLogSearchQuery(logSearch.trim());
    setLogPage(1);
  }, [logSearch]);

  const onLogSearchKey = useCallback(
    (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        applyLogFilter();
      }
    },
    [applyLogFilter],
  );

  if (!loaded) {
    return html`<div class="empty-state">
      <div class="empty-title">Loading analytics…</div>
    </div>`;
  }
  if (!supported) {
    return html`<div class="empty-state">
      <div class="empty-title">Analytics unavailable</div>
      <div class="empty-desc">
        This runtime does not expose request analytics endpoints.
      </div>
    </div>`;
  }

  const summary = series.summary || {
    count: 0,
    errorCount: 0,
    errorRate: 0,
    avgMs: 0,
    p95Ms: 0,
  };
  const routeChips = (series.routes || []).slice(0, 12);
  const slowLogs = (logs || []).filter(
    (r) => Number(r.durationMs) >= Number(slowMs || 0),
  );
  const selectedDetails = parseDetailsObject(
    selectedLog && selectedLog.details,
  );
  const traceEvents = extractTraceEvents(selectedDetails);
  const traceTimeline = buildTraceTimeline(traceEvents);
  const routeTypes =
    Array.isArray(config?.routeTypes) && config.routeTypes.length > 0
      ? config.routeTypes
      : ["request"];
  const typeLabelPlural = routeTypeLabel(selectedType, true);
  const typeLabelSingle = routeTypeLabel(selectedType, false);

  return html`
    <div class="analytics-panel">
      <div class="analytics-toolbar">
        <div class="analytics-title">${icons.chart} ${typeLabelPlural} ${isPerformance ? "Performance" : "Volume"}</div>
        <div class="analytics-actions">
          <${FieldSelect} class="analytics-select" value=${windowSize} onChange=${(
            e,
          ) => {
            setWindowSize(e.target.value);
            setLogPage(1);
          }}>
            <${FieldSelect.Option} value="1h">Last 1 hour</${FieldSelect.Option}>
            <${FieldSelect.Option} value="6h">Last 6 hours</${FieldSelect.Option}>
            <${FieldSelect.Option} value="24h">Last 24 hours</${FieldSelect.Option}>
            <${FieldSelect.Option} value="7d">Last 7 days</${FieldSelect.Option}>
          </${FieldSelect}>
        </div>
      </div>

      <div class="analytics-body">
        ${
          routeTypes.length > 1 &&
          html`
            <div class="analytics-route-list">
              ${routeTypes.map(
                (type) => html`
              <${Action}
                key=${type}
                class=${"analytics-route-chip" + (selectedType === type ? " active" : "")}
                onClick=${() => setSelectedType(type)}
              >
                ${routeTypeLabel(type, true)}
              </${Action}>
            `,
              )}
            </div>
          `
        }
        <div class="analytics-layout">
          <div class="analytics-main">
            <div class="analytics-cards">
              <${LayerCard} class="analytics-card"><div class="analytics-card-label">${typeLabelPlural}</div><div class="analytics-card-value">${fmtCount(summary.count)}</div></${LayerCard}>
              <${LayerCard} class="analytics-card"><div class="analytics-card-label">Errors</div><div class="analytics-card-value">${fmtCount(summary.errorCount)}</div></${LayerCard}>
              <${LayerCard} class="analytics-card"><div class="analytics-card-label">Error rate</div><div class="analytics-card-value">${fmtPct(summary.errorRate)}</div></${LayerCard}>
              <${LayerCard} class="analytics-card"><div class="analytics-card-label">Average duration</div><div class="analytics-card-value">${fmtMs(summary.avgMs)}</div></${LayerCard}>
              <${LayerCard} class="analytics-card"><div class="analytics-card-label">P95 duration</div><div class="analytics-card-value">${fmtMs(summary.p95Ms)}</div></${LayerCard}>
              ${isPerformance && html`<${LayerCard} class="analytics-card"><div class="analytics-card-label">Slow (${fmtMs(slowMs)}+)</div><div class="analytics-card-value">${fmtCount(slowLogs.length)}</div></${LayerCard}>`}
              ${isPerformance && html`<${LayerCard} class="analytics-card"><div class="analytics-card-label">Routes</div><div class="analytics-card-value">${fmtCount((series.routes || []).length)}</div></${LayerCard}>`}
              ${!isPerformance && html`<${LayerCard} class="analytics-card"><div class="analytics-card-label">Dropped events</div><div class="analytics-card-value">${fmtCount(series.droppedEvents)}</div></${LayerCard}>`}
            </div>

            <${LayerCard} class="analytics-chart-wrap">
              <div class="analytics-chart-title">
                <span>${isPerformance ? `${typeLabelPlural} duration and volume over time` : `${typeLabelPlural} volume over time`}</span>
                <span>Retention: ${Number(series.retentionHours || (config && config.retentionHours) || 168).toFixed(0)}h</span>
              </div>
              ${
                isPerformance
                  ? html`<${AnalyticsChart} points=${series.points || []} />`
                  : html`<${RequestsBarChart} points=${series.points || []} />`
              }
            </${LayerCard}>

            ${
              isPerformance &&
              html`
              <div class="analytics-route-list">
                <${Action} class=${"analytics-route-chip" + (!routeName ? " active" : "")} onClick=${() => {
                  setRouteName("");
                }}>All routes</${Action}>
                ${routeChips.map(
                  (rt) => html`
                  <${Action}
                    key=${rt.routeType + ":" + rt.routeName}
                    class=${"analytics-route-chip" + (routeName === rt.routeName ? " active" : "")}
                    onClick=${() => {
                      setRouteName(rt.routeName);
                      setLogPage(1);
                    }}
                  >
                    ${rt.routeName || "(unnamed)"} · ${fmtCount(rt.count)}
                  </${Action}>
                `,
                )}
              </div>
            `
            }

            <${LayerCard} class="analytics-logs">
              <div class="panel-toolbar">
                <div class="panel-title">
                  <span class="panel-title-icon">${icons.table}</span>
                  ${isPerformance ? `${typeLabelSingle} Performance` : `${typeLabelSingle} Events`}
                  <span class="panel-meta">${fmtCount(logTotal)} rows</span>
                </div>
                <div class="panel-actions">
                  ${
                    isPerformance &&
                    html`
                      <div class="search-box">
                        <span style="color:var(--text-faint);font-size:11px"
                          >slow ≥</span
                        >
                        <${FieldInput}
                          type="number"
                          min="1"
                          value=${String(slowMs)}
                          onInput=${(e) =>
                            setSlowMs(Number(e.target.value) || 0)}
                          style="width:80px"
                        />
                        <span style="color:var(--text-faint);font-size:11px"
                          >ms</span
                        >
                      </div>
                    `
                  }
                  <div class="search-box">
                    ${icons.search}
                    <${FieldInput} type="text" placeholder='routeName~"get_" durationMs>=20' value=${logSearch} onInput=${(e) => setLogSearch(e.target.value)} onKeyDown=${onLogSearchKey} />
                    <span class="filter-help" title="= equals  != not equal  ~ contains  !~ not contains&#10;> >= < <= comparisons&#10;&& and  || or  () grouping&#10;&#10;Plain text = search in route/error/details">?</span>
                    <${Action} class="btn-filter" onClick=${applyLogFilter}>${icons.chevRight}</${Action}>
                  </div>
                </div>
              </div>

              <div class="table-container panel-table-scroll">
                <${Table}>
                  <${Table.Header}>
                    <${Table.Row}>
                      <${Table.Head}><span class="th-name">time</span></${Table.Head}>
                      <${Table.Head}><span class="th-name">route</span></${Table.Head}>
                      <${Table.Head}><span class="th-name">status</span></${Table.Head}>
                      <${Table.Head}><span class="th-name">duration</span></${Table.Head}>
                      <${Table.Head}><span class="th-name">error</span></${Table.Head}>
                      <${Table.Head}><span class="th-name">details</span></${Table.Head}>
                    </${Table.Row}>
                  </${Table.Header}>
                  <${Table.Body}>
                    ${logs.length === 0 && html`<${Table.Row}><${Table.Cell} colspan="6" class="empty-row">No logs found</${Table.Cell}></${Table.Row}>`}
                    ${logs.map(
                      (row) => html`
                      <${Table.Row} key=${row.id} class="log-row" onClick=${() => setSelectedLog(row)}>
                        <${Table.Cell}>${fmtTime(row.ts)}</${Table.Cell}>
                        <${Table.Cell}>${row.routeType}/${row.routeName}</${Table.Cell}>
                        <${Table.Cell}>${row.status}</${Table.Cell}>
                        <${Table.Cell}>
                          ${fmtMs(row.durationMs)}
                          ${isPerformance && Number(row.durationMs) >= Number(slowMs || 0) && html`<${Badge} variant="secondary">slow</${Badge}>`}
                        </${Table.Cell}>
                        <${Table.Cell}>${row.errorMessage ? truncate(row.errorMessage, 90) : html`<span class="null">-</span>`}</${Table.Cell}>
                        <${Table.Cell} title=${row.details || ""}>${row.details ? truncate(row.details, 120) : html`<span class="null">-</span>`}</${Table.Cell}>
                      </${Table.Row}>
                    `,
                    )}
                  </${Table.Body}>
                </${Table}>
              </div>

              ${
                logPages > 1 &&
                html`
                <div class="pagination">
                  <${Action} class="btn-icon" disabled=${logPage <= 1} onClick=${() => setLogPage((p) => p - 1)}>${icons.chevLeft}</${Action}>
                  <span class="page-info">Page ${logPage} of ${logPages}</span>
                  <${Action} class="btn-icon" disabled=${logPage >= logPages} onClick=${() => setLogPage((p) => p + 1)}>${icons.chevRight}</${Action}>
                </div>
              `
              }
            </${LayerCard}>
          </div>

          ${
            selectedLog &&
            html`
            <aside class="analytics-detail">
              <div class="analytics-detail-header">
                <div>
                  <div class="analytics-detail-title">${typeLabelSingle} Detail</div>
                  <div class="panel-meta">${selectedLog.routeType}/${selectedLog.routeName}</div>
                </div>
                <${Action} class="btn-icon" onClick=${() => setSelectedLog(null)}>${icons.x}</${Action}>
              </div>
              <div class="analytics-detail-body">
                <div class="detail-row"><div class="detail-key">time</div><div class="detail-val">${fmtTime(selectedLog.ts)}</div></div>
                <div class="detail-row"><div class="detail-key">status</div><div class="detail-val">${String(selectedLog.status ?? "-")}</div></div>
                <div class="detail-row"><div class="detail-key">duration</div><div class="detail-val">${fmtMs(selectedLog.durationMs)}</div></div>
                <div class="detail-row"><div class="detail-key">route</div><div class="detail-val">${selectedLog.routeType}/${selectedLog.routeName}</div></div>
                <div class="detail-row"><div class="detail-key">error</div><div class="detail-val">${selectedLog.errorMessage || "-"}</div></div>
                <div class="detail-row">
                  <div class="detail-key">trace</div>
                  <div class="detail-val">
                    ${
                      traceEvents.length > 0
                        ? html`<div class="trace-timeline">
                            <div class="trace-axis">
                              <div
                                class="trace-axis-grid"
                                style=${{ left: "25%" }}
                              ></div>
                              <div
                                class="trace-axis-grid"
                                style=${{ left: "50%" }}
                              ></div>
                              <div
                                class="trace-axis-grid"
                                style=${{ left: "75%" }}
                              ></div>
                            </div>
                            <div class="trace-axis-labels">
                              <span>0ms</span>
                              <span>${fmtMs(traceTimeline.totalMs / 2)}</span>
                              <span>${fmtMs(traceTimeline.totalMs)}</span>
                            </div>
                            <div class="trace-list">
                              ${traceTimeline.rows.map((r) => {
                                const t = r.raw;
                                return html`
                                  <div key=${String(r.i)} class="trace-item">
                                    <div>
                                      <div class="trace-row-top">
                                        <div class="trace-name">
                                          ${t.op ||
                                          t.name ||
                                          t.stage ||
                                          "operation"}
                                        </div>
                                        <div class="trace-row-ms">
                                          ${fmtMs(r.ms)}
                                        </div>
                                      </div>
                                      <div class="trace-meta">
                                        ${t.table
                                          ? "table=" + t.table + " "
                                          : ""}${t.index
                                          ? "index=" + t.index + " "
                                          : ""}${t.rows != null
                                          ? "rows=" + t.rows + " "
                                          : ""}${t.note || ""}
                                      </div>
                                      <div class="trace-bar-wrap">
                                        <div class="trace-bar-track">
                                          <div
                                            class="trace-bar"
                                            style=${{
                                              left: `${r.offsetPct}%`,
                                              width: `${r.widthPct}%`,
                                            }}
                                          ></div>
                                        </div>
                                      </div>
                                    </div>
                                  </div>
                                `;
                              })}
                            </div>
                          </div>`
                        : "Per-request DB trace not available from backend yet (no scan/index timing emitted)."
                    }
                  </div>
                </div>
                <div class="detail-row">
                  <div class="detail-key">details</div>
                  <div class="detail-val">
                    ${
                      selectedDetails
                        ? html`<pre class="detail-pre">
${JSON.stringify(selectedDetails, null, 2)}</pre
                          >`
                        : selectedLog.details
                          ? html`<pre class="detail-pre">
${String(selectedLog.details)}</pre
                            >`
                          : "-"
                    }
                  </div>
                </div>
              </div>
            </div></${Dialog}></${Dialog.Root}>
          `
          }
        </div>
      </div>
    </div>
  `;
}

function ObservePanel() {
  const [loaded, setLoaded] = useState(false);
  const [supported, setSupported] = useState(true);
  const [report, setReport] = useState(null);
  const [runtimeStats, setRuntimeStats] = useState(null);
  const [query, setQuery] = useState("");

  const load = useCallback(async () => {
    try {
      const [indexResp, runtimeResp] = await Promise.all([
        api("/analytics/indexes"),
        api("/analytics/runtime"),
      ]);
      if (!indexResp || !indexResp.data) {
        setSupported(false);
        return;
      }
      setReport(indexResp.data);
      if (runtimeResp && runtimeResp.data) setRuntimeStats(runtimeResp.data);
      setSupported(true);
    } catch {
      setSupported(false);
    } finally {
      setLoaded(true);
    }
  }, []);

  useEffect(() => {
    load();
    const id = setInterval(load, 10000);
    return () => clearInterval(id);
  }, [load]);

  if (!loaded) {
    return html`<div class="empty-state">
      <div class="empty-title">Loading observability…</div>
    </div>`;
  }
  if (!supported) {
    return html`<div class="empty-state">
      <div class="empty-title">Observe unavailable</div>
      <div class="empty-desc">
        This runtime does not expose index diagnostics.
      </div>
    </div>`;
  }

  const tables = Array.isArray(report?.tables) ? report.tables : [];
  const q = query.trim().toLowerCase();
  const filtered = q
    ? tables.filter(
        (t) =>
          (t.table || "").toLowerCase().includes(q) ||
          (t.secondaryIndexes || []).some(
            (i) =>
              (i.name || "").toLowerCase().includes(q) ||
              (i.type || "").toLowerCase().includes(q) ||
              (i.fields || []).join(",").toLowerCase().includes(q),
          ),
      )
    : tables;

  const secondaryEntries = filtered.reduce(
    (sum, t) =>
      sum +
      (t.secondaryIndexes || []).reduce(
        (inner, i) => inner + (Number(i.entryCount) || 0),
        0,
      ),
    0,
  );

  return html`
    <div class="observe-panel">
      <div class="analytics-toolbar">
        <div class="analytics-title">${icons.pulse} Observe</div>
        <div class="analytics-actions">
          <div class="search-box">
            ${icons.search}
            <${FieldInput} type="text" placeholder="Filter tables/indexes…" value=${query} onInput=${(e) => setQuery(e.target.value)} />
          </div>
        </div>
      </div>

      <div class="observe-body">
        <div class="analytics-cards">
          <${LayerCard} class="analytics-card"><div class="analytics-card-label">Tables</div><div class="analytics-card-value">${fmtCount(report?.tableCount)}</div></${LayerCard}>
          <${LayerCard} class="analytics-card"><div class="analytics-card-label">Primary Indexes</div><div class="analytics-card-value">${fmtCount(report?.primaryIndexCount)}</div></${LayerCard}>
          <${LayerCard} class="analytics-card"><div class="analytics-card-label">Secondary Indexes</div><div class="analytics-card-value">${fmtCount(report?.secondaryIndexCount)}</div></${LayerCard}>
          <${LayerCard} class="analytics-card"><div class="analytics-card-label">Index Entries</div><div class="analytics-card-value">${fmtCount(secondaryEntries)}</div></${LayerCard}>
          <${LayerCard} class="analytics-card"><div class="analytics-card-label">Estimated Index Payload</div><div class="analytics-card-value">${fmtBytes(report?.estimatedPayloadBytes)}</div></${LayerCard}>
          <${LayerCard} class="analytics-card"><div class="analytics-card-label">Heap In Use</div><div class="analytics-card-value">${fmtBytes(runtimeStats?.heapInuse)}</div></${LayerCard}>
          <${LayerCard} class="analytics-card"><div class="analytics-card-label">Heap Idle</div><div class="analytics-card-value">${fmtBytes(runtimeStats?.heapIdle)}</div></${LayerCard}>
          <${LayerCard} class="analytics-card"><div class="analytics-card-label">Go Sys</div><div class="analytics-card-value">${fmtBytes(runtimeStats?.sys)}</div></${LayerCard}>
        </div>

        <${LayerCard} class="observe-table-wrap">
          <div class="panel-toolbar">
            <div class="panel-title">
              <span class="panel-title-icon">${icons.table}</span>
              Indexes By Table
              <span class="panel-meta">${fmtCount(filtered.length)} tables</span>
            </div>
            <div class="panel-meta">snapshot ${fmtTime(report?.generatedAtUnixMilli)}</div>
          </div>
          <div class="table-container panel-table-scroll">
            <${Table}>
              <${Table.Header}>
                <${Table.Row}>
                  <${Table.Head}><span class="th-name">table</span></${Table.Head}>
                  <${Table.Head}><span class="th-name">index</span></${Table.Head}>
                  <${Table.Head}><span class="th-name">type</span></${Table.Head}>
                  <${Table.Head}><span class="th-name">entries</span></${Table.Head}>
                  <${Table.Head}><span class="th-name">keys</span></${Table.Head}>
                  <${Table.Head}><span class="th-name">est. payload</span></${Table.Head}>
                  <${Table.Head}><span class="th-name">ready</span></${Table.Head}>
                </${Table.Row}>
              </${Table.Header}>
              <${Table.Body}>
                ${filtered.length === 0 && html`<${Table.Row}><${Table.Cell} colspan="7" class="empty-row">No matching tables/indexes</${Table.Cell}></${Table.Row}>`}
                ${filtered.map(
                  (t) => html`
                  <${Table.Row} key=${t.table + ":primary"}>
                    <${Table.Cell}>${t.table}<span class="th-type"> (${fmtCount(t.rows)} rows)</span></${Table.Cell}>
                    <${Table.Cell} class="mono-sm">${t.primaryKeyField || "id"} (primary)</${Table.Cell}>
                    <${Table.Cell}>primary</${Table.Cell}>
                    <${Table.Cell}>${fmtCount(t.primaryKeys)}</${Table.Cell}>
                    <${Table.Cell}>${fmtCount(t.primaryKeys)}</${Table.Cell}>
                    <${Table.Cell}>${fmtBytes(t.primaryEstimatedBytes)}</${Table.Cell}>
                    <${Table.Cell}>yes</${Table.Cell}>
                  </${Table.Row}>
                  ${(t.secondaryIndexes || []).map(
                    (idx) => html`
                    <${Table.Row} key=${t.table + ":" + idx.name}>
                      <${Table.Cell}><span class="null">↳</span></${Table.Cell}>
                      <${Table.Cell} class="mono-sm">${idx.name}${idx.fields && idx.fields.length ? " [" + idx.fields.join(", ") + "]" : ""}</${Table.Cell}>
                      <${Table.Cell}>${idx.type}${idx.unique ? " (u)" : ""}</${Table.Cell}>
                      <${Table.Cell}>${fmtCount(idx.entryCount)}</${Table.Cell}>
                      <${Table.Cell}>${fmtCount(idx.keyCount)}</${Table.Cell}>
                      <${Table.Cell}>${fmtBytes(idx.estimatedPayloadBytes)}</${Table.Cell}>
                      <${Table.Cell}>${t.secondaryReady ? "yes" : "building"}</${Table.Cell}>
                    </${Table.Row}>
                  `,
                  )}
                `,
                )}
              </${Table.Body}>
            </${Table}>
          </div>
        </${LayerCard}>
      </div>
    </div>
  `;
}

function MediaPanel() {
  const [rows, setRows] = useState([]);
  const [page, setPage] = useState(1);
  const [pages, setPages] = useState(1);
  const [total, setTotal] = useState(0);
  const [search, setSearch] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [orphansOnly, setOrphansOnly] = useState(false);
  const [loading, setLoading] = useState(false);

  const loadRows = useCallback(
    async (
      nextPage = page,
      nextSearch = searchQuery,
      nextOrphans = orphansOnly,
    ) => {
      setLoading(true);
      try {
        const params = new URLSearchParams({
          limit: "50",
          page: String(nextPage),
        });
        if (nextSearch) params.set("search", nextSearch);
        if (nextOrphans) params.set("orphans", "1");
        const data = await api("/media?" + params.toString());
        setRows(data?.rows || []);
        setTotal(data?.total || 0);
        setPages(Math.max(1, data?.pages || 1));
      } catch (err) {
        showToast(err.message || "Failed to load media", "error");
      } finally {
        setLoading(false);
      }
    },
    [page, searchQuery, orphansOnly],
  );

  useEffect(() => {
    loadRows(page, searchQuery, orphansOnly);
  }, [page, searchQuery, orphansOnly]);

  const applySearch = useCallback(() => {
    setPage(1);
    setSearchQuery(search.trim());
  }, [search]);

  const onSearchKey = useCallback(
    (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        applySearch();
      }
    },
    [applySearch],
  );

  return html`
    <div class="panel">
      <div class="panel-toolbar">
        <div class="panel-title">
          <span class="panel-title-icon">${icons.image}</span>
          <span>Media</span>
          <span class="panel-meta">${fmtCount(total)} files</span>
        </div>
        <div class="panel-actions">
          <div class="search-box">
            ${icons.search}
            <${FieldInput} type="text" placeholder="Search path, name, table, row..." value=${search} onInput=${(e) => setSearch(e.target.value)} onKeyDown=${onSearchKey} />
            <${Action} class="btn-filter" onClick=${applySearch}>${icons.chevRight}</${Action}>
          </div>
          <label class="btn" style="gap:6px">
            <${FieldInput} type="checkbox" checked=${orphansOnly} onChange=${(
              e,
            ) => {
              setPage(1);
              setOrphansOnly(e.target.checked);
            }} />
            Orphans only
          </label>
          <${Action} class="btn" onClick=${() => loadRows()} disabled=${loading}>${icons.refresh} Refresh</${Action}>
        </div>
      </div>
      <div class="table-layout">
        <div class="table-container">
          <${Table}>
            <${Table.Header}>
              <${Table.Row}>
                <${Table.Head}>File</${Table.Head}>
                <${Table.Head}>Location</${Table.Head}>
                <${Table.Head}>Type</${Table.Head}>
                <${Table.Head}>Disk</${Table.Head}>
                <${Table.Head}>Thumbs</${Table.Head}>
              </${Table.Row}>
            </${Table.Header}>
            <${Table.Body}>
              ${
                rows.length === 0
                  ? html`
                <${Table.Row}><${Table.Cell} colspan="5" class="empty-row">${loading ? "Loading media…" : "No media files found"}</${Table.Cell}></${Table.Row}>
              `
                  : rows.map((row, idx) => {
                      const key = row.path || String(idx);
                      const fileName =
                        row.name || fileDisplayName(row) || row.path || "file";
                      const location = row.tableName
                        ? `${row.tableName}/${row.rowId || "-"}/${row.fieldName || "-"}`
                        : "-";
                      return html`
                  <${Table.Row} key=${key}>
                    <${Table.Cell}>
                      <span class="file-display">
                        ${isImageFile(row) && row.url ? html`<img class="file-thumb" src=${row.url} alt=${fileName} />` : ""}
                        ${row.url ? html`<a class="file-link" href=${row.url} target="_blank">${fileName}</a>` : fileName}
                        ${row.orphaned ? html`<${Badge} variant="secondary">orphan</${Badge}>` : ""}
                      </span>
                    </${Table.Cell}>
                    <${Table.Cell} title=${row.path || ""}>
                      <div>${location}</div>
                      <div class="th-type">${row.path || ""}</div>
                    </${Table.Cell}>
                    <${Table.Cell}>${row.mime || "-"}</${Table.Cell}>
                    <${Table.Cell}>${fmtBytes(row.diskSize || row.refSize || 0)}</${Table.Cell}>
                    <${Table.Cell}>${fmtCount(row.thumbCount || 0)}${row.thumbBytes ? html`<span class="th-type"> · ${fmtBytes(row.thumbBytes)}</span>` : ""}</${Table.Cell}>
                  </${Table.Row}>
                `;
                    })
              }
            </${Table.Body}>
          </${Table}>
        </div>
      </div>
      <div class="pagination">
        <${Action} class="btn" disabled=${page <= 1} onClick=${() => setPage((p) => Math.max(1, p - 1))}>${icons.chevLeft} Prev</${Action}>
        <span class="page-info">Page ${page} of ${pages}</span>
        <${Action} class="btn" disabled=${page >= pages} onClick=${() => setPage((p) => Math.min(pages, p + 1))}>Next ${icons.chevRight}</${Action}>
      </div>
    </div>
  `;
}

function Toggle({ label, checked, onChange, disabled = false }) {
  return html`<${Switch}
    aria-label=${label}
    checked=${checked}
    onCheckedChange=${onChange}
    disabled=${disabled}
  />`;
}

function BackupsPage() {
  const [items, setItems] = useState([]);
  const [busy, setBusy] = useState(false);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [creating, setCreating] = useState(false);
  const [testing, setTesting] = useState(false);
  const [testError, setTestError] = useState("");
  const [testOk, setTestOk] = useState(false);
  const [showOptions, setShowOptions] = useState(true);
  const [settings, setSettings] = useState({
    cron: "",
    cronMaxKeep: 3,
    s3: {
      enabled: false,
      endpoint: "",
      bucket: "",
      region: "",
      accessKey: "",
      secret: "",
      forcePathStyle: false,
    },
  });
  const [originalSettings, setOriginalSettings] = useState({
    cron: "",
    cronMaxKeep: 3,
    s3: {
      enabled: false,
      endpoint: "",
      bucket: "",
      region: "",
      accessKey: "",
      secret: "",
      forcePathStyle: false,
    },
  });
  const [downloadBusy, setDownloadBusy] = useState({});
  const [deleteBusy, setDeleteBusy] = useState({});
  const [restoreBusy, setRestoreBusy] = useState({});
  const [uploading, setUploading] = useState(false);

  const clone = useCallback((value) => JSON.parse(JSON.stringify(value)), []);
  const hasChanges =
    JSON.stringify(settings) !== JSON.stringify(originalSettings);
  const autoEnabled = !!settings.cron;

  const normalizeSettings = useCallback(
    (incoming) => {
      const next = clone(incoming || {});
      if (!next.s3) next.s3 = {};
      return {
        cron: next.cron || "",
        cronMaxKeep: Number(next.cronMaxKeep || 3),
        s3: {
          enabled: !!next.s3.enabled,
          endpoint: next.s3.endpoint || "",
          bucket: next.s3.bucket || "",
          region: next.s3.region || "",
          accessKey: next.s3.accessKey || "",
          secret: next.s3.secret || "",
          forcePathStyle: !!next.s3.forcePathStyle,
        },
      };
    },
    [clone],
  );

  const maybeTestS3 = useCallback(
    async (candidate) => {
      const current = candidate || settings;
      if (!current?.s3?.enabled) {
        setTestError("");
        setTestOk(false);
        return;
      }
      setTesting(true);
      setTestError("");
      setTestOk(false);
      try {
        await api("/backups/settings/test-s3", {
          method: "POST",
          body: JSON.stringify({ s3: current.s3 }),
        });
        setTestOk(true);
      } catch (err) {
        setTestError(err.message || "S3 connection failed");
      } finally {
        setTesting(false);
      }
    },
    [settings],
  );

  const loadPage = useCallback(async () => {
    setLoading(true);
    try {
      const settingsData = await api("/backups/settings");
      const normalized = normalizeSettings(settingsData?.settings || {});
      setSettings(normalized);
      setOriginalSettings(clone(normalized));
      if (normalized.s3.enabled) {
        maybeTestS3(normalized);
      } else {
        setTestError("");
        setTestOk(false);
      }
      try {
        const listData = await api("/backups");
        setItems(
          (listData?.items || [])
            .slice()
            .sort((a, b) => new Date(b.modified) - new Date(a.modified)),
        );
        setBusy(!!listData?.busy || !!settingsData?.busy);
      } catch (err) {
        setItems([]);
        setBusy(!!settingsData?.busy);
        showToast(err.message || "Failed to load backups list", "error");
      }
    } catch (err) {
      showToast(err.message || "Failed to load backups", "error");
    } finally {
      setLoading(false);
    }
  }, [clone, normalizeSettings, maybeTestS3]);

  useEffect(() => {
    loadPage();
  }, []);

  const setCronEnabled = useCallback(
    (enabled) => {
      setSettings((prev) => {
        const next = clone(prev);
        next.cron = enabled ? next.cron || "0 0 * * *" : "";
        if (enabled && (!next.cronMaxKeep || next.cronMaxKeep < 1))
          next.cronMaxKeep = 3;
        return next;
      });
    },
    [clone],
  );

  const setS3Enabled = useCallback(
    (enabled) => {
      setSettings((prev) => {
        const next = clone(prev);
        next.s3.enabled = enabled;
        return next;
      });
      if (!enabled) {
        setTestError("");
        setTestOk(false);
      }
    },
    [clone],
  );

  const saveSettings = useCallback(async () => {
    setSaving(true);
    try {
      const payload = clone(settings);
      if (
        payload.s3?.secret === backupSecretMask &&
        originalSettings.s3?.secret === backupSecretMask
      ) {
        delete payload.s3.secret;
      }
      const res = await api("/backups/settings", {
        method: "PUT",
        body: JSON.stringify({ settings: payload }),
      });
      const normalized = normalizeSettings(res?.settings || payload);
      setSettings(normalized);
      setOriginalSettings(clone(normalized));
      setBusy(false);
      showToast("Backup settings saved");
      if (normalized.s3.enabled) {
        maybeTestS3(normalized);
      }
    } catch (err) {
      showToast(err.message || "Failed to save backup settings", "error");
    } finally {
      setSaving(false);
    }
  }, [settings, originalSettings, clone, normalizeSettings, maybeTestS3]);

  const createBackup = useCallback(async () => {
    setCreating(true);
    try {
      await api("/backups", { method: "POST", body: "{}" });
      showToast("Backup created");
      await loadPage();
    } catch (err) {
      showToast(err.message || "Backup failed", "error");
    } finally {
      setCreating(false);
    }
  }, [loadPage]);

  const downloadBackup = useCallback(async (key) => {
    setDownloadBusy((prev) => ({ ...prev, [key]: true }));
    try {
      const res = await fetch(
        "/_/api/backups/" + encodeURIComponent(key) + "/download",
        {
          headers: { Authorization: "Bearer " + TOKEN },
        },
      );
      if (!res.ok) throw new Error("Download failed");
      const blob = await res.blob();
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = key;
      a.click();
    } catch (err) {
      showToast(err.message || "Download failed", "error");
    } finally {
      setDownloadBusy((prev) => ({ ...prev, [key]: false }));
    }
  }, []);

  const deleteBackup = useCallback(
    async (key) => {
      if (!(await confirmAction('Delete backup "' + key + '"?'))) return;
      setDeleteBusy((prev) => ({ ...prev, [key]: true }));
      try {
        await apiDelete("/backups/" + encodeURIComponent(key));
        showToast("Backup deleted");
        await loadPage();
      } catch (err) {
        showToast(err.message || "Delete failed", "error");
      } finally {
        setDeleteBusy((prev) => ({ ...prev, [key]: false }));
      }
    },
    [loadPage],
  );

  const restoreBackup = useCallback(
    async (key) => {
      if (
        !(await confirmAction(
          'Restore "' +
            key +
            '"?\n\nThis replaces the current database data and files with the selected backup.',
        ))
      )
        return;
      setRestoreBusy((prev) => ({ ...prev, [key]: true }));
      try {
        const res = await api(
          "/backups/" + encodeURIComponent(key) + "/restore",
          {
            method: "POST",
            body: "{}",
          },
        );
        showToast(res?.message || "Backup restored");
        await loadPage();
      } catch (err) {
        showToast(err.message || "Restore failed", "error");
      } finally {
        setRestoreBusy((prev) => ({ ...prev, [key]: false }));
      }
    },
    [loadPage],
  );

  const uploadBackup = useCallback(
    async (event) => {
      const file = event?.target?.files?.[0];
      if (!file) return;
      setUploading(true);
      try {
        await apiUpload("/backups/upload", file);
        showToast("Backup uploaded");
        await loadPage();
      } catch (err) {
        showToast(err.message || "Upload failed", "error");
      } finally {
        event.target.value = "";
        setUploading(false);
      }
    },
    [loadPage],
  );

  const selectedPreset =
    settings.cron === "0 0 * * *"
      ? "daily"
      : settings.cron === "0 0 * * 0"
        ? "weekly"
        : settings.cron === "0 0 * * 1,3"
          ? "monwed"
          : settings.cron === "0 0 1 * *"
            ? "monthly"
            : "custom";

  return html`
    <div class="backups-page">
      <div class="backups-hero">
        <div><h1 class="backups-title">Backups</h1><p class="workflow-subtitle">Protect your data and restore it when you need to.</p></div>
        <div class="backups-actions">
          <${Action} class="btn" onClick=${loadPage} disabled=${loading || creating || saving}>${icons.refresh} Refresh</${Action}>
          <${UploadButton} disabled=${busy || loading || uploading}>
            ${uploading ? "Uploading…" : html`${icons.upload} Upload backup`}
            <${FieldInput} type="file" accept=".zip,application/zip" style="display:none" onChange=${uploadBackup} disabled=${busy || loading || uploading} />
          </${UploadButton}>
        </div>
      </div>

      <${LayerCard} class="backups-card">
        <div class="backups-list">
          ${
            loading
              ? html`<div class="backup-empty">Loading backups…</div>`
              : items.length === 0
                ? html`<div class="backup-empty">No backups yet.</div>`
                : items.map(
                    (item) => html`
            <div class="backup-row" key=${item.key}>
              <div class="backup-row-main">
                <span class="panel-title-icon">${icons.table}</span>
                <div style="min-width:0;flex:1">
                  <div class="backup-row-name" title=${item.key}>${item.key}</div>
                  <div class="backup-row-meta">${formatDateTime(item.modified)}</div>
                </div>
                <div class="backup-row-size">(${formatBytes(item.size)})</div>
              </div>
              <div class="backup-row-actions">
                <${Action} class="btn-icon" title="Download" disabled=${busy || !!downloadBusy[item.key] || !!restoreBusy[item.key] || !!deleteBusy[item.key]} onClick=${() => downloadBackup(item.key)}>${icons.download}</${Action}>
                <${Action} class="btn-icon" title="Restore" disabled=${busy || !!restoreBusy[item.key] || !!deleteBusy[item.key]} onClick=${() => restoreBackup(item.key)}>${icons.upload}</${Action}>
                <${Action} class="btn-icon danger" title="Delete" disabled=${busy || !!restoreBusy[item.key] || !!deleteBusy[item.key]} onClick=${() => deleteBackup(item.key)}>${icons.trash}</${Action}>
              </div>
            </div>
          `,
                  )
          }
          <${Action} class="backup-init" disabled=${busy || creating || loading} onClick=${createBackup}>
            ${busy || creating ? html`<span class="null">${creating ? "Creating backup…" : "Backup/restore in progress…"}</span>` : html`<span>${icons.upload}</span><span>Initialize new backup</span>`}
          </${Action}>
        </div>
      </${LayerCard}>

      <div>
        <${Action} class="btn backup-settings-toggle" onClick=${() => setShowOptions((v) => !v)}>
          <span>Backups options</span>
          <span>${showOptions ? icons.chevUp : icons.chevDown}</span>
        </${Action}>
      </div>

      ${
        showOptions
          ? html`
        <div class="backups-options">
          <div class="backup-option-block">
            <div class="backup-checkbox">
              <${Toggle} label="Automatic backups" checked=${autoEnabled} onChange=${setCronEnabled} disabled=${saving || busy} />
              <span>Enable auto backups</span>
            </div>
            ${
              autoEnabled
                ? html`
              <div class="backup-option-row">
                <div>
                  <label class="backup-label">Cron expression</label>
                  <div style="display:flex;gap:8px">
                    <${FieldInput} class="backup-input" type="text" value=${settings.cron} onInput=${(e) => setSettings((prev) => ({ ...prev, cron: e.target.value }))} />
                    <${FieldSelect} class="backup-select" value=${selectedPreset} onChange=${(
                      e,
                    ) => {
                      const value = e.target.value;
                      if (value === "daily")
                        setSettings((prev) => ({ ...prev, cron: "0 0 * * *" }));
                      if (value === "weekly")
                        setSettings((prev) => ({ ...prev, cron: "0 0 * * 0" }));
                      if (value === "monwed")
                        setSettings((prev) => ({
                          ...prev,
                          cron: "0 0 * * 1,3",
                        }));
                      if (value === "monthly")
                        setSettings((prev) => ({ ...prev, cron: "0 0 1 * *" }));
                    }}>
                      <${FieldSelect.Option} value="custom">Presets</${FieldSelect.Option}>
                      <${FieldSelect.Option} value="daily">Every day at 00:00h</${FieldSelect.Option}>
                      <${FieldSelect.Option} value="weekly">Every Sunday at 00:00h</${FieldSelect.Option}>
                      <${FieldSelect.Option} value="monwed">Every Mon and Wed at 00:00h</${FieldSelect.Option}>
                      <${FieldSelect.Option} value="monthly">Every first day of month at 00:00h</${FieldSelect.Option}>
                    </${FieldSelect}>
                  </div>
                  <div class="backup-help">Supports numeric list, steps, ranges or macros. The timezone is in UTC.</div>
                </div>
                <div>
                  <label class="backup-label">Max @auto backups to keep</label>
                  <${FieldInput} aria-label="Max @auto backups to keep" class="backup-input" type="number" min="1" value=${settings.cronMaxKeep} onInput=${(e) => setSettings((prev) => ({ ...prev, cronMaxKeep: Number(e.target.value || 0) }))} />
                </div>
              </div>
            `
                : null
            }
          </div>

          <div class="backup-option-block">
            <div class="backup-checkbox">
              <${Toggle} label="S3 backup storage" checked=${!!settings.s3.enabled} onChange=${setS3Enabled} disabled=${saving || busy} />
              <span>Store backups in S3 storage</span>
            </div>
            ${
              settings.s3.enabled
                ? html`
                    <div class="backup-option-row backup-option-row-3">
                      <div>
                        <label class="backup-label">Endpoint</label>
                        <${FieldInput}
                          aria-label="Endpoint"
                          class="backup-input"
                          type="text"
                          value=${settings.s3.endpoint}
                          onInput=${(e) =>
                            setSettings((prev) => ({
                              ...prev,
                              s3: { ...prev.s3, endpoint: e.target.value },
                            }))}
                        />
                      </div>
                      <div>
                        <label class="backup-label">Bucket</label>
                        <${FieldInput}
                          aria-label="Bucket"
                          class="backup-input"
                          type="text"
                          value=${settings.s3.bucket}
                          onInput=${(e) =>
                            setSettings((prev) => ({
                              ...prev,
                              s3: { ...prev.s3, bucket: e.target.value },
                            }))}
                        />
                      </div>
                      <div>
                        <label class="backup-label">Region</label>
                        <${FieldInput}
                          aria-label="Region"
                          class="backup-input"
                          type="text"
                          value=${settings.s3.region}
                          onInput=${(e) =>
                            setSettings((prev) => ({
                              ...prev,
                              s3: { ...prev.s3, region: e.target.value },
                            }))}
                        />
                      </div>
                    </div>
                    <div class="backup-option-row">
                      <div>
                        <label class="backup-label">Access key</label>
                        <${FieldInput}
                          aria-label="Access key"
                          class="backup-input"
                          type="text"
                          value=${settings.s3.accessKey}
                          onInput=${(e) =>
                            setSettings((prev) => ({
                              ...prev,
                              s3: { ...prev.s3, accessKey: e.target.value },
                            }))}
                        />
                      </div>
                      <div>
                        <label class="backup-label">Secret</label>
                        <${FieldInput}
                          aria-label="Secret"
                          class="backup-input"
                          type="password"
                          value=${settings.s3.secret}
                          onInput=${(e) =>
                            setSettings((prev) => ({
                              ...prev,
                              s3: { ...prev.s3, secret: e.target.value },
                            }))}
                        />
                      </div>
                    </div>
                    <label class="backup-checkbox">
                      <${FieldInput}
                        type="checkbox"
                        checked=${!!settings.s3.forcePathStyle}
                        onChange=${(e) =>
                          setSettings((prev) => ({
                            ...prev,
                            s3: {
                              ...prev.s3,
                              forcePathStyle: e.target.checked,
                            },
                          }))}
                      />
                      <span>Force path-style addressing</span>
                    </label>
                  `
                : null
            }
          </div>

          <div class="backup-footer">
            ${
              settings.s3.enabled && !hasChanges && !saving
                ? testing
                  ? html`<span class="backup-status-ok">Testing S3…</span>`
                  : testError
                    ? html`<span class="backup-status-error"
                        >${testError}</span
                      >`
                    : testOk
                      ? html`<span class="backup-status-ok"
                          >${icons.check} S3 connected successfully</span
                        >`
                      : null
                : null
            }
            ${
              hasChanges
                ? html`<${Action} class="btn" onClick=${() => {
                    setSettings(clone(originalSettings));
                    setTestError("");
                    setTestOk(false);
                  }}>Reset</${Action}>`
                : null
            }
            <${Action} class="btn btn-primary" disabled=${saving || !hasChanges || busy} onClick=${saveSettings}>Save changes</${Action}>
          </div>
        </div>
      `
          : null
      }
    </div>
  `;
}

function MailSettingsPage() {
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [testEmail, setTestEmail] = useState("");
  const [testTemplate, setTestTemplate] = useState("");
  const [testOk, setTestOk] = useState(false);
  const [testError, setTestError] = useState("");
  const [settings, setSettings] = useState({
    appName: "",
    appURL: "",
    senderName: "",
    senderAddress: "",
    useSMTP: true,
    smtp: {
      host: "",
      port: 587,
      username: "",
      password: "",
      tls: "auto",
      authMethod: "plain",
      localName: "",
    },
    templates: {
      verification: { subject: "", body: "" },
      "email-change": { subject: "", body: "" },
      "password-reset": { subject: "", body: "" },
    },
  });
  const [originalSettings, setOriginalSettings] = useState({
    appName: "",
    appURL: "",
    senderName: "",
    senderAddress: "",
    useSMTP: true,
    smtp: {
      host: "",
      port: 587,
      username: "",
      password: "",
      tls: "auto",
      authMethod: "plain",
      localName: "",
    },
    templates: {
      verification: { subject: "", body: "" },
      "email-change": { subject: "", body: "" },
      "password-reset": { subject: "", body: "" },
    },
  });

  const clone = useCallback((value) => JSON.parse(JSON.stringify(value)), []);
  const hasChanges =
    JSON.stringify(settings) !== JSON.stringify(originalSettings);

  const normalizeSettings = useCallback(
    (incoming) => {
      const next = clone(incoming || {});
      if (!next.smtp) next.smtp = {};
      if (!next.templates) next.templates = {};
      return {
        appName: next.appName || "",
        appURL: next.appURL || "",
        senderName: next.senderName || "",
        senderAddress: next.senderAddress || "",
        useSMTP: next.useSMTP !== false,
        smtp: {
          host: next.smtp.host || "",
          port: Number(next.smtp.port || 587),
          username: next.smtp.username || "",
          password: next.smtp.password || "",
          tls: next.smtp.tls || "auto",
          authMethod: next.smtp.authMethod || "plain",
          localName: next.smtp.localName || "",
        },
        templates: {
          verification: {
            subject: next.templates.verification?.subject || "",
            body: next.templates.verification?.body || "",
          },
          "email-change": {
            subject: next.templates["email-change"]?.subject || "",
            body: next.templates["email-change"]?.body || "",
          },
          "password-reset": {
            subject: next.templates["password-reset"]?.subject || "",
            body: next.templates["password-reset"]?.body || "",
          },
        },
      };
    },
    [clone],
  );

  const loadPage = useCallback(async () => {
    setLoading(true);
    try {
      const res = await api("/mail/settings");
      const normalized = normalizeSettings(res?.settings || {});
      setSettings(normalized);
      setOriginalSettings(clone(normalized));
      if (!testEmail && normalized.senderAddress)
        setTestEmail(normalized.senderAddress);
    } catch (err) {
      showToast(err.message || "Failed to load mail settings", "error");
    } finally {
      setLoading(false);
    }
  }, [clone, normalizeSettings, testEmail]);

  useEffect(() => {
    loadPage();
  }, []);

  const saveSettings = useCallback(async () => {
    setSaving(true);
    setTestOk(false);
    setTestError("");
    try {
      const payload = clone(settings);
      if (
        payload.smtp?.password === emailPasswordMask &&
        originalSettings.smtp?.password === emailPasswordMask
      ) {
        delete payload.smtp.password;
      }
      const res = await api("/mail/settings", {
        method: "PUT",
        body: JSON.stringify({ settings: payload }),
      });
      const normalized = normalizeSettings(res?.settings || payload);
      setSettings(normalized);
      setOriginalSettings(clone(normalized));
      showToast("Mail settings saved");
    } catch (err) {
      showToast(err.message || "Failed to save mail settings", "error");
    } finally {
      setSaving(false);
    }
  }, [settings, originalSettings, clone, normalizeSettings]);

  const sendTestEmail = useCallback(async () => {
    setTesting(true);
    setTestOk(false);
    setTestError("");
    try {
      const payload = clone(settings);
      if (
        payload.smtp?.password === emailPasswordMask &&
        originalSettings.smtp?.password === emailPasswordMask
      ) {
        delete payload.smtp.password;
      }
      await api("/mail/settings/test", {
        method: "POST",
        body: JSON.stringify({
          settings: payload,
          email: testEmail,
          template: testTemplate,
        }),
      });
      setTestOk(true);
      showToast(testTemplate ? "Template test email sent" : "Test email sent");
    } catch (err) {
      setTestError(err.message || "Failed to send test email");
      showToast(err.message || "Failed to send test email", "error");
    } finally {
      setTesting(false);
    }
  }, [settings, originalSettings, clone, testEmail, testTemplate]);

  const updateTemplate = useCallback((name, field, value) => {
    setSettings((prev) => ({
      ...prev,
      templates: {
        ...prev.templates,
        [name]: {
          ...prev.templates[name],
          [field]: value,
        },
      },
    }));
  }, []);

  return html`
    <div class="mail-page">
      <div class="backups-hero">
        <div><h1 class="backups-title">Mail</h1><p class="workflow-subtitle">Sender details, delivery settings, and email templates.</p></div>
        <div class="backups-actions">
          <${Action} class="btn" onClick=${loadPage} disabled=${loading || saving || testing}>${icons.refresh} Refresh</${Action}>
        </div>
      </div>

      <${LayerCard} class="mail-card">
        <div class="mail-section">
          <div class="mail-grid">
            <div>
              <label class="backup-label">App name</label>
              <${FieldInput} aria-label="App name" class="backup-input" type="text" value=${settings.appName} onInput=${(e) => setSettings((prev) => ({ ...prev, appName: e.target.value }))} />
            </div>
            <div>
              <label class="backup-label">App URL</label>
              <${FieldInput} aria-label="App URL" class="backup-input" type="url" placeholder="https://example.com" value=${settings.appURL} onInput=${(e) => setSettings((prev) => ({ ...prev, appURL: e.target.value }))} />
            </div>
          </div>

          <div class="mail-grid">
            <div>
              <label class="backup-label">Sender name</label>
              <${FieldInput} aria-label="Sender name" class="backup-input" type="text" value=${settings.senderName} onInput=${(e) => setSettings((prev) => ({ ...prev, senderName: e.target.value }))} />
            </div>
            <div>
              <label class="backup-label">Sender address</label>
              <${FieldInput} aria-label="Sender address" class="backup-input" type="email" value=${settings.senderAddress} onInput=${(e) => setSettings((prev) => ({ ...prev, senderAddress: e.target.value }))} />
            </div>
          </div>

          <div class="backup-checkbox">
            <${Toggle} label="Use SMTP mail server" checked=${!!settings.useSMTP} onChange=${(value) => setSettings((prev) => ({ ...prev, useSMTP: value }))} disabled=${loading || saving || testing} />
            <span>Use SMTP mail server (recommended)</span>
          </div>

          ${
            settings.useSMTP
              ? html`
            <div class="mail-grid mail-grid-4">
              <div>
                <label class="backup-label">SMTP server host</label>
                <${FieldInput} aria-label="SMTP server host" class="backup-input" type="text" value=${settings.smtp.host} onInput=${(e) => setSettings((prev) => ({ ...prev, smtp: { ...prev.smtp, host: e.target.value } }))} />
              </div>
              <div>
                <label class="backup-label">Port</label>
                <${FieldInput} aria-label="Port" class="backup-input" type="number" min="1" value=${settings.smtp.port} onInput=${(e) => setSettings((prev) => ({ ...prev, smtp: { ...prev.smtp, port: Number(e.target.value || 0) } }))} />
              </div>
              <div>
                <label class="backup-label">Username</label>
                <${FieldInput} aria-label="Username" class="backup-input" type="text" value=${settings.smtp.username} onInput=${(e) => setSettings((prev) => ({ ...prev, smtp: { ...prev.smtp, username: e.target.value } }))} />
              </div>
              <div>
                <label class="backup-label">Password</label>
                <${FieldInput} aria-label="Password" class="backup-input" type="password" value=${settings.smtp.password} onInput=${(e) => setSettings((prev) => ({ ...prev, smtp: { ...prev.smtp, password: e.target.value } }))} />
              </div>
            </div>

            <div>
              <${Action} class="btn backup-settings-toggle" onClick=${() => setShowAdvanced((v) => !v)}>
                <span>${showAdvanced ? "Hide more options" : "Show more options"}</span>
                <span>${showAdvanced ? icons.chevUp : icons.chevDown}</span>
              </${Action}>
            </div>

            ${
              showAdvanced
                ? html`
              <div class="mail-grid">
                <div>
                  <label class="backup-label">TLS encryption</label>
                  <${FieldSelect} aria-label="TLS encryption" class="backup-select" value=${settings.smtp.tls} onChange=${(e) => setSettings((prev) => ({ ...prev, smtp: { ...prev.smtp, tls: e.target.value } }))}>
                    <${FieldSelect.Option} value="auto">Auto (StartTLS)</${FieldSelect.Option}>
                    <${FieldSelect.Option} value="starttls">StartTLS required</${FieldSelect.Option}>
                    <${FieldSelect.Option} value="ssl">SSL / TLS</${FieldSelect.Option}>
                    <${FieldSelect.Option} value="none">None</${FieldSelect.Option}>
                  </${FieldSelect}>
                </div>
                <div>
                  <label class="backup-label">AUTH method</label>
                  <${FieldSelect} aria-label="AUTH method" class="backup-select" value=${settings.smtp.authMethod} onChange=${(e) => setSettings((prev) => ({ ...prev, smtp: { ...prev.smtp, authMethod: e.target.value } }))}>
                    <${FieldSelect.Option} value="plain">PLAIN (default)</${FieldSelect.Option}>
                    <${FieldSelect.Option} value="login">LOGIN</${FieldSelect.Option}>
                    <${FieldSelect.Option} value="cram-md5">CRAM-MD5</${FieldSelect.Option}>
                    <${FieldSelect.Option} value="none">None</${FieldSelect.Option}>
                  </${FieldSelect}>
                </div>
                <div>
                  <label class="backup-label">EHLO/HELO domain</label>
                  <${FieldInput} aria-label="EHLO/HELO domain" class="backup-input" type="text" placeholder="Default to localhost" value=${settings.smtp.localName} onInput=${(e) => setSettings((prev) => ({ ...prev, smtp: { ...prev.smtp, localName: e.target.value } }))} />
                </div>
              </div>
            `
                : null
            }
          `
              : null
          }

          <div class="mail-footer">
            <div class="mail-inline-actions">
              <${FieldInput} class="backup-input" style="min-width:280px" type="email" placeholder="Test email recipient" value=${testEmail} onInput=${(e) => setTestEmail(e.target.value)} />
              <${FieldSelect} class="backup-select" style="min-width:220px" value=${testTemplate} onChange=${(e) => setTestTemplate(e.target.value)}>
                <${FieldSelect.Option} value="">Generic test email</${FieldSelect.Option}>
                <${FieldSelect.Option} value="verification">Verification template</${FieldSelect.Option}>
                <${FieldSelect.Option} value="email-change">Email change template</${FieldSelect.Option}>
                <${FieldSelect.Option} value="password-reset">Password reset template</${FieldSelect.Option}>
              </${FieldSelect}>
              <${Action} class="btn" onClick=${sendTestEmail} disabled=${testing || loading || saving}>${icons.mail} Send test email</${Action}>
            </div>
            <div class="mail-inline-actions">
              ${testing ? html`<span class="backup-status-ok">Sending test email…</span>` : null}
              ${testOk ? html`<span class="backup-status-ok">${icons.check} Test email sent successfully</span>` : null}
              ${testError ? html`<span class="backup-status-error">${testError}</span>` : null}
            </div>
          </div>
        </div>
      </${LayerCard}>

      <${LayerCard} class="mail-card">
        <div class="mail-section">
          <div class="backups-title" style="font-size:20px">Email templates</div>
          <div class="backup-help">Available template variables: {{.Email}}, {{.Token}}, {{.URL}}, {{.AppName}}, {{.AppURL}}</div>

          ${[
            ["verification", "Verification email"],
            ["email-change", "Email change confirmation"],
            ["password-reset", "Password reset"],
          ].map(
            ([name, label]) => html`
            <div class="mail-template-card" key=${name}>
              <div class="mail-template-title">${label}</div>
              <div>
                <label class="backup-label">Subject</label>
                <${FieldInput} aria-label="Subject" class="backup-input" type="text" value=${settings.templates[name].subject} onInput=${(e) => updateTemplate(name, "subject", e.target.value)} />
              </div>
              <div>
                <label class="backup-label">HTML body</label>
                <${FieldArea} aria-label="HTML body" class="mail-textarea" value=${settings.templates[name].body} onInput=${(e) => updateTemplate(name, "body", e.target.value)}></${FieldArea}>
              </div>
            </div>
          `,
          )}
        </div>
      </${LayerCard}>

      <div class="backup-footer">
        ${
          hasChanges
            ? html`<${Action} class="btn" onClick=${() => {
                setSettings(clone(originalSettings));
                setTestOk(false);
                setTestError("");
              }}>Reset</${Action}>`
            : null
        }
        <${Action} class="btn btn-primary" disabled=${saving || loading || !hasChanges} onClick=${saveSettings}>Save changes</${Action}>
      </div>
    </div>
  `;
}

const emptyWorkflow = {
  id: "",
  name: "",
  category: "",
  enabled: true,
  trigger: { type: "row_insert", table: "", events: [] },
  conditions: [],
  lookups: [],
  ai: {
    model: "",
    provider: "",
    dataCollection: "deny",
    prompt: "",
    resultSchema: {},
  },
  actions: [{ type: "queue_review", requireApproval: true }],
  holdUntilComplete: false,
  subjectPath: "",
  newUserClearedLimit: 0,
  maxRetries: 2,
};

const cloneWorkflow = (workflow) =>
  JSON.parse(JSON.stringify(workflow || emptyWorkflow));

function workflowRunContent(run) {
  const input = run?.input;
  if (input && typeof input === "object" && input.row != null) return input.row;
  return input ?? null;
}

function formatWorkflowRunContent(run) {
  const content = workflowRunContent(run);
  if (content == null) return "No captured input.";
  try {
    return JSON.stringify(content, null, 2);
  } catch {
    return String(content);
  }
}

function workflowValueAtPath(scope, path) {
  let current = scope;
  for (const part of String(path || "")
    .split(".")
    .filter(Boolean)) {
    if (current == null || typeof current !== "object") return null;
    current = current[part];
  }
  return current;
}

function legacyWorkflowActionEffect(run, workflows) {
  const type = run?.recommendedAction || run?.action || "";
  const workflow = (workflows || []).find(
    (item) => item.id === run?.workflowId,
  );
  const action = workflow?.actions?.find((item) => item.type === type);
  if (!action) return { type };
  const scope = {
    input: run.input || {},
    lookups: run.lookupResults || {},
    result: run.result || {},
  };
  const effect = {
    type: action.type,
    table: action.table || "",
    field: action.field || "",
  };
  if (["delete", "archive", "block"].includes(action.type))
    effect.id = workflowValueAtPath(scope, action.idPath);
  if (action.type === "block")
    effect.value = action.value == null ? true : action.value;
  if (["create_alias", "propose_alias"].includes(action.type)) {
    effect.data = Object.fromEntries(
      Object.entries(action.data || {}).map(([field, path]) => [
        field,
        workflowValueAtPath(scope, path),
      ]),
    );
  }
  return effect;
}

function workflowActionEffectText(run, workflows) {
  const effect =
    run?.actionEffect || legacyWorkflowActionEffect(run, workflows);
  const type = effect?.type || run?.recommendedAction || run?.action || "";
  const target =
    effect?.table && effect?.id
      ? `${effect.table} row ${effect.id}`
      : effect?.table || "the configured target";
  switch (type) {
    case "approve":
      return run?.holdUntilComplete && run?.table && run?.rowId
        ? `Release ${run.table} row ${run.rowId} from the workflow hold and make it visible.`
        : "Complete the run without changing application data.";
    case "queue_review":
      return "Complete the review without changing application data.";
    case "delete":
      return `Permanently delete ${target}.`;
    case "archive":
      return `Archive ${target}.`;
    case "block": {
      const value = effect?.value === undefined ? true : effect.value;
      return `Set ${effect?.table || "the configured table"} row ${effect?.id || "the configured target"} field ${effect?.field || "the configured field"} to ${JSON.stringify(value)}.`;
    }
    case "create_alias":
    case "propose_alias": {
      const fields = Object.entries(effect?.data || {})
        .map(([field, value]) => `${field}=${JSON.stringify(value)}`)
        .join(", ");
      return `Create a row in ${effect?.table || "the configured alias table"}${fields ? ` with ${fields}` : ""}.`;
    }
    case "rejected":
      return "No action was taken.";
    default:
      return type
        ? `Run the configured ${type.replaceAll("_", " ")} action.`
        : "No action has been selected yet.";
  }
}

function workflowActionOutcome(run, workflows) {
  const effect = workflowActionEffectText(run, workflows);
  if (run?.status === "awaiting_approval")
    return {
      state: "Awaiting admin decision",
      effect: `Approval will: ${effect}`,
    };
  if (run?.status === "error")
    return {
      state: "No action taken",
      effect: run.error || "The run failed before an action completed.",
    };
  if (run?.status === "cancelled")
    return {
      state: "No action taken",
      effect: run.reasoning || "The run was cancelled.",
    };
  if (run?.status === "pending")
    return { state: "No action yet", effect: "Waiting for model evaluation." };
  if (run?.status === "running")
    return {
      state: "No action yet",
      effect: "Model evaluation is in progress.",
    };
  if (run?.status === "completed" && run?.action === "rejected")
    return {
      state: "Declined by admin",
      effect: `No action was taken. Declined action: ${effect}`,
    };
  if (run?.status === "completed") {
    const noDataChange = ["approve", "queue_review"].includes(run?.action);
    return {
      state: noDataChange ? "Completed — no data change" : "Action taken",
      effect,
    };
  }
  return { state: "No action yet", effect };
}

function WorkflowPage({ tables }) {
  const [tab, setTab] = useState(() =>
    window.location.hash.includes("/moderation") ? "moderation" : "runs",
  );
  const [workflows, setWorkflows] = useState([]);
  const [templates, setTemplates] = useState([]);
  const [runs, setRuns] = useState([]);
  const [apiKeyConfigured, setApiKeyConfigured] = useState(false);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [editing, setEditing] = useState(null);
  const [inspectingRun, setInspectingRun] = useState(null);
  const [status, setStatus] = useState("");
  const appTables = useMemo(
    () => (tables || []).filter((t) => !t.name.startsWith("_")),
    [tables],
  );

  const loadConfig = useCallback(async () => {
    const data = await api("/workflows/config");
    setWorkflows(data?.workflows || []);
    setTemplates(data?.templates || []);
    setApiKeyConfigured(!!data?.apiKeyConfigured);
  }, []);

  const loadRuns = useCallback(async () => {
    const suffix = status ? "?status=" + encodeURIComponent(status) : "";
    const data = await api("/workflows/runs" + suffix);
    setRuns(data?.runs || []);
  }, [status]);

  const loadAll = useCallback(async () => {
    setLoading(true);
    try {
      await Promise.all([loadConfig(), loadRuns()]);
    } catch (err) {
      showToast(err.message || "Failed to load workflow", "error");
    } finally {
      setLoading(false);
    }
  }, [loadConfig, loadRuns]);

  useEffect(() => {
    loadAll();
  }, []);
  useEffect(() => {
    if (!loading) loadRuns();
  }, [status]);
  useEffect(() => {
    if (!inspectingRun) return;
    const closeOnEscape = (event) => {
      if (event.key === "Escape") setInspectingRun(null);
    };
    window.addEventListener("keydown", closeOnEscape);
    return () => window.removeEventListener("keydown", closeOnEscape);
  }, [inspectingRun]);

  const editWorkflow = useCallback((workflow) => {
    const next = cloneWorkflow(workflow);
    next.trigger = next.trigger || { type: "manual" };
    next.ai = next.ai || {};
    next.actions = next.actions || [];
    next._conditionsJSON = JSON.stringify(next.conditions || [], null, 2);
    next._lookupsJSON = JSON.stringify(next.lookups || [], null, 2);
    next._schemaJSON = JSON.stringify(next.ai.resultSchema || {}, null, 2);
    next._actionsJSON = JSON.stringify(next.actions || [], null, 2);
    setEditing(next);
  }, []);

  const update = useCallback(
    (field, value) => setEditing((prev) => ({ ...prev, [field]: value })),
    [],
  );
  const updateTrigger = useCallback(
    (field, value) =>
      setEditing((prev) => ({
        ...prev,
        trigger: { ...(prev.trigger || {}), [field]: value },
      })),
    [],
  );
  const updateAI = useCallback(
    (field, value) =>
      setEditing((prev) => ({
        ...prev,
        ai: { ...(prev.ai || {}), [field]: value },
      })),
    [],
  );

  const save = useCallback(async () => {
    if (!editing) return;
    setSaving(true);
    try {
      const payload = cloneWorkflow(editing);
      payload.conditions = JSON.parse(payload._conditionsJSON || "[]");
      payload.lookups = JSON.parse(payload._lookupsJSON || "[]");
      payload.actions = JSON.parse(payload._actionsJSON || "[]");
      payload.ai.resultSchema = JSON.parse(payload._schemaJSON || "{}");
      delete payload._conditionsJSON;
      delete payload._lookupsJSON;
      delete payload._actionsJSON;
      delete payload._schemaJSON;
      const path = payload.id
        ? "/workflows/" + encodeURIComponent(payload.id)
        : "/workflows";
      await api(path, {
        method: payload.id ? "PUT" : "POST",
        body: JSON.stringify(payload),
      });
      showToast(editing.id ? "Workflow updated" : "Workflow created");
      setEditing(null);
      await loadConfig();
    } catch (err) {
      showToast(err.message || "Failed to save workflow", "error");
    } finally {
      setSaving(false);
    }
  }, [editing, loadConfig]);

  const remove = useCallback(
    async (workflow) => {
      if (
        !(await confirmAction(
          'Delete workflow "' + workflow.name + '"? Audit runs will be kept.',
        ))
      )
        return;
      try {
        await apiDelete("/workflows/" + encodeURIComponent(workflow.id));
        showToast("Workflow deleted");
        await loadConfig();
      } catch (err) {
        showToast(err.message || "Failed to delete workflow", "error");
      }
    },
    [loadConfig],
  );

  const resolve = useCallback(
    async (run, action) => {
      const outcome = workflowActionOutcome(run, workflows);
      const messages = {
        approve: `Apply this action?\n\n${outcome.effect}`,
        reject: `Decline this action?\n\nNo action will be taken.`,
      };
      if (action !== "retry" && !(await confirmAction(messages[action])))
        return;
      try {
        await api(
          "/workflows/runs/" + encodeURIComponent(run.id) + "/resolve",
          {
            method: "POST",
            body: JSON.stringify({ action }),
          },
        );
        showToast("Workflow action applied");
        await loadRuns();
      } catch (err) {
        showToast(err.message || "Workflow action failed", "error");
      }
    },
    [loadRuns, workflows],
  );

  const runManual = useCallback(
    async (workflow) => {
      const raw = prompt("Manual input as JSON", "{}");
      if (raw === null) return;
      try {
        await api("/workflows/" + encodeURIComponent(workflow.id) + "/run", {
          method: "POST",
          body: JSON.stringify({ input: JSON.parse(raw) }),
        });
        showToast("Workflow queued");
        setTab("runs");
        await loadRuns();
      } catch (err) {
        showToast(err.message || "Failed to run workflow", "error");
      }
    },
    [loadRuns],
  );

  return html`
    <div class="workflow-page">
      <div class="workflow-hero">
        <div>
          <div class="workflow-title">AI workflows</div>
          <div class="workflow-subtitle">Connect events, conditions, indexed lookups, structured AI decisions, approvals, and database actions.</div>
        </div>
        <${Action} class="btn" onClick=${loadAll} disabled=${loading}>${icons.refresh} Refresh</${Action}>
      </div>
      ${
        !apiKeyConfigured &&
        html`
        <${Banner} variant="alert">Set <code>OPENROUTER_API_KEY</code> or <code>Config.Workflow.OpenRouterAPIKey</code>. Runs remain in error and unpublished content stays hidden until retried or approved.</${Banner}>
      `
      }
      <${Tabs} value=${tab} onValueChange=${setTab} tabs=${[
        { value: "runs", label: "Runs & approvals" },
        { value: "moderation", label: "Moderation setup" },
        { value: "workflows", label: "Workflows (" + workflows.length + ")" },
      ]} />

      ${
        tab === "moderation"
          ? html`
              <div class="workflow-grid">
                ${templates.map(
                  (template) => html`
            <${LayerCard} class="workflow-card" key=${template.id}>
              <div class="workflow-card-header">
                <div><div class="workflow-name">${template.name}</div><div class="workflow-meta">${template.workflow.category || "workflow"} template</div></div>
                <${Action} class="btn btn-primary btn-sm" onClick=${() => editWorkflow(template.workflow)}>Use template</${Action}>
              </div>
              <div class="workflow-meta">${template.description}</div>
              <div class="workflow-badges">
                <${Badge} variant="secondary">${template.workflow.trigger?.type}</${Badge}>
                ${(template.workflow.actions || []).map((a) => html`<${Badge} variant="secondary" key=${a.type}>${a.type.replaceAll("_", " ")}</${Badge}>`)}
              </div>
            </${LayerCard}>
          `,
                )}
              </div>
            `
          : tab === "workflows"
            ? html`
        <div style="display:flex;justify-content:flex-end">
          <${Action} class="btn btn-primary" onClick=${() => editWorkflow(emptyWorkflow)}>${icons.plus} New workflow</${Action}>
        </div>
        <div class="workflow-grid">
          ${
            workflows.length === 0
              ? html`<div class="empty-state" style="min-height:220px">
                  <div class="empty-title">No workflows configured</div>
                  <div class="empty-desc">
                    Use a moderation template or create a workflow.
                  </div>
                </div>`
              : workflows.map(
                  (workflow) => html`
            <${LayerCard} class="workflow-card" key=${workflow.id}>
              <div class="workflow-card-header">
                <div><div class="workflow-name">${workflow.name}</div><div class="workflow-meta">${workflow.trigger?.type}${workflow.trigger?.table ? " · " + workflow.trigger.table : ""} · ${workflow.ai?.model || ""}</div></div>
                <div style="display:flex;gap:4px">
                  ${workflow.trigger?.type === "manual" && html`<${Action} class="btn btn-sm" onClick=${() => runManual(workflow)}>Run</${Action}>`}
                  <${Action} class="btn-icon" title="Edit" onClick=${() => editWorkflow(workflow)}>${icons.edit}</${Action}>
                  <${Action} class="btn-icon danger" title="Delete" onClick=${() => remove(workflow)}>${icons.trash}</${Action}>
                </div>
              </div>
              <div class="workflow-badges">
                <${Badge} variant=${workflow.enabled ? "success" : "secondary"}>${workflow.enabled ? "Enabled" : "Disabled"}</${Badge}>
                ${workflow.category && html`<${Badge} variant="secondary">${workflow.category}</${Badge}>`}
                <${Badge} variant="secondary">${workflow.holdUntilComplete ? "hold rows" : "non-blocking"}</${Badge}>
                ${(workflow.actions || []).map((a) => html`<${Badge} variant="secondary" key=${a.type}>${a.type.replaceAll("_", " ")}${a.requireApproval ? " · approval" : ""}</${Badge}>`)}
              </div>
              <div class="workflow-meta">${(workflow.lookups || []).length} lookups · ${workflow.maxRetries || 0} retries</div>
            </${LayerCard}>
          `,
                )
          }
        </div>
      `
            : html`
        <div style="display:flex;justify-content:flex-end;gap:8px">
          <${FieldSelect} class="analytics-select" value=${status} onChange=${(e) => setStatus(e.target.value)}>
            <${FieldSelect.Option} value="">All statuses</${FieldSelect.Option}>
            ${["pending", "running", "awaiting_approval", "error", "completed", "cancelled"].map((v) => html`<${FieldSelect.Option} key=${v} value=${v}>${v.replace("_", " ")}</${FieldSelect.Option}>`)}
          </${FieldSelect}>
        </div>
        <${LayerCard} class="observe-table-wrap">
          <div class="panel-table-scroll">
            <${Table}>
              <${Table.Header}><${Table.Row}><${Table.Head}>Status</${Table.Head}><${Table.Head}>Workflow / event</${Table.Head}><${Table.Head}>Source</${Table.Head}><${Table.Head}>Action state and effect</${Table.Head}><${Table.Head}>Model reasoning</${Table.Head}><${Table.Head}>Review</${Table.Head}></${Table.Row}></${Table.Header}>
              <${Table.Body}>
                ${
                  runs.length === 0
                    ? html`<${Table.Row}><${Table.Cell} colspan="6" class="empty-row">${loading ? "Loading workflow runs…" : "No workflow runs found"}</${Table.Cell}></${Table.Row}>`
                    : runs.map((run) => {
                        const outcome = workflowActionOutcome(run, workflows);
                        return html`<${Table.Row} key=${run.id}>
                    <${Table.Cell}><${Badge} variant=${run.status === "error" ? "error" : "secondary"}>${run.status.replace("_", " ")}</${Badge}><div class="th-type">${run.createdAtUnixMilli ? formatDateTime(run.createdAtUnixMilli) : ""}</div><div class="th-type">attempt ${run.attempt}/${(run.maxRetries || 0) + 1}</div></${Table.Cell}>
                    <${Table.Cell}><div>${run.workflowName}</div><div class="th-type">${run.trigger}${run.event ? " · " + run.event : ""}</div></${Table.Cell}>
                    <${Table.Cell} class="workflow-run-source">
                      ${
                        run.table && run.rowId
                          ? html`<div class="workflow-run-source-name">
                                ${run.table}
                              </div>
                              <div class="th-type">row ${run.rowId}</div>`
                          : html`<div class="th-type">
                              ${run.trigger === "manual"
                                ? "Manual input"
                                : "External event input"}
                            </div>`
                      }
                      ${workflowRunContent(run) != null && html`<${Action} class="btn btn-sm workflow-run-source-action" onClick=${() => setInspectingRun(run)}>Inspect content</${Action}>`}
                    </${Table.Cell}>
                    <${Table.Cell} class="workflow-run-detail">
                      <div class="workflow-run-label">Execution</div>
                      <div class="workflow-run-action-state">${outcome.state}</div>
                      <div class="workflow-run-effect">${outcome.effect}</div>
                    </${Table.Cell}>
                    <${Table.Cell} class="workflow-run-reasoning">
                      <div class="workflow-run-label">Model analysis</div>
                      <div>${run.reasoning || (run.status === "error" ? "No model reasoning was produced." : "No model reasoning was recorded.")}</div>
                      ${(run.model || run.provider) && html`<div class="th-type">${[run.provider, run.model].filter(Boolean).join(" · ")}</div>`}
                    </${Table.Cell}>
                    <${Table.Cell}><div class="workflow-run-controls">
                        ${run.status === "awaiting_approval" && html`<${Action} class="btn btn-sm btn-primary" onClick=${() => resolve(run, "approve")}>Apply action</${Action}>`}
                        ${run.status === "awaiting_approval" && html`<${Action} class="btn btn-sm" onClick=${() => resolve(run, "reject")}>Decline</${Action}>`}
                        ${run.status === "error" && html`<${Action} class="btn btn-sm" onClick=${() => resolve(run, "retry")}>Retry</${Action}>`}
                      </div></${Table.Cell}>
                  </${Table.Row}>`;
                      })
                }
              </${Table.Body}>
            </${Table}>
          </div>
        </${LayerCard}>
      `
      }

      ${
        inspectingRun &&
        html`
        <${Dialog.Root} open=${true} onOpenChange=${(open) => {
          if (!open) setInspectingRun(null);
        }}><${Dialog} size="xl" className="dialog-content">
            <div class="workflow-source-header">
              <div>
                <${Dialog.Title}>Workflow source content</${Dialog.Title}>
                <div class="workflow-meta">
                  ${
                    inspectingRun.table && inspectingRun.rowId
                      ? inspectingRun.table + " · row " + inspectingRun.rowId
                      : inspectingRun.trigger === "manual"
                        ? "Manual input"
                        : "External event input"
                  }
                </div>
              </div>
              <${Action} class="btn-icon" aria-label="Close" onClick=${() => setInspectingRun(null)}>×</${Action}>
            </div>
            <div class="workflow-source-body">
              <div class="workflow-meta">Captured when this run was created</div>
              <pre class="workflow-source-content">${formatWorkflowRunContent(inspectingRun)}</pre>
            </div>
          </${Dialog}></${Dialog.Root}>
      `
      }

      ${
        editing &&
        html`
          <${LayerCard} class="workflow-form">
            <div class="workflow-card-header">
              <div class="workflow-name">${editing.id ? "Edit workflow" : "New workflow"}</div>
              <${Action} class="btn-icon" onClick=${() => setEditing(null)}>${icons.x}</${Action}>
            </div>
            <div class="workflow-form-grid">
              <div class="row-editor-field">
                <label>Name</label>
                <${FieldInput} value=${editing.name} onInput=${(e) => update("name", e.target.value)} />
              </div>
              <div class="row-editor-field">
                <label>Category</label>
                <${FieldInput} placeholder="moderation" value=${editing.category || ""} onInput=${(e) => update("category", e.target.value)} />
              </div>
              <div class="row-editor-field">
                <label>Trigger</label>
                <${FieldSelect} value=${editing.trigger?.type || ""} onChange=${(e) => updateTrigger("type", e.target.value)}>
                  ${["row_insert", "row_update", "report", "discord", "manual"].map((v) => html`<${FieldSelect.Option} value=${v} key=${v}>${v.replace("_", " ")}</${FieldSelect.Option}>`)}
                </${FieldSelect}>
              </div>
              ${
                ["row_insert", "row_update", "report"].includes(
                  editing.trigger?.type,
                ) &&
                html`
                <div class="row-editor-field">
                  <label>Trigger table</label>
                  <${FieldSelect} value=${editing.trigger?.table || ""} onChange=${(e) => updateTrigger("table", e.target.value)}>
                    <${FieldSelect.Option} value="">Select table</${FieldSelect.Option}>
                    ${appTables.map((t) => html`<${FieldSelect.Option} key=${t.name} value=${t.name}>${t.name}</${FieldSelect.Option}>`)}
                  </${FieldSelect}>
                </div>
              `
              }
              ${
                ["discord", "report"].includes(editing.trigger?.type) &&
                html`
                  <div class="row-editor-field">
                    <label>Event name (optional)</label>
                    <${FieldInput}
                      placeholder="activity_mismatch"
                      value=${editing.trigger?.event || ""}
                      onInput=${(e) => updateTrigger("event", e.target.value)}
                    />
                  </div>
                `
              }
              <div class="row-editor-field">
                <label>Model</label>
                <${FieldInput} placeholder="provider/model-name" value=${editing.ai?.model || ""} onInput=${(e) => updateAI("model", e.target.value)} />
              </div>
              <div class="row-editor-field">
                <label>OpenRouter provider slug (optional)</label>
                <${FieldInput} placeholder="openai" value=${editing.ai?.provider || ""} onInput=${(e) => updateAI("provider", e.target.value)} />
              </div>
              <div class="row-editor-field">
                <label>Provider data collection</label>
                <${FieldSelect} value=${editing.ai?.dataCollection || "deny"} onChange=${(e) => updateAI("dataCollection", e.target.value)}>
                  <${FieldSelect.Option} value="deny">Deny (private providers only)</${FieldSelect.Option}>
                  <${FieldSelect.Option} value="allow">Allow (provider may store or train)</${FieldSelect.Option}>
                </${FieldSelect}>
              </div>
              <div class="row-editor-field">
                <label>Stop per-subject workflow after completed runs</label>
                <${FieldInput} type="number" min="0" value=${editing.newUserClearedLimit || 0} onInput=${(e) => update("newUserClearedLimit", Number(e.target.value || 0))} />
              </div>
              <div class="row-editor-field">
                <label>Subject path</label>
                <${FieldInput} placeholder="input.row.userId" value=${editing.subjectPath || ""} onInput=${(e) => update("subjectPath", e.target.value)} />
              </div>
              <div class="row-editor-field">
                <label>Automatic retries</label>
                <${FieldInput} type="number" min="0" max="10" value=${editing.maxRetries || 0} onInput=${(e) => update("maxRetries", Number(e.target.value || 0))} />
              </div>
            </div>
            <div class="row-editor-field">
              <label>AI prompt</label>
              <${FieldArea} value=${editing.ai?.prompt || ""} onInput=${(e) => updateAI("prompt", e.target.value)}></${FieldArea}>
            </div>
            <div class="workflow-checks">
              <label class="workflow-check"><${FieldInput} type="checkbox" checked=${editing.enabled} onChange=${(e) => update("enabled", e.target.checked)} /> Enabled</label>
              <label class="workflow-check"><${FieldInput} type="checkbox" checked=${editing.holdUntilComplete} onChange=${(e) => update("holdUntilComplete", e.target.checked)} /> Hold row visibility until complete</label>
            </div>
            <div class="workflow-form-grid">
              ${[
                [
                  "_conditionsJSON",
                  "Conditions",
                  '[{"field":"input.row.kind","operator":"equals","value":"post"}]',
                ],
                [
                  "_lookupsJSON",
                  "Lookups / tools",
                  '[{"name":"candidates","type":"search","table":"games","inputPath":"input.name","searchFields":["name"],"limit":10}]',
                ],
                [
                  "_schemaJSON",
                  "Structured result JSON Schema",
                  '{"type":"object","properties":{...}}',
                ],
                [
                  "_actionsJSON",
                  "Actions",
                  '[{"type":"delete","table":"posts","idPath":"input.rowId","requireApproval":true}]',
                ],
              ].map(
                ([field, label, placeholder]) => html`
                <div class="row-editor-field" key=${field}>
                  <label>${label} (JSON)</label>
                  <${FieldArea} placeholder=${placeholder} value=${editing[field] || ""} onInput=${(e) => update(field, e.target.value)}></${FieldArea}>
                </div>
              `,
              )}
            </div>
            <div class="workflow-meta">
              Paths may reference <code>input</code>, <code>lookups</code>, and <code>result</code>. Sensitive actions can set <code>requireApproval</code>.
            </div>
            <div style="display:flex;justify-content:flex-end;gap:8px">
              <${Action} class="btn" onClick=${() => setEditing(null)}>Cancel</${Action}>
              <${Action} class="btn btn-primary" disabled=${saving} onClick=${save}>${saving ? "Saving…" : "Save workflow"}</${Action}>
            </div>
          </${LayerCard}>
      `
      }
    </div>
  `;
}

// ── Main App ──
function App() {
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [tables, setTables] = useState([]);
  const [archiveTables, setArchiveTables] = useState([]);
  const [currentTable, setCurrentTable] = useState(null);
  const [currentArchive, setCurrentArchive] = useState(false);
  const [viewMode, setViewMode] = useState("table");
  const [analyticsEnabled, setAnalyticsEnabled] = useState(false);
  const [mediaEnabled, setMediaEnabled] = useState(false);
  const [observeEnabled, setObserveEnabled] = useState(false);
  const [workflowEnabled, setWorkflowEnabled] = useState(false);
  const evtRef = useRef(null);

  const loadTables = useCallback(async () => {
    setLoadError("");
    try {
      const data = await api("/tables");
      if (data) setTables(data.tables);
      const archiveData = await api("/archive/tables").catch(() => null);
      if (archiveData) setArchiveTables(archiveData.tables || []);
    } catch (err) {
      setLoadError(err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  // SSE change signal — bumped for the currently viewed table so DataTable can debounce-reload
  const [sseSignal, setSseSignal] = useState(0);

  // SSE — batch rapid events into a single state update per animation frame
  useEffect(() => {
    const pendingDeltas = {}; // tableName -> net count delta
    const dirtyRefCache = {}; // tables whose refCache needs clearing
    let rafId = 0;

    function flushDeltas() {
      rafId = 0;
      const deltas = Object.assign({}, pendingDeltas);
      const dirty = Object.keys(dirtyRefCache);
      // Clear accumulators
      for (const k in pendingDeltas) delete pendingDeltas[k];
      for (const k in dirtyRefCache) delete dirtyRefCache[k];
      // Invalidate ref cache for changed tables
      for (const t of dirty) delete refCache[t];
      // Bump SSE signal so DataTable can debounce-reload
      if (dirty.length > 0) setSseSignal((c) => c + 1);
      // Single state update for all accumulated changes
      setTables((prev) => {
        let changed = false;
        const next = prev.map((t) => {
          const d = deltas[t.name];
          if (d === undefined || d === 0) return t;
          changed = true;
          return { ...t, rowCount: Math.max(0, t.rowCount + d) };
        });
        return changed ? next : prev;
      });
    }

    function scheduleFlush() {
      if (!rafId) rafId = requestAnimationFrame(flushDeltas);
    }

    function connect() {
      if (evtRef.current) evtRef.current.close();
      const es = new EventSource("/_/api/events?_token=" + TOKEN);
      evtRef.current = es;

      es.addEventListener("snapshot", (e) => {
        try {
          const { tableCounts } = JSON.parse(e.data);
          if (tableCounts) {
            setTables((prev) =>
              prev.map((t) =>
                tableCounts[t.name] !== undefined
                  ? { ...t, rowCount: tableCounts[t.name] }
                  : t,
              ),
            );
          }
        } catch {}
      });

      es.addEventListener("change", (e) => {
        try {
          const evt = JSON.parse(e.data);
          dirtyRefCache[evt.table] = 1;
          if (evt.op === "insert")
            pendingDeltas[evt.table] = (pendingDeltas[evt.table] || 0) + 1;
          else if (evt.op === "delete")
            pendingDeltas[evt.table] = (pendingDeltas[evt.table] || 0) - 1;
          // updates don't change count — still schedule to clear refCache
          scheduleFlush();
        } catch {}
      });

      es.onerror = () => {
        es.close();
        setTimeout(connect, 3000);
      };
    }
    connect();
    return () => {
      if (evtRef.current) evtRef.current.close();
      if (rafId) cancelAnimationFrame(rafId);
    };
  }, []);

  useEffect(() => {
    loadTables();
  }, []);
  useEffect(() => {
    api("/media?limit=1")
      .then(() => {
        setMediaEnabled(true);
      })
      .catch(() => setMediaEnabled(false));
    api("/analytics/config")
      .then((data) => {
        if (data && data.enabled) setAnalyticsEnabled(true);
      })
      .catch(() => setAnalyticsEnabled(false));
    api("/analytics/indexes")
      .then((data) => {
        if (data && data.data) setObserveEnabled(true);
      })
      .catch(() => setObserveEnabled(false));
    api("/workflows/config")
      .then((data) => {
        setWorkflowEnabled(!!data?.enabled);
      })
      .catch(() => setWorkflowEnabled(false));
  }, []);

  // Hash sync
  useEffect(() => {
    function read() {
      const h = window.location.hash.slice(1);
      if (!h) {
        setViewMode("table");
        setCurrentTable(null);
        setCurrentArchive(false);
        return;
      }
      const decoded = decodeURIComponent(h.split("/")[0]);
      if (decoded === "_analytics" || decoded === "_analytics-volume") {
        setViewMode("analytics-volume");
        return;
      }
      if (decoded === "_analytics-performance") {
        setViewMode("analytics-performance");
        return;
      }
      if (decoded === "_observe") {
        setViewMode("observe");
        return;
      }
      if (decoded === "_backups") {
        setViewMode("backups");
        return;
      }
      if (decoded === "_mail") {
        setViewMode("mail");
        return;
      }
      if (decoded === "_media") {
        setViewMode("media");
        return;
      }
      if (decoded === "_workflow") {
        setViewMode("workflow");
        return;
      }
      if (h.startsWith("_archive/")) {
        setViewMode("table");
        setCurrentArchive(true);
        setCurrentTable(decodeURIComponent(h.slice("_archive/".length)));
        return;
      }
      setViewMode("table");
      setCurrentArchive(false);
      setCurrentTable(decoded);
    }
    read();
    window.addEventListener("hashchange", read);
    return () => window.removeEventListener("hashchange", read);
  }, []);

  const selectTable = useCallback((name) => {
    setViewMode("table");
    setCurrentArchive(false);
    setCurrentTable(name);
    window.location.hash = name;
  }, []);

  const selectArchive = useCallback((name) => {
    setViewMode("table");
    setCurrentArchive(true);
    setCurrentTable(name);
    window.location.hash = "_archive/" + encodeURIComponent(name);
  }, []);

  const currentSchema = useMemo(() => {
    if (!currentTable) return null;
    const source = currentArchive ? archiveTables : tables;
    const t = source.find((t) => t.name === currentTable);
    return t ? t.schema : null;
  }, [tables, archiveTables, currentTable, currentArchive]);

  return html`
    <${Toasty} toastManager=${toastManager}>
    <${Sidebar.Provider} defaultOpen=${true} className="admin-shell">
      <${AppNavigation} tables=${tables} archiveTables=${archiveTables} viewMode=${viewMode} currentTable=${currentTable} currentArchive=${currentArchive} analyticsEnabled=${analyticsEnabled} mediaEnabled=${mediaEnabled} observeEnabled=${observeEnabled} workflowEnabled=${workflowEnabled} onLogout=${logout} />
      <${PageFrame} title=${viewMode === "table" ? currentTable || "Tables" : titleCaseToken(viewMode)}>
      ${
        (viewMode === "analytics-volume" ||
          viewMode === "analytics-performance") &&
        analyticsEnabled
          ? html`<${AnalyticsPanel}
              page=${viewMode === "analytics-performance"
                ? "performance"
                : "volume"}
            />`
          : viewMode === "media" && mediaEnabled
            ? html`<${MediaPanel} />`
            : viewMode === "observe" && observeEnabled
              ? html`<${ObservePanel} />`
              : viewMode === "mail"
                ? html`<${MailSettingsPage} />`
                : viewMode === "backups"
                  ? html`<${BackupsPage} />`
                  : viewMode === "workflow" && workflowEnabled
                    ? html`<${WorkflowPage} tables=${tables} />`
                    : currentTable
                      ? html`<${DataTable}
                          key=${(currentArchive ? "archive:" : "table:") +
                          currentTable}
                          tableName=${currentTable}
                          tableMeta=${(currentArchive
                            ? archiveTables
                            : tables
                          ).find((t) => t.name === currentTable)}
                          schema=${currentSchema}
                          archive=${currentArchive}
                          onReload=${loadTables}
                          sseSignal=${sseSignal}
                        />`
                      : html`<${TableLibrary}
                          tables=${tables}
                          onSelect=${selectTable}
                          loading=${loading}
                          error=${loadError}
                          onRetry=${loadTables}
                        />`
      }
      </${PageFrame}>
    </${Sidebar.Provider}>
    <${ConfirmDialog} />
    </${Toasty}>
  `;
}

createRoot(document.getElementById("app")).render(html`<${App} />`);
