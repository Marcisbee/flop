// Embedded admin panel UI — served as inline HTML/CSS/JS

export function renderLoginPage(): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Flop Admin - Login</title>
<style>${baseCSS}</style>
</head>
<body>
<div class="login-container">
  <h1>flop <span class="badge">admin</span></h1>
  <form id="login-form">
    <input type="email" id="email" placeholder="Email" required autofocus>
    <input type="password" id="password" placeholder="Password" required>
    <button type="submit">Login</button>
    <div id="error" class="error hidden"></div>
  </form>
</div>
<script>
document.getElementById('login-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const email = document.getElementById('email').value;
  const password = document.getElementById('password').value;
  const errEl = document.getElementById('error');
  errEl.classList.add('hidden');
  try {
    const res = await fetch('/_/api/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password })
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Login failed');
    localStorage.setItem('flop_admin_token', data.token);
    window.location.href = '/_';
  } catch(err) {
    errEl.textContent = err.message;
    errEl.classList.remove('hidden');
  }
});
</script>
</body></html>`;
}

export function renderSetupPage(): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Flop Admin - Setup</title>
<style>${baseCSS}</style>
</head>
<body>
<div class="login-container">
  <h1>flop <span class="badge">setup</span></h1>
  <p style="color:#888;font-size:13px;margin-bottom:16px;text-align:center">Create your admin account</p>
  <form id="setup-form">
    <input type="email" id="email" placeholder="Email" required autofocus>
    <input type="text" id="name" placeholder="Name (optional)">
    <input type="password" id="password" placeholder="Password" required minlength="6">
    <input type="password" id="confirm" placeholder="Confirm password" required minlength="6">
    <button type="submit" style="width:100%">Create Admin</button>
    <div id="error" class="error hidden"></div>
  </form>
</div>
<script>
document.getElementById('setup-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const email = document.getElementById('email').value;
  const name = document.getElementById('name').value;
  const password = document.getElementById('password').value;
  const confirm = document.getElementById('confirm').value;
  const errEl = document.getElementById('error');
  errEl.classList.add('hidden');
  if (password !== confirm) {
    errEl.textContent = 'Passwords do not match';
    errEl.classList.remove('hidden');
    return;
  }
  const token = new URLSearchParams(window.location.search).get('token');
  try {
    const res = await fetch('/_/api/setup', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token, email, password, name: name || undefined })
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Setup failed');
    window.location.href = '/_/login';
  } catch(err) {
    errEl.textContent = err.message;
    errEl.classList.remove('hidden');
  }
});
</script>
</body></html>`;
}

export function renderAdminPage(): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Flop Admin</title>
<style>${baseCSS}${adminCSS}</style>
</head>
<body>
<div id="app">
  <nav>
    <h1>flop <span class="badge">admin</span></h1>
    <div id="nav-tables"></div>
    <div class="nav-bottom">
      <button onclick="downloadBackup()" class="btn-sm">Download Backup</button>
      <label class="btn-sm">Upload Backup<input type="file" accept=".tar.gz,.gz" onchange="uploadBackup(this)" hidden></label>
      <button onclick="logout()" class="btn-sm btn-danger">Logout</button>
    </div>
  </nav>
  <main id="content">
    <div class="empty">Select a table from the sidebar</div>
  </main>
</div>
<script>
const TOKEN = localStorage.getItem('flop_admin_token');
if (!TOKEN) window.location.href = '/_/login';

const api = (path, opts = {}) => fetch('/_/api' + path, {
  ...opts,
  headers: { 'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/json', ...opts.headers }
}).then(async r => {
  if (r.status === 401 || r.status === 403) { logout(); throw new Error('Unauthorized'); }
  if (r.headers.get('content-type')?.includes('json')) return r.json();
  return r;
});

let currentTable = null;
let currentPage = 1;
let tablesCache = [];
let rowsCache = [];
let editingCell = null;
let refDataCache = {}; // { tableName: [{id, label}, ...] }

async function fetchRefOptions(tableName) {
  if (refDataCache[tableName]) return refDataCache[tableName];
  try {
    const data = await api('/tables/' + tableName + '/rows?limit=500');
    const rows = data.rows || [];
    const options = rows.map(r => {
      const keys = Object.keys(r);
      const id = r[keys[0]];
      // Use second column as label, or first if only one column
      const label = keys.length > 1 ? (r[keys[1]] || id) : id;
      return { id: String(id), label: String(label) };
    });
    refDataCache[tableName] = options;
    return options;
  } catch {
    return [];
  }
}

// Invalidate ref cache when SSE events come in for referenced tables
function invalidateRefCache(tableName) {
  delete refDataCache[tableName];
}

// ---- URL hash sync ----
function syncToHash() {
  if (currentTable) {
    window.location.hash = currentPage > 1 ? currentTable + '/' + currentPage : currentTable;
  } else {
    window.location.hash = '';
  }
}

function readFromHash() {
  const h = window.location.hash.slice(1);
  if (!h) return;
  const parts = h.split('/');
  currentTable = decodeURIComponent(parts[0]);
  currentPage = parts[1] ? parseInt(parts[1], 10) || 1 : 1;
}

// ---- SSE for live updates ----
let evtSource = null;
function connectSSE() {
  if (evtSource) evtSource.close();
  evtSource = new EventSource('/_/api/events?_token=' + TOKEN);
  evtSource.onmessage = (e) => {
    try {
      const evt = JSON.parse(e.data);
      // Refresh sidebar counts
      loadTables(true);
      invalidateRefCache(evt.table);
      // If the changed table is the one we're viewing, flash and refresh
      if (currentTable && evt.table === currentTable) {
        loadRows(true).then(() => {
          // Flash the affected row
          const rowEl = document.querySelector('tr[data-id="' + CSS.escape(evt.rowId) + '"]');
          if (rowEl) {
            rowEl.classList.add('flash-' + evt.op);
            setTimeout(() => rowEl.classList.remove('flash-' + evt.op), 800);
          }
        });
      }
    } catch {}
  };
  evtSource.onerror = () => {
    // Reconnect after 3s
    evtSource.close();
    setTimeout(connectSSE, 3000);
  };
}

// ---- Tables ----
async function loadTables(silent) {
  try {
    const data = await api('/tables');
    tablesCache = data.tables;
    renderNav();
  } catch(err) {
    if (!silent) throw err;
  }
}

function renderNav() {
  const nav = document.getElementById('nav-tables');
  nav.innerHTML = tablesCache.map(t => {
    const active = t.name === currentTable ? ' active' : '';
    return '<button class="nav-btn' + active + '" data-table="' + escapeHtml(t.name) + '" onclick="selectTable(this.dataset.table)">' +
      escapeHtml(t.name) + ' <span class="count">' + t.rowCount + '</span></button>';
  }).join('');
}

// ---- Table selection ----
async function selectTable(name) {
  currentTable = name;
  currentPage = 1;
  syncToHash();
  renderNav();
  await loadRows();
}

// ---- Rows ----
async function loadRows(silent) {
  if (!currentTable) return;
  try {
    const data = await api('/tables/' + currentTable + '/rows?page=' + currentPage + '&limit=50');
    rowsCache = data.rows || [];
    renderContent(data);
  } catch(err) {
    if (!silent) throw err;
  }
}

function escapeHtml(s) {
  if (s == null) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function getTableSchema() {
  const t = tablesCache.find(t => t.name === currentTable);
  return t ? t.schema : null;
}

function isReadOnly(col) {
  const schema = getTableSchema();
  if (!schema || !schema[col]) return false;
  const f = schema[col];
  return f.type === 'bcrypt';
}

function isAutoField(col) {
  // Primary key (first column) is typically autogenerated
  const schema = getTableSchema();
  if (!schema) return false;
  const cols = Object.keys(schema);
  return cols[0] === col;
}

function renderContent(data) {
  const content = document.getElementById('content');
  const schema = getTableSchema();
  const schemaCols = schema ? Object.keys(schema) : [];
  const cols = schemaCols.length > 0 ? schemaCols : (data.rows.length > 0 ? Object.keys(data.rows[0]) : []);

  let html = '<div class="table-header"><h2>' + escapeHtml(currentTable) + '</h2>';
  html += '<div class="table-actions">';
  html += '<span class="meta">Page ' + data.page + '/' + data.pages + ' (' + data.total + ' rows)</span>';
  html += '<button onclick="showCreateForm()" class="btn-create">+ New Row</button>';
  html += '</div></div>';

  // Create row form (hidden by default)
  html += '<div id="create-form" class="create-form hidden">';
  html += '<div class="create-form-header"><strong>New Row</strong><button onclick="hideCreateForm()" class="btn-close">x</button></div>';
  html += '<div class="create-form-fields">';
  if (schema) {
    for (const col of cols) {
      const f = schema[col];
      if (!f) continue;
      // Skip auto-generated fields
      if (isAutoField(col)) continue;
      // Skip bcrypt fields in create — they need special handling
      if (f.type === 'bcrypt') {
        html += '<div class="form-field"><label>' + escapeHtml(col) + ' <span class="field-type">' + f.type + '</span></label>';
        html += '<input data-col="' + escapeHtml(col) + '" type="password" placeholder="Enter password">';
        html += '</div>';
        continue;
      }
      if (f.type === 'ref' && f.refTable) {
        html += '<div class="form-field"><label>' + escapeHtml(col) + ' <span class="field-type">' + f.type + ' &rarr; ' + escapeHtml(f.refTable) + '</span></label>';
        html += '<select data-col="' + escapeHtml(col) + '" data-ref="' + escapeHtml(f.refTable) + '" class="ref-select"><option value="">— loading... —</option></select>';
        html += '</div>';
        continue;
      }
      if (f.type === 'refMulti' && f.refTable) {
        html += '<div class="form-field"><label>' + escapeHtml(col) + ' <span class="field-type">' + f.type + ' &rarr; ' + escapeHtml(f.refTable) + '</span></label>';
        html += '<select data-col="' + escapeHtml(col) + '" data-ref="' + escapeHtml(f.refTable) + '" class="ref-select" multiple><option value="">— loading... —</option></select>';
        html += '</div>';
        continue;
      }
      if (f.type === 'enum' && f.enumValues) {
        html += '<div class="form-field"><label>' + escapeHtml(col) + ' <span class="field-type">' + f.type + '</span></label>';
        html += '<select data-col="' + escapeHtml(col) + '"><option value="">—</option>';
        f.enumValues.forEach(function(v) { html += '<option value="' + escapeHtml(v) + '">' + escapeHtml(v) + '</option>'; });
        html += '</select></div>';
        continue;
      }
      if (f.type === 'roles' || f.type === 'set' || f.type === 'fileMulti') {
        html += '<div class="form-field"><label>' + escapeHtml(col) + ' <span class="field-type">' + f.type + '</span></label>';
        html += '<input data-col="' + escapeHtml(col) + '" placeholder="[&quot;value1&quot;,&quot;value2&quot;]">';
        html += '</div>';
        continue;
      }
      if (f.type === 'json' || f.type === 'vector') {
        html += '<div class="form-field"><label>' + escapeHtml(col) + ' <span class="field-type">' + f.type + '</span></label>';
        html += '<input data-col="' + escapeHtml(col) + '" placeholder="JSON value">';
        html += '</div>';
        continue;
      }
      if (f.type === 'boolean') {
        html += '<div class="form-field"><label>' + escapeHtml(col) + ' <span class="field-type">' + f.type + '</span></label>';
        html += '<select data-col="' + escapeHtml(col) + '"><option value="">—</option><option value="true">true</option><option value="false">false</option></select>';
        html += '</div>';
        continue;
      }
      if (f.type === 'number' || f.type === 'integer' || f.type === 'timestamp') {
        html += '<div class="form-field"><label>' + escapeHtml(col) + ' <span class="field-type">' + f.type + '</span></label>';
        html += '<input data-col="' + escapeHtml(col) + '" type="number" placeholder="' + f.type + '">';
        html += '</div>';
        continue;
      }
      // Default: string, etc.
      html += '<div class="form-field"><label>' + escapeHtml(col) + ' <span class="field-type">' + f.type + (f.required ? ' *' : '') + '</span></label>';
      html += '<input data-col="' + escapeHtml(col) + '" placeholder="' + escapeHtml(col) + '">';
      html += '</div>';
    }
  }
  html += '</div>';
  html += '<div class="create-form-actions"><button onclick="submitCreate()" class="btn-save">Create</button>';
  html += '<div id="create-error" class="error hidden"></div></div>';
  html += '</div>';

  // Table
  html += '<div class="table-wrap"><table><thead><tr>';
  cols.forEach(c => {
    const f = schema ? schema[c] : null;
    const refLabel = f && f.refTable ? ' &rarr; ' + escapeHtml(f.refTable) + '.' + escapeHtml(f.refField || 'id') : '';
    const typeLabel = f ? ' <span class="col-type">' + f.type + refLabel + '</span>' : '';
    html += '<th>' + escapeHtml(c) + typeLabel + '</th>';
  });
  html += '<th class="actions-col">Actions</th></tr></thead><tbody>';

  if (data.rows.length === 0) {
    html += '<tr><td colspan="' + (cols.length + 1) + '" class="empty-row">No rows</td></tr>';
  }

  data.rows.forEach(row => {
    const pk = row[cols[0]];
    html += '<tr data-id="' + escapeHtml(pk) + '">';
    cols.forEach(c => {
      let val = row[c];
      const ro = isReadOnly(c) || isAutoField(c);
      const f = schema ? schema[c] : null;
      const isRef = f && (f.type === 'ref' || f.type === 'refMulti') && f.refTable;
      const isObj = typeof val === 'object' && val !== null;
      const display = isObj ? JSON.stringify(val) : val;
      const redacted = typeof val === 'string' && val === '[REDACTED]';
      const truncated = typeof display === 'string' && display.length > 80
        ? escapeHtml(display.slice(0, 80)) + '...'
        : escapeHtml(display);

      if (ro || redacted) {
        html += '<td class="cell-ro">' + (truncated || '<em>null</em>') + '</td>';
      } else {
        const refAttr = isRef ? ' data-ref="' + escapeHtml(f.refTable) + '"' : '';
        html += '<td class="cell-edit' + (isRef ? ' ref-cell' : '') + '" data-pk="' + escapeHtml(pk) + '" data-col="' + escapeHtml(c) + '" data-raw="' + escapeHtml(isObj ? JSON.stringify(val) : String(val ?? '')) + '"' + refAttr + ' onclick="startEdit(this)">';
        html += (truncated || '<em>null</em>');
        html += '</td>';
      }
    });
    html += '<td><button data-pk="' + escapeHtml(pk) + '" onclick="deleteRow(this.dataset.pk)" class="btn-sm btn-danger">Delete</button></td>';
    html += '</tr>';
  });

  html += '</tbody></table></div>';

  // Pagination
  if (data.pages > 1) {
    html += '<div class="pagination">';
    if (currentPage > 1) html += '<button onclick="prevPage()">Prev</button>';
    html += '<span>Page ' + data.page + ' of ' + data.pages + '</span>';
    if (currentPage < data.pages) html += '<button onclick="nextPage()">Next</button>';
    html += '</div>';
  }

  content.innerHTML = html;
  // Populate ref selects and resolve ref cell labels asynchronously
  populateRefSelects();
  resolveRefCells();
}

async function populateRefSelects() {
  const selects = document.querySelectorAll('select.ref-select');
  for (const sel of selects) {
    const refTable = sel.getAttribute('data-ref');
    if (!refTable) continue;
    const isMulti = sel.hasAttribute('multiple');
    const currentVal = sel.getAttribute('data-current') || '';
    const options = await fetchRefOptions(refTable);
    sel.innerHTML = '';
    if (!isMulti) {
      const empty = document.createElement('option');
      empty.value = '';
      empty.textContent = '—';
      sel.appendChild(empty);
    }
    options.forEach(function(opt) {
      const o = document.createElement('option');
      o.value = opt.id;
      o.textContent = opt.id + (opt.label !== opt.id ? ' — ' + opt.label : '');
      if (currentVal === opt.id) o.selected = true;
      sel.appendChild(o);
    });
  }
}

async function resolveRefCells() {
  const cells = document.querySelectorAll('td.ref-cell');
  // Group by ref table to batch fetches
  const byTable = {};
  cells.forEach(function(td) {
    const ref = td.getAttribute('data-ref');
    if (!ref) return;
    if (!byTable[ref]) byTable[ref] = [];
    byTable[ref].push(td);
  });
  for (const refTable of Object.keys(byTable)) {
    const options = await fetchRefOptions(refTable);
    const lookup = {};
    options.forEach(function(opt) { lookup[opt.id] = opt.label; });
    byTable[refTable].forEach(function(td) {
      const raw = td.getAttribute('data-raw');
      if (!raw) return;
      const label = lookup[raw];
      if (label && label !== raw) {
        td.innerHTML = '<span class="ref-id">' + escapeHtml(raw) + '</span> <span class="ref-label">' + escapeHtml(label) + '</span>';
      }
    });
  }
}

// ---- Inline editing ----
function startEdit(td) {
  // If we're already editing this cell, don't restart
  if (editingCell === td) return;
  if (editingCell) commitEdit(editingCell);
  editingCell = td;
  const raw = td.getAttribute('data-raw');
  const col = td.getAttribute('data-col');
  const schema = getTableSchema();
  const f = schema ? schema[col] : null;

  td.classList.add('editing');

  if (f && f.type === 'boolean') {
    const cur = raw === 'true' ? 'true' : raw === 'false' ? 'false' : '';
    const sel = document.createElement('select');
    sel.className = 'cell-input';
    ['', 'true', 'false'].forEach(v => {
      const opt = document.createElement('option');
      opt.value = v;
      opt.textContent = v || 'null';
      if (v === cur) opt.selected = true;
      sel.appendChild(opt);
    });
    sel.onchange = () => commitEdit(td);
    sel.onblur = () => { td._blurTimer = setTimeout(() => commitEdit(td), 80); };
    sel.onfocus = () => { clearTimeout(td._blurTimer); };
    td.textContent = '';
    td.appendChild(sel);
    sel.focus();
    return;
  }

  if (f && f.type === 'ref' && f.refTable) {
    const sel = document.createElement('select');
    sel.className = 'cell-input';
    const loading = document.createElement('option');
    loading.value = raw;
    loading.textContent = raw + ' (loading...)';
    loading.selected = true;
    sel.appendChild(loading);
    sel.onchange = () => commitEdit(td);
    sel.onblur = () => { td._blurTimer = setTimeout(() => commitEdit(td), 80); };
    sel.onfocus = () => { clearTimeout(td._blurTimer); };
    td.textContent = '';
    td.appendChild(sel);
    sel.focus();
    // Load options async
    fetchRefOptions(f.refTable).then(options => {
      sel.innerHTML = '';
      const empty = document.createElement('option');
      empty.value = '';
      empty.textContent = '—';
      sel.appendChild(empty);
      options.forEach(function(opt) {
        const o = document.createElement('option');
        o.value = opt.id;
        o.textContent = opt.id + (opt.label !== opt.id ? ' — ' + opt.label : '');
        if (raw === opt.id) o.selected = true;
        sel.appendChild(o);
      });
    });
    return;
  }

  if (f && f.type === 'enum' && f.enumValues) {
    const sel = document.createElement('select');
    sel.className = 'cell-input';
    const empty = document.createElement('option');
    empty.value = '';
    empty.textContent = '—';
    sel.appendChild(empty);
    f.enumValues.forEach(function(v) {
      const o = document.createElement('option');
      o.value = v;
      o.textContent = v;
      if (raw === v) o.selected = true;
      sel.appendChild(o);
    });
    sel.onchange = () => commitEdit(td);
    sel.onblur = () => { td._blurTimer = setTimeout(() => commitEdit(td), 80); };
    sel.onfocus = () => { clearTimeout(td._blurTimer); };
    td.textContent = '';
    td.appendChild(sel);
    sel.focus();
    return;
  }

  const inp = document.createElement('input');
  inp.className = 'cell-input';
  inp.value = raw;
  inp.onblur = () => { td._blurTimer = setTimeout(() => commitEdit(td), 80); };
  inp.onfocus = () => { clearTimeout(td._blurTimer); };
  inp.onmousedown = (e) => { e.stopPropagation(); };
  inp.onkeydown = (e) => { if(e.key==='Enter') commitEdit(td); if(e.key==='Escape') cancelEdit(td); };
  td.textContent = '';
  td.appendChild(inp);
  inp.focus();
  inp.select();
}

async function commitEdit(td) {
  if (!td || !td.classList.contains('editing')) return;
  clearTimeout(td._blurTimer);
  const input = td.querySelector('input, select');
  if (!input) return;
  const newVal = input.value;
  const oldVal = td.getAttribute('data-raw');
  const pk = td.getAttribute('data-pk');
  const col = td.getAttribute('data-col');

  td.classList.remove('editing');
  editingCell = null;

  if (newVal === oldVal) {
    await loadRows();
    return;
  }

  // Parse value based on schema type
  const schema = getTableSchema();
  const f = schema ? schema[col] : null;
  let parsed = newVal;
  if (f) {
    if (f.type === 'number' || f.type === 'integer' || f.type === 'timestamp') {
      parsed = newVal === '' ? null : Number(newVal);
    } else if (f.type === 'boolean') {
      parsed = newVal === '' ? null : newVal === 'true';
    } else if (f.type === 'json' || f.type === 'vector' || f.type === 'roles' || f.type === 'set' || f.type === 'refMulti' || f.type === 'fileMulti') {
      try { parsed = JSON.parse(newVal); } catch { parsed = newVal; }
    }
  }

  try {
    const update = {};
    update[col] = parsed;
    await api('/tables/' + currentTable + '/rows/' + pk, {
      method: 'PUT',
      body: JSON.stringify(update)
    });
    td.classList.add('flash-update');
    setTimeout(() => td.classList.remove('flash-update'), 800);
  } catch(err) {
    td.classList.add('flash-error');
    setTimeout(() => td.classList.remove('flash-error'), 800);
  }
  await loadRows();
}

function cancelEdit(td) {
  td.classList.remove('editing');
  editingCell = null;
  loadRows();
}

// ---- Create row ----
function showCreateForm() {
  document.getElementById('create-form').classList.remove('hidden');
  const first = document.querySelector('#create-form input, #create-form select');
  if (first) first.focus();
}

function hideCreateForm() {
  document.getElementById('create-form').classList.add('hidden');
  document.getElementById('create-error').classList.add('hidden');
}

async function submitCreate() {
  const schema = getTableSchema();
  if (!schema) return;
  const cols = Object.keys(schema);
  const data = {};
  const errEl = document.getElementById('create-error');
  errEl.classList.add('hidden');

  for (const col of cols) {
    if (isAutoField(col)) continue;
    const input = document.querySelector('#create-form [data-col="' + col + '"]');
    if (!input) continue;

    const f = schema[col];
    // Handle multi-select for refMulti
    if (f.type === 'refMulti' && input.tagName === 'SELECT' && input.multiple) {
      const selected = Array.from(input.selectedOptions).map(function(o) { return o.value; }).filter(Boolean);
      if (selected.length > 0) data[col] = selected;
      continue;
    }

    let val = input.value;
    if (val === '') continue;

    if (f.type === 'number' || f.type === 'integer' || f.type === 'timestamp') {
      data[col] = Number(val);
    } else if (f.type === 'boolean') {
      data[col] = val === 'true';
    } else if (f.type === 'json' || f.type === 'vector' || f.type === 'roles' || f.type === 'set' || f.type === 'fileMulti') {
      try { data[col] = JSON.parse(val); } catch { data[col] = val; }
    } else {
      data[col] = val;
    }
  }

  try {
    await api('/tables/' + currentTable + '/rows', {
      method: 'POST',
      body: JSON.stringify(data)
    });
    hideCreateForm();
    // Clear inputs
    document.querySelectorAll('#create-form input, #create-form select').forEach(i => {
      if (i.tagName === 'SELECT') i.selectedIndex = 0;
      else i.value = '';
    });
    await loadRows();
    await loadTables();
  } catch(err) {
    errEl.textContent = err.message || 'Create failed';
    errEl.classList.remove('hidden');
  }
}

// ---- Pagination ----
function prevPage() { currentPage--; syncToHash(); loadRows(); }
function nextPage() { currentPage++; syncToHash(); loadRows(); }

// ---- Delete ----
async function deleteRow(id) {
  if (!confirm('Delete row ' + id + '?')) return;
  await api('/tables/' + currentTable + '/rows/' + id, { method: 'DELETE' });
  await loadRows();
  await loadTables();
}

// ---- Backup ----
async function downloadBackup() {
  const res = await fetch('/_/api/backup', {
    headers: { 'Authorization': 'Bearer ' + TOKEN }
  });
  const blob = await res.blob();
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'flop-backup-' + new Date().toISOString().replace(/[:.]/g, '-') + '.tar.gz';
  a.click();
}

async function uploadBackup(input) {
  const file = input.files[0];
  if (!file) return;
  if (!confirm('This will REPLACE all data. Continue?')) return;
  const res = await fetch('/_/api/backup', {
    method: 'POST',
    headers: { 'Authorization': 'Bearer ' + TOKEN },
    body: file,
  });
  const data = await res.json();
  alert(data.message || data.error || 'Done');
  window.location.reload();
}

function logout() {
  localStorage.removeItem('flop_admin_token');
  if (evtSource) evtSource.close();
  window.location.href = '/_/login';
}

// ---- Init ----
readFromHash();
loadTables().then(() => {
  if (currentTable) {
    renderNav();
    loadRows();
  }
  connectSSE();
});
window.addEventListener('hashchange', () => {
  const prev = currentTable;
  const prevPage = currentPage;
  readFromHash();
  if (currentTable !== prev || currentPage !== prevPage) {
    renderNav();
    loadRows();
  }
});
</script>
</body></html>`;
}

const baseCSS = `
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',system-ui,sans-serif;background:#0a0a0a;color:#e0e0e0;font-size:14px}
h1{font-size:1.4rem;font-weight:700;letter-spacing:-0.5px}
h1 .badge{font-size:0.65rem;background:#333;padding:2px 6px;border-radius:4px;vertical-align:middle;color:#888}
input{width:100%;padding:10px 12px;background:#111;border:1px solid #333;border-radius:6px;color:#e0e0e0;font-size:14px;margin-bottom:10px}
input:focus{outline:none;border-color:#555}
button{cursor:pointer;padding:10px 16px;background:#1a1a2e;border:1px solid #333;border-radius:6px;color:#e0e0e0;font-size:14px}
button:hover{background:#252545}
.error{color:#ff6b6b;font-size:13px;margin-top:8px}
.hidden{display:none}
.login-container{max-width:340px;margin:120px auto;padding:32px;background:#111;border:1px solid #222;border-radius:12px}
.login-container h1{margin-bottom:24px;text-align:center}
`;

const adminCSS = `
#app{display:flex;height:100vh}
nav{width:240px;background:#111;border-right:1px solid #222;padding:16px;display:flex;flex-direction:column;gap:8px;flex-shrink:0}
nav h1{margin-bottom:16px}
.nav-btn{display:flex;justify-content:space-between;width:100%;text-align:left;padding:8px 12px;background:transparent;border:1px solid transparent;border-radius:6px;font-size:13px}
.nav-btn:hover{background:#1a1a1a;border-color:#333}
.nav-btn.active{background:#1a1a2e;border-color:#333}
.count{color:#666;font-size:12px}
.nav-bottom{margin-top:auto;display:flex;flex-direction:column;gap:6px}
.btn-sm{padding:6px 10px;font-size:12px;text-align:center}
.btn-danger{color:#ff6b6b;border-color:#3a1a1a}
.btn-danger:hover{background:#2a1111}
main{flex:1;overflow:auto;padding:24px}
.empty{color:#555;text-align:center;padding:80px 0;font-size:15px}
.empty-row{color:#555;text-align:center;padding:24px 0}
.table-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px}
.table-actions{display:flex;align-items:center;gap:12px}
.meta{color:#666;font-size:13px}
.btn-create{padding:6px 14px;font-size:12px;background:#1a2e1a;border-color:#2a3a2a;color:#8f8}
.btn-create:hover{background:#253525}
.table-wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;padding:8px 12px;background:#111;border-bottom:1px solid #222;color:#888;font-weight:500;position:sticky;top:0}
th .col-type{font-weight:400;color:#555;font-size:11px}
td{padding:8px 12px;border-bottom:1px solid #1a1a1a;max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.actions-col{width:80px}
tr:hover td{background:#111}
em{color:#444}
.cell-edit{cursor:pointer;transition:background .15s}
.cell-edit:hover{background:#1a1a2e!important}
.cell-ro{color:#777}
.editing{padding:0!important;background:#0a0a1a!important}
.cell-input{width:100%;padding:8px 12px;background:#0a0a1a;border:2px solid #3a3a6a;color:#e0e0e0;font-size:13px;font-family:inherit;outline:none;border-radius:0;margin:0}
select.cell-input{-webkit-appearance:auto;appearance:auto}
@keyframes flashInsert{0%{background:#1a3a1a}100%{background:transparent}}
@keyframes flashUpdate{0%{background:#1a1a3a}100%{background:transparent}}
@keyframes flashDelete{0%{background:#3a1a1a}100%{background:transparent}}
@keyframes flashError{0%{background:#3a1a1a}100%{background:transparent}}
.flash-insert td,.flash-insert{animation:flashInsert .8s ease-out}
.flash-update td,.flash-update{animation:flashUpdate .8s ease-out}
.flash-delete td,.flash-delete{animation:flashDelete .8s ease-out}
.flash-error{animation:flashError .8s ease-out}
.create-form{background:#111;border:1px solid #222;border-radius:8px;padding:16px;margin-bottom:16px}
.create-form-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}
.btn-close{padding:4px 8px;font-size:12px;background:transparent;border:none;color:#888;cursor:pointer}
.btn-close:hover{color:#fff}
.create-form-fields{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:8px;margin-bottom:12px}
.form-field label{display:block;font-size:12px;color:#888;margin-bottom:4px}
.form-field .field-type{color:#555}
.form-field input,.form-field select{margin-bottom:0;padding:8px 10px;font-size:13px}
.create-form-actions{display:flex;align-items:center;gap:12px}
.btn-save{padding:8px 20px;font-size:13px;background:#1a2e1a;border-color:#2a3a2a;color:#8f8}
.btn-save:hover{background:#253525}
.pagination{display:flex;align-items:center;gap:12px;margin-top:16px;justify-content:center}
.pagination span{color:#666;font-size:13px}
.ref-id{color:#666;font-size:11px}
.ref-label{color:#b0b0e0}
`;
