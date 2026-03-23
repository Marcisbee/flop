function $(id) {
  return document.getElementById(id);
}

function esc(value) {
  if (value === null || value === undefined) return "<span class='muted'>null</span>";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

async function api(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

function renderShell() {
  const root = $("admin");
  root.innerHTML = `
    <header class="top">
      <h1>Flop Admin</h1>
      <p>Blog example admin view (read-only)</p>
      <nav class="nav">
        <a href="/">Back to Blog</a>
      </nav>
    </header>
    <main>
      <section>
        <h2>Tables</h2>
        <div id="tables" class="cards"></div>
      </section>
      <section>
        <h2 id="rowsTitle">Rows</h2>
        <div id="rowsWrap"></div>
      </section>
    </main>
  `;
}

function renderTableCards(tables) {
  const host = $("tables");
  host.innerHTML = "";
  for (const table of tables) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "card";
    button.innerHTML = `<strong>${table.name}</strong><span>${table.rowCount} rows</span>`;
    button.addEventListener("click", () => loadRows(table.name));
    host.appendChild(button);
  }
}

function renderRows(page) {
  $("rowsTitle").textContent = `${page.table} (${page.total} rows)`;
  const host = $("rowsWrap");
  const rows = page.rows || [];

  if (rows.length === 0) {
    host.innerHTML = "<p class='muted'>No rows.</p>";
    return;
  }

  const fields = Object.keys(rows[0]);
  const table = document.createElement("table");

  const thead = document.createElement("thead");
  const trHead = document.createElement("tr");
  for (const field of fields) {
    const th = document.createElement("th");
    th.textContent = field;
    trHead.appendChild(th);
  }
  thead.appendChild(trHead);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  for (const row of rows) {
    const tr = document.createElement("tr");
    for (const field of fields) {
      const td = document.createElement("td");
      td.innerHTML = esc(row[field]);
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);

  host.replaceChildren(table);
}

async function loadRows(table) {
  try {
    const result = await api(`/_/api/tables/${encodeURIComponent(table)}/rows?limit=100&offset=0`);
    renderRows(result.data);
  } catch (error) {
    $("rowsWrap").innerHTML = `<p class='error'>Failed to load rows: ${esc(error.message)}</p>`;
  }
}

async function init() {
  renderShell();
  try {
    const result = await api("/_/api/tables");
    const tables = result.tables || [];
    renderTableCards(tables);
    if (tables.length > 0) {
      await loadRows(tables[0].name);
    } else {
      $("rowsWrap").innerHTML = "<p class='muted'>No tables.</p>";
    }
  } catch (error) {
    $("admin").innerHTML = `<main><section><p class='error'>Failed to load admin data: ${esc(error.message)}</p></section></main>`;
  }
}

init();
