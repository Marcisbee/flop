import { Flop } from "./flop-client.js";

function $(id) {
  return document.getElementById(id);
}

function normalizePath(pathname) {
  if (!pathname) return "/";
  if (pathname !== "/" && pathname.endsWith("/")) return pathname.slice(0, -1);
  return pathname;
}

function isMoviePath(pathname) {
  if (!pathname.startsWith("/movie/")) return false;
  const slug = pathname.slice("/movie/".length);
  return slug.length > 0 && !slug.includes("/");
}

function canHandlePath(pathname) {
  return pathname === "/" || pathname === "/404" || isMoviePath(pathname);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

async function getJSON(url, options = {}) {
  const res = await fetch(url, options);
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    const msg = body?.error || body?.message || `Request failed: ${res.status}`;
    throw new Error(msg);
  }
  return body;
}

const headCache = new Map();
let statsCache = null;
let moviesCache = null;
let searchTimer = null;
const client = new Flop({ host: "" });

async function getHead(pathname) {
  if (headCache.has(pathname)) return headCache.get(pathname);
  const json = await getJSON(`/api/head?path=${encodeURIComponent(pathname)}`);
  const head = json.data || { title: "Flop Movies", meta: [] };
  headCache.set(pathname, head);
  return head;
}

function applyHead(head) {
  document.title = head.title || "Flop Movies";
  document.querySelectorAll('meta[data-flop-managed="1"]').forEach((el) => el.remove());

  for (const m of head.meta || []) {
    if (!m?.name) continue;
    const el = document.createElement("meta");
    el.setAttribute("name", m.name);
    el.setAttribute("content", m.content || "");
    el.setAttribute("data-flop-managed", "1");
    document.head.appendChild(el);
  }
}

function renderShell() {
  const root = $("app");
  root.innerHTML = `
    <div class="grain"></div>
    <header class="masthead">
      <p class="kicker">Flop Demo</p>
      <h1>Movie Atlas</h1>
      <p class="lede">Search the movie catalog instantly and open any movie page.</p>
    </header>

    <main class="layout">
      <section class="search-panel">
        <label for="searchInput">Search title</label>
        <input id="searchInput" type="search" autocomplete="off" placeholder="Type: blade, parasite, memory..." />
        <div id="suggestions" class="suggestions" aria-live="polite"></div>
        <p id="stats" class="stats"></p>
        <p class="hint">Autocomplete is prefix-indexed and optimized for large datasets.</p>
      </section>

      <section id="routeContent" class="content-panel"></section>
    </main>
  `;
}

function normalizeGenres(genres) {
  if (!Array.isArray(genres)) return [];
  return genres.map((g) => String(g));
}

function ratingText(movie) {
  const n = Number(movie.rating || 0);
  if (!Number.isFinite(n) || n <= 0) return "n/a";
  return n.toFixed(1);
}

function votesText(movie) {
  const n = Number(movie.votes || 0);
  if (!Number.isFinite(n) || n <= 0) return "0";
  return n.toLocaleString();
}

function movieCard(movie) {
  const genres = normalizeGenres(movie.genres).slice(0, 3).join(" · ");
  const year = Number(movie.year || 0);
  return `
    <article class="movie-card">
      <a href="/movie/${encodeURIComponent(movie.slug)}" data-flop-link="1" class="movie-title">${escapeHtml(movie.title)}</a>
      <p class="movie-meta">${year || "Unknown"} · ${Number(movie.runtimeMinutes || 0) || "?"}m · ★ ${ratingText(movie)}</p>
      <p class="movie-overview">${escapeHtml(movie.overview || "No overview.")}</p>
      <p class="movie-genres">${escapeHtml(genres)}</p>
    </article>
  `;
}

async function fetchStats() {
  const data = await client.view("get_stats", {});
  statsCache = data || { movies: 0 };
  return statsCache;
}

async function fetchMovies(limit = 36, offset = 0) {
  const key = `${limit}:${offset}`;
  if (moviesCache?.key === key) return moviesCache.rows;
  const data = await client.view("list_movies", { limit, offset });
  const rows = Array.isArray(data) ? data : [];
  moviesCache = { key, rows };
  return rows;
}

function setStatsLine(stats) {
  const el = $("stats");
  const total = Number(stats?.movies || 0).toLocaleString();
  if (stats?.autocompleteReady) {
    el.textContent = `${total} movies indexed`;
    return;
  }
  if (stats?.autocompleteBuild) {
    el.textContent = `${total} movies loaded, search index warming up...`;
    return;
  }
  el.textContent = `${total} movies loaded`;
}

async function refreshHome() {
  const [stats, rows] = await Promise.all([fetchStats(), fetchMovies(36, 0)]);
  setStatsLine(stats);

  const content = $("routeContent");
  content.innerHTML = `
    <div class="content-head">
      <h2>Catalog Snapshot</h2>
      <p>Newest by release year.</p>
    </div>
    <div class="movie-grid">
      ${rows.map(movieCard).join("")}
    </div>
  `;
}

async function renderMoviePage(slug) {
  const content = $("routeContent");
  content.innerHTML = `<p class="muted">Loading movie...</p>`;

  try {
    const movie = await client.view("get_movie_by_slug", { slug });
    if (!movie) {
      renderNotFound();
      return;
    }

    const genres = normalizeGenres(movie.genres);
    content.innerHTML = `
      <article class="movie-page">
        <a href="/" data-flop-link="1" class="back-link">← Back to catalog</a>
        <h2>${escapeHtml(movie.title)}</h2>
        <p class="movie-page-meta">${Number(movie.year || 0)} · ${Number(movie.runtimeMinutes || 0)} minutes · ★ ${ratingText(movie)} · ${votesText(movie)} votes</p>
        <p class="movie-page-overview">${escapeHtml(movie.overview || "No overview available.")}</p>
        <div class="chips">${genres.map((g) => `<span>${escapeHtml(g)}</span>`).join("")}</div>
        <p class="slug">slug: ${escapeHtml(movie.slug || "")}</p>
      </article>
    `;
  } catch {
    renderNotFound();
  }
}

function renderNotFound() {
  const content = $("routeContent");
  content.innerHTML = `
    <div class="not-found">
      <h2>Movie Not Found</h2>
      <p>The requested page does not exist.</p>
      <a href="/" data-flop-link="1">Return to catalog</a>
    </div>
  `;
}

async function updateSuggestions(query) {
  const box = $("suggestions");
  const value = query.trim();
  if (!value) {
    box.innerHTML = "";
    return;
  }

  try {
    if (!statsCache) {
      try {
        await fetchStats();
      } catch {
        // Ignore stats errors for search path.
      }
    }
    const data = await client.view("autocomplete_movies", { q: value, limit: 10 });
    const items = Array.isArray(data) ? data : [];
    if (items.length === 0) {
      if (statsCache && statsCache.autocompleteReady === false) {
        box.innerHTML = `<p class="muted">Search index is still warming up...</p>`;
      } else {
        box.innerHTML = `<p class="muted">No matches</p>`;
      }
      return;
    }

    box.innerHTML = items
      .map((it) => `<a href="/movie/${encodeURIComponent(it.slug)}" data-flop-link="1"><strong>${escapeHtml(it.title)}</strong><span>${escapeHtml(String(it.year || ""))}</span></a>`)
      .join("");
  } catch {
    box.innerHTML = `<p class="error">Autocomplete unavailable</p>`;
  }
}

function bindInteractions() {
  const input = $("searchInput");
  input.addEventListener("input", () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      updateSuggestions(input.value);
    }, 90);
  });

  input.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    const first = document.querySelector("#suggestions a[data-flop-link='1']");
    if (!first) return;
    event.preventDefault();
    navigate(new URL(first.href).pathname, false, false);
  });
}

async function renderPath(pathname, opts = {}) {
  pathname = normalizePath(pathname);
  if (!canHandlePath(pathname)) pathname = "/404";

  let head = null;
  if (opts.initial && window.__FLOP_INITIAL_PATH__ === pathname && window.__FLOP_INITIAL_HEAD__) {
    head = window.__FLOP_INITIAL_HEAD__;
    headCache.set(pathname, head);
  } else {
    head = await getHead(pathname);
  }
  applyHead(head);

  if (pathname === "/") {
    await refreshHome();
    return;
  }
  if (isMoviePath(pathname)) {
    const slug = decodeURIComponent(pathname.slice("/movie/".length));
    await renderMoviePage(slug);
    return;
  }
  renderNotFound();
}

async function navigate(pathname, replace = false, initial = false) {
  pathname = normalizePath(pathname);
  const target = canHandlePath(pathname) ? pathname : "/404";
  const current = normalizePath(window.location.pathname);

  if (target !== current) {
    if (replace) history.replaceState({}, "", target);
    else history.pushState({}, "", target);
  }

  await renderPath(target, { initial });
}

document.addEventListener("click", (event) => {
  const link = event.target.closest("a[href]");
  if (!link) return;
  if (link.target === "_blank" || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;

  const url = new URL(link.href, window.location.origin);
  if (url.origin !== window.location.origin) return;

  const pathname = normalizePath(url.pathname);
  if (!canHandlePath(pathname)) return;

  event.preventDefault();
  navigate(pathname, false, false);
});

window.addEventListener("popstate", () => {
  const pathname = normalizePath(window.location.pathname);
  if (!canHandlePath(pathname)) {
    window.location.reload();
    return;
  }
  navigate(pathname, true, false);
});

renderShell();
bindInteractions();
navigate(normalizePath(window.location.pathname), true, true);
