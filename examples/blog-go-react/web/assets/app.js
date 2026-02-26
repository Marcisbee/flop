function $(id) {
  return document.getElementById(id);
}

function normalizePath(pathname) {
  if (!pathname) return "/";
  if (pathname !== "/" && pathname.endsWith("/")) return pathname.slice(0, -1);
  return pathname;
}

function isPostPath(pathname) {
  if (!pathname.startsWith("/post/")) return false;
  const slug = pathname.slice("/post/".length);
  return slug.length > 0 && !slug.includes("/");
}

function canHandlePath(pathname) {
  return pathname === "/" || pathname === "/about" || pathname === "/404" || isPostPath(pathname);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

let postsCache = null;
const headCache = new Map();

async function getPosts() {
  if (postsCache) return postsCache;
  const res = await fetch("/api/posts");
  const json = await res.json();
  postsCache = json.data || [];
  return postsCache;
}

async function getHead(pathname) {
  if (headCache.has(pathname)) return headCache.get(pathname);
  const res = await fetch(`/api/head?path=${encodeURIComponent(pathname)}`);
  const json = await res.json();
  const head = json.data || { title: "My Blog", meta: [] };
  headCache.set(pathname, head);
  return head;
}

function applyHead(head) {
  document.title = head.title || "My Blog";
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
    <header class="top">
      <h1>Flop Blog</h1>
      <p>Go-first app spec + generated TS artifacts</p>
      <nav class="nav">
        <a href="/" data-flop-link="1">Home</a>
        <a href="/about" data-flop-link="1">About</a>
        <a href="/_">Admin</a>
      </nav>
    </header>
    <main>
      <section>
        <h2 id="routeTitle">Posts</h2>
        <ul id="posts"></ul>
      </section>
    </main>
  `;
}

function setActiveNav(pathname) {
  const links = document.querySelectorAll('a[data-flop-link="1"]');
  for (const link of links) {
    const href = link.getAttribute("href");
    const active = href === "/" ? pathname === "/" : pathname.startsWith(href);
    link.setAttribute("aria-current", active ? "page" : "false");
  }
}

function renderHome(posts) {
  const titleEl = $("routeTitle");
  const postsEl = $("posts");
  titleEl.textContent = "Posts";
  postsEl.innerHTML = "";
  for (const p of posts) {
    const li = document.createElement("li");
    li.innerHTML =
      `<a href="/post/${encodeURIComponent(p.slug)}" data-flop-link="1">${escapeHtml(p.title)}</a>` +
      ` - ${escapeHtml(p.authorName)}`;
    postsEl.appendChild(li);
  }
}

function renderPost(posts, slug) {
  const titleEl = $("routeTitle");
  const postsEl = $("posts");
  const post = posts.find((p) => p.slug === slug);

  if (!post) {
    renderNotFound();
    return;
  }

  titleEl.textContent = "Post";
  postsEl.innerHTML = "";

  const title = document.createElement("li");
  title.innerHTML = `<strong>${escapeHtml(post.title)}</strong> by ${escapeHtml(post.authorName)}`;
  postsEl.appendChild(title);

  const body = document.createElement("li");
  body.textContent = post.body;
  postsEl.appendChild(body);
}

function renderAbout() {
  const titleEl = $("routeTitle");
  const postsEl = $("posts");
  titleEl.textContent = "About";
  postsEl.innerHTML = "";
  const li = document.createElement("li");
  li.textContent = "About page placeholder for the blog-go-react scaffold.";
  postsEl.appendChild(li);
}

function renderNotFound() {
  const titleEl = $("routeTitle");
  const postsEl = $("posts");
  titleEl.textContent = "Not Found";
  postsEl.innerHTML = "";
  const li = document.createElement("li");
  li.innerHTML = `Page not found. <a href="/" data-flop-link="1">Go home</a>.`;
  postsEl.appendChild(li);
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
  setActiveNav(pathname);

  const posts = await getPosts();
  if (pathname === "/") {
    renderHome(posts);
    return;
  }
  if (pathname === "/about") {
    renderAbout();
    return;
  }
  if (isPostPath(pathname)) {
    const slug = decodeURIComponent(pathname.slice("/post/".length));
    renderPost(posts, slug);
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
navigate(normalizePath(window.location.pathname), true, true);
