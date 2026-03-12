// Simple SPA for Movies demo
const API = '';
let currentPage = 0;
const PAGE_SIZE = 36;
let searchTimer = null;

// Router
function navigate(path, push = true) {
  if (push) history.pushState(null, '', path);
  render();
}

window.addEventListener('popstate', () => render());

// API helpers
async function fetchJSON(url) {
  const res = await fetch(API + url);
  return res.json();
}

// Render
function render() {
  const path = location.pathname;
  const app = document.getElementById('app');

  if (path === '/') {
    renderHome(app);
  } else if (path.startsWith('/movie/')) {
    const slug = path.slice(7);
    renderMovie(app, slug);
  } else {
    app.innerHTML = `
      <div class="not-found">
        <h2>404</h2>
        <p>Page not found</p>
      </div>`;
  }
}

async function renderHome(app) {
  app.innerHTML = `
    ${headerHTML()}
    <div class="main">
      <div class="loading">Loading movies...</div>
    </div>`;

  setupSearch();

  try {
    const data = await fetchJSON(`/api/movies?limit=${PAGE_SIZE}&offset=${currentPage * PAGE_SIZE}`);
    const stats = await fetchJSON('/api/stats');
    const total = data.total || 0;
    const rows = data.data || [];

    const statsEl = document.querySelector('.stats-line');
    if (statsEl && stats.total) {
      statsEl.textContent = `${stats.total.toLocaleString()} movies indexed`;
    }

    const main = app.querySelector('.main');
    main.innerHTML = `
      <div class="movie-grid">
        ${rows.map(m => movieCard(m)).join('')}
      </div>
      ${rows.length > 0 ? `
        <div class="pagination">
          <button id="prev-btn" ${currentPage === 0 ? 'disabled' : ''}>Previous</button>
          <span class="stats-line">Page ${currentPage + 1} of ${Math.ceil(total / PAGE_SIZE)}</span>
          <button id="next-btn" ${(currentPage + 1) * PAGE_SIZE >= total ? 'disabled' : ''}>Next</button>
        </div>` : '<div class="loading">No movies found</div>'}`;

    main.querySelectorAll('.movie-card').forEach(card => {
      card.addEventListener('click', (e) => {
        e.preventDefault();
        navigate(card.getAttribute('href'));
      });
    });

    const prevBtn = document.getElementById('prev-btn');
    const nextBtn = document.getElementById('next-btn');
    if (prevBtn) prevBtn.addEventListener('click', () => { currentPage--; renderHome(app); });
    if (nextBtn) nextBtn.addEventListener('click', () => { currentPage++; renderHome(app); });
  } catch (err) {
    app.querySelector('.main').innerHTML = `<div class="loading">Error loading movies</div>`;
  }
}

async function renderMovie(app, slug) {
  app.innerHTML = `
    ${headerHTML()}
    <div class="main">
      <div class="loading">Loading...</div>
    </div>`;

  setupSearch();

  try {
    const data = await fetchJSON(`/api/movies/by-slug/${encodeURIComponent(slug)}`);
    const rows = data.data || [];
    const movie = rows[0];

    const main = app.querySelector('.main');
    if (!movie) {
      main.innerHTML = `
        <div class="not-found">
          <h2>Movie not found</h2>
          <p><a href="/" class="back-link">Back to catalog</a></p>
        </div>`;
      main.querySelector('.back-link')?.addEventListener('click', e => {
        e.preventDefault();
        navigate('/');
      });
      return;
    }

    const genres = parseGenres(movie.Data?.genres || movie.genres);
    const m = movie.Data || movie;

    main.innerHTML = `
      <div class="movie-detail">
        <a href="/" class="back-link">&larr; Back to catalog</a>
        <h2>${esc(m.title)}</h2>
        <div class="detail-meta">
          <span>${m.year || ''}</span>
          ${m.runtime_minutes ? `<span>${m.runtime_minutes} min</span>` : ''}
          ${m.rating ? `<span><span class="star">&#9733;</span> ${m.rating}</span>` : ''}
          ${m.votes ? `<span>${Number(m.votes).toLocaleString()} votes</span>` : ''}
        </div>
        <div class="genres">
          ${genres.map(g => `<span class="genre-tag">${esc(g)}</span>`).join('')}
        </div>
        ${m.overview ? `<p class="overview">${esc(m.overview)}</p>` : ''}
      </div>`;

    main.querySelector('.back-link').addEventListener('click', e => {
      e.preventDefault();
      navigate('/');
    });
  } catch (err) {
    app.querySelector('.main').innerHTML = `<div class="loading">Error loading movie</div>`;
  }
}

function movieCard(movie) {
  const m = movie.Data || movie;
  const genres = parseGenres(m.genres);
  const slug = m.slug || '';
  return `
    <a href="/movie/${esc(slug)}" class="movie-card">
      <div class="card-title">${esc(m.title)}</div>
      <div class="card-meta">
        <span>${m.year || ''}</span>
        ${m.runtime_minutes ? `<span>${m.runtime_minutes} min</span>` : ''}
        ${m.rating ? `<span><span class="star">&#9733;</span> ${m.rating}</span>` : ''}
      </div>
      <div class="genres">
        ${genres.slice(0, 3).map(g => `<span class="genre-tag">${esc(g)}</span>`).join('')}
      </div>
    </a>`;
}

function headerHTML() {
  return `
    <header class="header">
      <h1><a href="/" id="home-link"><span>&#127916;</span> Movies</a></h1>
      <div class="search-wrap">
        <input type="text" class="search-input" placeholder="Search movies..." id="search-input" autocomplete="off" />
        <div class="search-results" id="search-results"></div>
      </div>
      <span class="stats-line" id="stats-line"></span>
    </header>`;
}

function setupSearch() {
  const input = document.getElementById('search-input');
  const results = document.getElementById('search-results');
  const homeLink = document.getElementById('home-link');

  if (homeLink) {
    homeLink.addEventListener('click', e => {
      e.preventDefault();
      currentPage = 0;
      navigate('/');
    });
  }

  if (!input || !results) return;

  input.addEventListener('input', () => {
    clearTimeout(searchTimer);
    const q = input.value.trim();
    if (!q) {
      results.innerHTML = '';
      return;
    }
    searchTimer = setTimeout(async () => {
      try {
        const data = await fetchJSON(`/api/search?q=${encodeURIComponent(q)}&limit=10`);
        const rows = data.data || [];
        results.innerHTML = rows.map(r => {
          const slug = r.slug || '';
          const title = r.title || '';
          const year = r.year || '';
          return `<a href="/movie/${esc(slug)}" class="search-result">
            <span class="title">${esc(title)}</span>
            <span class="year">${year}</span>
          </a>`;
        }).join('');
        results.querySelectorAll('.search-result').forEach(el => {
          el.addEventListener('click', e => {
            e.preventDefault();
            input.value = '';
            results.innerHTML = '';
            navigate(el.getAttribute('href'));
          });
        });
      } catch (err) {
        results.innerHTML = '';
      }
    }, 90);
  });

  // Close results on outside click
  document.addEventListener('click', (e) => {
    if (!e.target.closest('.search-wrap')) {
      results.innerHTML = '';
    }
  });
}

function parseGenres(s) {
  if (!s) return [];
  if (Array.isArray(s)) return s;
  try { return JSON.parse(s); } catch { return []; }
}

function esc(s) {
  if (!s) return '';
  const div = document.createElement('div');
  div.textContent = s;
  return div.innerHTML;
}

// Boot
render();
