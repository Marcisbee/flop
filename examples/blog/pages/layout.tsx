// Layout component — wraps all pages, provides navigation
// This runs client-side only; the server renders the HTML shell from head() config

export default function Layout({ children }: { children?: any }) {
  return (
    <div className="layout">
      <nav className="nav">
        <a href="/" className="nav-brand">My Blog</a>
        <div className="nav-links">
          <a href="/">Home</a>
          <a href="/about">About</a>
        </div>
      </nav>
      <main className="main">
        {children}
      </main>
      <footer className="footer">
        <p>Powered by Flop</p>
      </footer>
    </div>
  );
}
