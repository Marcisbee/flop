// About page — static content

export default function About() {
  return (
    <div className="about">
      <h1>About</h1>
      <p>
        This is a blog built with <strong>Flop</strong> — a code-first database
        with built-in auth, real-time subscriptions, and SSR page routing.
      </p>
      <p>
        The server renders lightweight HTML shells with SEO-ready head tags,
        while the full React SPA runs client-side for interactive navigation.
      </p>
    </div>
  );
}
