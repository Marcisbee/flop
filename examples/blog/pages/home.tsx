// Home page — lists all published blog posts
// Fetches data from /api/view/list_posts

import { useState, useEffect } from "react";

interface Post {
  id: string;
  slug: string;
  title: string;
  excerpt: string;
  authorName: string;
  publishedAt: number;
  coverImage?: { url: string; name: string; mime: string } | null;
}

export default function Home() {
  const [posts, setPosts] = useState<Post[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/view/list_posts")
      .then((r) => r.json())
      .then((res) => {
        setPosts(res.data ?? []);
        setLoading(false);
      });
  }, []);

  if (loading) return <div className="loading">Loading...</div>;

  return (
    <div className="home">
      <h1>Blog</h1>
      {posts.length === 0 && <p className="empty">No posts yet.</p>}
      <div className="post-list">
        {posts.map((post) => (
          <article key={post.id} className="post-card">
            <a href={`/post/${post.slug}`}>
              <h2>{post.title}</h2>
            </a>
            {post.excerpt && <p className="excerpt">{post.excerpt}</p>}
            <div className="post-meta">
              <span>{post.authorName}</span>
              <time>{new Date(post.publishedAt).toLocaleDateString()}</time>
            </div>
          </article>
        ))}
      </div>
    </div>
  );
}
