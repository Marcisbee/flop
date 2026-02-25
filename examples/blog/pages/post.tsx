// Post page — displays a single blog post with comments
// Reads :slug from URL, fetches from /api/view/get_post and /api/view/get_comments

import { useState, useEffect } from "react";

interface Post {
  id: string;
  slug: string;
  title: string;
  excerpt: string;
  body: string;
  authorName: string;
  publishedAt: number;
  coverImage?: { url: string; name: string } | null;
}

interface Comment {
  id: string;
  authorName: string;
  body: string;
  createdAt: number;
}

export default function PostPage({ params }: { params: { slug: string } }) {
  const [post, setPost] = useState<Post | null>(null);
  const [comments, setComments] = useState<Comment[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`/api/view/get_post?slug=${params.slug}`)
      .then((r) => r.json())
      .then((res) => {
        setPost(res.data ?? null);
        setLoading(false);
        if (res.data?.id) {
          fetch(`/api/view/get_comments?postId=${res.data.id}`)
            .then((r) => r.json())
            .then((cr) => setComments(cr.data ?? []));
        }
      });
  }, [params.slug]);

  if (loading) return <div className="loading">Loading...</div>;
  if (!post) return <div className="not-found">Post not found</div>;

  return (
    <article className="post">
      <header className="post-header">
        <h1>{post.title}</h1>
        <div className="post-meta">
          <span>{post.authorName}</span>
          <time>{new Date(post.publishedAt).toLocaleDateString()}</time>
        </div>
      </header>
      {post.coverImage && (
        <img className="post-cover" src={post.coverImage.url} alt={post.title} />
      )}
      <div className="post-body" dangerouslySetInnerHTML={{ __html: post.body }} />
      <section className="comments">
        <h3>Comments ({comments.length})</h3>
        {comments.length === 0 && <p>No comments yet.</p>}
        {comments.map((c) => (
          <div key={c.id} className="comment">
            <strong>{c.authorName}</strong>
            <time>{new Date(c.createdAt).toLocaleDateString()}</time>
            <p>{c.body}</p>
          </div>
        ))}
      </section>
    </article>
  );
}
