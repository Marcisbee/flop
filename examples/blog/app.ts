import { flop, table, t, route } from "flop";

// ── Tables ──

const users = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{12}/),
    email: t.string().required().unique(),
    password: t.bcrypt(10).required(),
    name: t.string().required(),
    roles: t.roles(),
  },
  auth: true,
});

const posts = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{8}/),
    slug: t.string().required().unique(),
    title: t.string().required(),
    excerpt: t.string(),
    body: t.string().required(),
    coverImage: t.fileSingle("image/*"),
    authorId: t.refSingle(users, "id").required(),
    published: t.boolean().default(false),
    publishedAt: t.timestamp(),
    createdAt: t.timestamp().default("now"),
  },
});

const comments = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{12}/),
    postId: t.refSingle(posts, "id").required(),
    authorId: t.refSingle(users, "id").required(),
    body: t.string().required(),
    createdAt: t.timestamp().default("now"),
  },
});

// ── Database ──

export const db = flop({ users, posts, comments }, {
  dataDir: `${import.meta.dirname}/data`,
});

// ── Views ──

export const list_posts = db.view({}, async (ctx) => {
  const all = await ctx.db.posts.scan(1000);
  const published = (all as any[])
    .filter((p) => p.published)
    .sort((a, b) => b.publishedAt - a.publishedAt);
  return Promise.all(published.map(async (p) => {
    const author = await ctx.db.users.get(p.authorId) as any;
    return { ...p, authorName: author?.name ?? "Unknown" };
  }));
}).public();

export const get_post = db.view({ slug: t.string() }, async (ctx, { slug }) => {
  const all = await ctx.db.posts.scan(1000);
  const post = (all as any[]).find((p) => p.slug === slug && p.published);
  if (!post) return null;
  const author = await ctx.db.users.get(post.authorId) as any;
  return { ...post, authorName: author?.name ?? "Unknown" };
}).public();

export const get_comments = db.view({ postId: t.string() }, async (ctx, { postId }) => {
  const all = await ctx.db.comments.scan(10000);
  const filtered = (all as any[])
    .filter((c) => c.postId === postId)
    .sort((a, b) => a.createdAt - b.createdAt);
  return Promise.all(filtered.map(async (c) => {
    const author = await ctx.db.users.get(c.authorId) as any;
    return { ...c, authorName: author?.name ?? "Unknown" };
  }));
}).public();

// ── Reducers ──

export const create_post = db.reduce(
  { title: t.string(), slug: t.string(), excerpt: t.string(), body: t.string(), coverImage: t.string() },
  async (ctx, params) => {
    if (!ctx.request.auth) throw new Error("Not logged in");
    return ctx.db.posts.insert({
      ...params,
      authorId: ctx.request.auth.id,
      published: true,
      publishedAt: Date.now(),
    });
  },
).roles("admin");

export const add_comment = db.reduce(
  { postId: t.string(), body: t.string() },
  async (ctx, { postId, body }) => {
    if (!ctx.request.auth) throw new Error("Not logged in");
    const post = await ctx.db.posts.get(postId);
    if (!post) throw new Error("Post not found");
    return ctx.db.comments.insert({
      postId,
      authorId: ctx.request.auth.id,
      body,
    });
  },
);

// ── Pages ──

export const pages = route("/", {
  head: () => ({
    charset: "utf-8",
    viewport: "width=device-width, initial-scale=1",
    link: [
      { rel: "icon", href: "/assets/favicon.svg" },
      { rel: "stylesheet", href: "/assets/app.css" },
    ],
  }),
  component: () => import("./pages/layout.tsx"),
  children: [
    route("/", {
      head: async (ctx) => {
        const posts = await ctx.api.list_posts({});
        return {
          title: "My Blog",
          meta: [
            { name: "description", content: `A blog with ${posts.length} articles about software engineering` },
          ],
          og: { title: "My Blog", type: "website", image: "/assets/og-default.png" },
        };
      },
      component: () => import("./pages/home.tsx"),
    }),
    route("/post/:slug", {
      head: async (ctx) => {
        const post = await ctx.api.get_post({ slug: ctx.params.slug });
        if (!post) return { title: "Not Found - My Blog" };
        return {
          title: `${post.title} - My Blog`,
          meta: [
            { name: "description", content: post.excerpt || post.title },
            { name: "author", content: post.authorName },
          ],
          og: {
            title: post.title,
            description: post.excerpt || post.title,
            type: "article",
            image: post.coverImage?.url || "/assets/og-default.png",
          },
          script: [{
            type: "application/ld+json",
            content: JSON.stringify({
              "@context": "https://schema.org",
              "@type": "BlogPosting",
              "headline": post.title,
              "description": post.excerpt,
              "author": { "@type": "Person", "name": post.authorName },
              "datePublished": new Date(post.publishedAt).toISOString(),
            }),
          }],
        };
      },
      component: () => import("./pages/post.tsx"),
    }),
    route("/about", {
      head: () => ({
        title: "About - My Blog",
        meta: [{ name: "description", content: "About this blog and its author" }],
        og: { title: "About - My Blog", type: "website" },
      }),
      component: () => import("./pages/about.tsx"),
    }),
  ],
});
