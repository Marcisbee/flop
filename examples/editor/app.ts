import { flop, table, t } from "flop";

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

const documents = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{8}/),
    title: t.string().required(),
    content: t.string().required(),
    createdBy: t.string().required(),
    updatedAt: t.timestamp().default("now"),
    createdAt: t.timestamp().default("now"),
  },
});

const cursors = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{12}/),
    docId: t.string().required(),
    userId: t.string().required(),
    userName: t.string().required(),
    position: t.integer().required(),
    color: t.string().required(),
    updatedAt: t.timestamp().default("now"),
  },
});

// ── Database ──

export const db = flop({ users, documents, cursors }, {
  dataDir: `${import.meta.dirname}/data`,
});

// ── Reducers ──

export const create_doc = db.reduce(
  { title: t.string() },
  async (ctx, { title }) => {
    if (!ctx.request.auth) throw new Error("Not logged in");
    return ctx.db.documents.insert({
      title: title || "Untitled",
      content: "",
      createdBy: ctx.request.auth.id,
    });
  },
);

export const update_doc = db.reduce(
  { docId: t.string(), content: t.string() },
  async (ctx, { docId, content }) => {
    if (!ctx.request.auth) throw new Error("Not logged in");
    const doc = await ctx.db.documents.get(docId);
    if (!doc) throw new Error("Document not found");
    return ctx.db.documents.update(docId, { content, updatedAt: Date.now() });
  },
);

export const rename_doc = db.reduce(
  { docId: t.string(), title: t.string() },
  async (ctx, { docId, title }) => {
    if (!ctx.request.auth) throw new Error("Not logged in");
    const doc = await ctx.db.documents.get(docId);
    if (!doc) throw new Error("Document not found");
    return ctx.db.documents.update(docId, { title, updatedAt: Date.now() });
  },
);

export const update_cursor = db.reduce(
  { docId: t.string(), position: t.integer(), color: t.string() },
  async (ctx, { docId, position, color }) => {
    if (!ctx.request.auth) throw new Error("Not logged in");
    const user = await ctx.db.users.get(ctx.request.auth.id);
    const userName = (user as any)?.name ?? "Anonymous";
    // Upsert: find existing cursor for this user+doc or create
    const all = await ctx.db.cursors.scan(10000);
    const existing = (all as any[]).find(
      (c) => c.docId === docId && c.userId === ctx.request.auth!.id,
    );
    if (existing) {
      return ctx.db.cursors.update(existing.id, { position, color, updatedAt: Date.now() });
    }
    return ctx.db.cursors.insert({
      docId,
      userId: ctx.request.auth.id,
      userName,
      position,
      color,
    });
  },
);

// ── Views ──

export const get_docs = db.view(
  {},
  async (ctx) => {
    const all = await ctx.db.documents.scan(1000);
    return (all as any[])
      .map(({ id, title, createdBy, updatedAt, createdAt }) => ({
        id, title, createdBy, updatedAt, createdAt,
      }))
      .sort((a, b) => b.updatedAt - a.updatedAt);
  },
).public();

export const get_doc = db.view(
  { docId: t.string() },
  async (ctx, { docId }) => {
    const doc = await ctx.db.documents.get(docId);
    if (!doc) throw new Error("Document not found");
    // Include cursors for this document (exclude stale > 30s)
    const allCursors = await ctx.db.cursors.scan(10000);
    const now = Date.now();
    const activeCursors = (allCursors as any[]).filter(
      (c) => c.docId === docId && (now - c.updatedAt) < 30000,
    );
    return { ...(doc as any), cursors: activeCursors };
  },
).public();
