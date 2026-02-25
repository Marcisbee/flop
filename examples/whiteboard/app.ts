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

const strokes = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{12}/),
    boardId: t.string().required(),
    authorId: t.string().required(),
    authorName: t.string().required(),
    points: t.json<number[][]>().required(),
    color: t.string().required(),
    width: t.number().required(),
    createdAt: t.timestamp().default("now"),
  },
});

// ── Database ──

export const db = flop({ users, strokes }, {
  dataDir: `${import.meta.dirname}/data`,
});

// ── Reducers ──

export const add_stroke = db.reduce(
  {
    boardId: t.string(),
    points: t.json<number[][]>(),
    color: t.string(),
    width: t.number(),
  },
  async (ctx, { boardId, points, color, width }) => {
    if (!ctx.request.auth) throw new Error("Not logged in");
    const user = await ctx.db.users.get(ctx.request.auth.id);
    return ctx.db.strokes.insert({
      boardId: boardId || "default",
      authorId: ctx.request.auth.id,
      authorName: (user as any)?.name ?? "Anonymous",
      points,
      color,
      width,
    });
  },
);

export const clear_board = db.reduce(
  { boardId: t.string() },
  async (ctx, { boardId }) => {
    if (!ctx.request.auth) throw new Error("Not logged in");
    const all = await ctx.db.strokes.scan(100000);
    const bid = boardId || "default";
    let count = 0;
    for (const s of all as any[]) {
      if (s.boardId === bid) {
        await ctx.db.strokes.delete(s.id);
        count++;
      }
    }
    return { cleared: count };
  },
);

// ── Views ──

export const get_strokes = db.view(
  { boardId: t.string() },
  async (ctx, { boardId }) => {
    const all = await ctx.db.strokes.scan(100000);
    const bid = boardId || "default";
    return (all as any[])
      .filter((s) => s.boardId === bid)
      .sort((a, b) => a.createdAt - b.createdAt);
  },
).public();
