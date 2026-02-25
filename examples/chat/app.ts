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

const rooms = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{8}/),
    name: t.string().required(),
    createdBy: t.string().required(),
    createdAt: t.timestamp().default("now"),
  },
});

const messages = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{12}/),
    roomId: t.string().required(),
    authorId: t.string().required(),
    authorName: t.string().required(),
    text: t.string().required(),
    createdAt: t.timestamp().default("now"),
  },
});

// ── Database ──

export const db = flop({ users, rooms, messages }, {
  dataDir: `${import.meta.dirname}/data`,
});

// ── Reducers ──

export const create_room = db.reduce(
  { name: t.string() },
  async (ctx, { name }) => {
    if (!ctx.request.auth) throw new Error("Not logged in");
    return ctx.db.rooms.insert({
      name,
      createdBy: ctx.request.auth.id,
    });
  },
);

export const send_message = db.reduce(
  { roomId: t.string(), text: t.string() },
  async (ctx, { roomId, text }) => {
    if (!ctx.request.auth) throw new Error("Not logged in");
    const room = await ctx.db.rooms.get(roomId);
    if (!room) throw new Error("Room not found");
    const user = await ctx.db.users.get(ctx.request.auth.id);
    return ctx.db.messages.insert({
      roomId,
      authorId: ctx.request.auth.id,
      authorName: (user as any)?.name ?? "Anonymous",
      text,
    });
  },
);

// ── Views ──

export const get_rooms = db.view(
  {},
  async (ctx) => {
    return ctx.db.rooms.scan(1000);
  },
).public();

export const get_messages = db.view(
  { roomId: t.string() },
  async (ctx, { roomId }) => {
    const all = await ctx.db.messages.scan(10000);
    return (all as any[])
      .filter((m) => m.roomId === roomId)
      .sort((a, b) => a.createdAt - b.createdAt)
      .slice(-200);
  },
).public();
