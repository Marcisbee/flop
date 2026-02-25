import { flop, table, t } from "flop";

const users = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{15}/),
    email: t.string().required().unique(),
    password: t.bcrypt(10).required(),
    name: t.string(),
    roles: t.roles(),
  },
  auth: true,
});

const messages = table({
  schema: {
    id: t.string().autogenerate(/[a-z0-9]{15}/),
    text: t.string().required(),
    author: t.refSingle(users, "id").required(),
    createdAt: t.timestamp().default("now"),
  },
});

export const db = flop({ users, messages });

export const send_message = db.reduce(
  { text: t.string() },
  (ctx, { text }) => {
    if (!ctx.request.auth) {
      throw new Error("Not logged in")
    }

    return ctx.db.messages.insert({ text, author: ctx.request.auth.id });
  },
);

export const get_messages = db.view(
  {},
  (ctx) => {
    return ctx.db.messages.scan(100);
  },
).public();
