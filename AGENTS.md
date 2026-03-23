# Agent Instructions

## Kanban
Default kanban board path:
- `/Users/marcisbee/Library/Mobile Documents/iCloud~md~obsidian/Documents/test/🔮 Private/Flop db/🏕️ KANBAN.md`

When a user asks to complete tasks from kanban, use this file by default unless they provide a different kanban path.

Kanban completion workflow:
- Move tasks being worked on to `In progress`.
- When agent work is finished, move completed tasks to `Done`.
- Keep completed cards as checked items (`- [x]`).
- Never move tasks to `Archive`; only the user archives tasks.
- NEVER EVER remove or overwrite the `**Complete**` text line under `## Done` line (required by Obsidian kanban completion behavior).
