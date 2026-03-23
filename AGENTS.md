# Agent Instructions

This repository is the Flop engine itself.

Primary Go module location:
- repository root

## Core Values
- Preserve behavior and public API shape unless the user explicitly asks for a change.
- Prefer correctness and parity with existing Flop semantics over shortcuts.
- Do not add in-process caches, global memoization layers, request-result caches, or hidden background memory stores as a substitute for fixing real engine or query problems.
- Avoid memory-heavy workarounds that hide underlying performance problems.
- If runtime behavior is uncertain, verify against the current Flop implementation before changing it.

## Performance Rules
- Performance work must preserve behavior and payload shape unless the user explicitly approves a contract change.
- Prefer fixing hot paths with indexes, narrower indexed lookups, smaller result sets, better query shaping, schema/index improvements, and materialized/precomputed database data.
- It is allowed to optimize Flop internals and table/index behavior when that improves real performance and does not break correctness.
- It is allowed to add or improve materialized tables, projections, and refresh flows when they replace expensive per-request recomputation.
- Measure first when possible. Use traces, index scan counts, payload size, benchmarks, and repeated lookup patterns to identify the real bottleneck before editing.

### Hard No
- Do not add in-process caches, global memoization, request-result caches, or background memory stores in the Go runtime.
- Do not “fix” slow paths by loading more rows into memory and filtering later.
- Do not hide performance problems with oversized fallback scans, duplicated projections, or permanently growing in-memory structures.
- Do not change user-visible behavior, ranking rules, date boundaries, active-state semantics, or endpoint contracts just to make a path cheaper.

### Preferred Optimization Order
- First: verify the exact slow path with traces, benchmarks, or query/index inspection.
- Second: reduce scanned rows and repeated lookups.
- Third: make indexed access paths explicit and reuse existing indexed tables/materialized data.
- Fourth: add schema/index/materialized-table improvements if the request is still too expensive.
- Last: consider larger engine-level changes only when the bottleneck is clearly inside Flop itself.

## Kanban
Default kanban board path:
- `/Users/marcisbee/Library/Mobile Documents/iCloud~md~obsidian/Documents/test/🔮 Private/Flop db/🏕️ KANBAN.md`

When a user asks to complete tasks from kanban, use this file by default unless they provide a different kanban path.
Always re-read the kanban file immediately before making any kanban edits, because the user may have updated it since your last read.

Kanban completion workflow:
- Move tasks being worked on to `In progress`.
- When agent work is finished, move completed tasks to `Done`.
- Keep completed cards as checked items (`- [x]`).
- Never move tasks to `Archive`; only the user archives tasks.
- Never remove or overwrite the `**Complete**` line under `## Done` (required by Obsidian kanban completion behavior).
