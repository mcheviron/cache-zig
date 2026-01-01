# Agent Guidelines for `cache-zig/`

This directory is the Zig implementation of the cache.

- Module: `cache_zig`
- `zig build test` runs unit tests from `src/root.zig`.

## Commands (use `Justfile`)

- List tasks: `just help`
- Format: `just fmt` (`zig fmt build.zig src`)
- Build/check: `just check` (`zig build`)
- Test all: `just test` (`zig build test`)

### Run a single test

- `just test-one "evicts least hit dense"`
- Direct: `zig test src/root.zig --test-filter "evicts least hit dense"`

## Code layout

- `src/config.zig`: `Config` and options
- `src/cache.zig`: main cache implementation + eviction
- `src/item.zig`: item storage + TTL + atomic metadata + ref counting
- `src/shard.zig`: sharded map storage
- `src/tests.zig`: unit tests (imported by `src/root.zig`)

## Style

### Formatting

- Run `just fmt` before finalizing; do not hand-format.

### Naming

- Public types/functions: `UpperCamelCase` / `lowerCamelCase` per Zig norms.

### Public API types

- Avoid `anytype` in public APIs.
- Use explicit vtable-style interfaces (e.g., weigher/predicate `{ ctx, callFn }`).

### ItemRef lifetime (important)

- `Cache(V).ItemRef` is a ref-counted handle.
- Always `defer ref.deinit()` for values returned from `get`/`peek`/`set`/`delete`.
- When you retain an `Item` manually, ensure a matching release occurs.

### Error handling

- Prefer returning error unions for fallible operations.
- Avoid `@panic` in library code.

### Concurrency

- Keep shard lock hold-times minimal.
- Don’t call user-provided callbacks while holding shard locks.

## Tests

- Add tests for semantic changes (TTL, eviction policy, weight behavior).
- Prefer deterministic tests; avoid long sleeps.
