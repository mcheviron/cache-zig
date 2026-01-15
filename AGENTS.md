# Agent Guidelines for `cache-zig/`

These instructions are for agentic coding assistants working in this repository.

## Project

- Language: Zig
- Package/module: `cache_zig` (see `build.zig`)
- Root module: `src/root.zig`
- Minimum Zig: `0.15.2` (see `build.zig.zon`)

## Commands

Prefer the `Justfile` (it defines the intended workflows).

- List tasks: `just -l` (or `just help`)
- Verify Zig install/version: `just install` (runs `zig version`)
- Format: `just fmt` (runs `zig fmt build.zig src`)
- Build: `just build [-- <zig args...>]` (runs `zig build`)
- Test: `just test [-- <zig args...>]` (runs `zig build test`)
- Run a single test: `just test-one "<filter>" [-- <zig test args...>]`
  - Direct: `zig test src/root.zig --test-filter "<filter>"`

- View Zig standard library documentation locally:
  - Start the documentation server by running: `just std`
    - **Note:** Run this in a PTY (such as a terminal, not a background process) to ensure the documentation server starts and can accept interactive input if needed.
  - This will print the address (e.g. `http://127.0.0.1:5000/`). Open this URL in your browser to view and search the standard library documentation.

Notes:

- All recipes accept extra Zig args, e.g. `just build -Doptimize=ReleaseSafe`.
- No separate linter; rely on `zig fmt` + `just build`/`just test`.

## Code layout

- `src/root.zig`: public API + docs; imports `src/tests.zig`
- `src/cache.zig`: cache implementation + eviction policies + worker
- `src/shard.zig`: sharded storage (`RwLock` + maps + item list)
- `src/item.zig`: heap item, TTL, atomics, ref-counting
- `src/config.zig`: `Config` + validation (`Config.build()`)
- `src/weigher.zig`: built-in weighers
- `src/tests.zig`: unit tests

## Zig style guide

Follow existing patterns in `src/*.zig` and prefer small, reviewable changes.

### Formatting

- Run `just fmt` before finalizing; do not hand-format.

### Imports

- Put `const std = @import("std");` at the top.
- Put local imports next, grouped as `const X = @import("file.zig").X;`.
- Optionally alias common types like `const Allocator = std.mem.Allocator;`.
- Prefer explicit imports; avoid `usingnamespace`.

### Naming

- Public types: `UpperCamelCase`.
- Public functions/methods: `lowerCamelCase`.
- Struct fields and locals: `snake_case`.
- Keep abbreviations consistent (`ttl_ns`, `cfg`, `alloc`).
- Prefer `const` locals; use `var` only for mutation.
- In structs, use `const Self = @This();` for readability.

### Documentation

- Use `//!` for file/module docs (see `src/root.zig`).
- Use `///` for public decl docs; keep examples compiling.

### Types, slices, and casts

- Keys are `[]const u8` everywhere; only `Item.key` owns heap `[]u8`.
- For slice/string equality, use `std.mem.eql(u8, a, b)`.
- TTL/time is nanoseconds (`ttl_ns: u64`); use `std.time.ns_per_*` constants.
- Prefer `usize` for sizes/weights and `u64` for ticks/counters.
- Zig passes structs by value; avoid accidental large copies.
- Use explicit casts (`@as`, `@intCast`) and saturating/clamping ops (`-|`).

### Error handling

- Prefer narrow error sets and `Error!T`; propagate with `try`.
- Use `catch` only when handling (avoid `anyerror` in public APIs).
- Use `defer`/`errdefer` to keep cleanup next to allocations/inits.
- Model misses as optionals (`?T`); avoid forced unwrap (`.?`), prefer `orelse return`.
- Avoid `@panic` in library code; use `@compileError` for invalid comptime inputs.
- Use `unreachable` only for true internal invariants.

### Control flow and scoping

- Prefer optional capture: `if (opt) |v| { ... } else { ... }`.
- Prefer error-union capture: `if (foo()) |v| { ... } else |err| { ... }`.
- Handle specific errors with `switch (err) { ... else => return err }`.
- Use labeled blocks (`blk: { ... break :blk value; }`) for early-exit with a value.
- In loops, wrap per-iteration resources in a block so `defer` runs each iteration.

### Allocation and containers

- Keep `std.mem.Allocator` explicit for any allocating API.
- Use `allocator.create/destroy` for single items; `allocator.alloc/free` for slices.
- Pair allocations with `errdefer` close to the allocation site.
- Prefer stack buffers (`std.fmt.bufPrint`) or `std.BoundedArray` when max size is known.
- Prefer `std.*Unmanaged` containers; pass allocator to mutation ops (`try list.append(allocator, x)`).
- Initialize containers with `.{} / .empty` and call `deinit(allocator)` consistently.

### Hash maps and string keys (important)

- `std.StringHashMapUnmanaged` stores key slices as-is: key memory must outlive the map entry.
- When replacing items whose key allocation changes, remove + reinsert to avoid dangling keys (see `src/shard.zig`).
- For maps that own keys:
  - `dupe` only on insert (via `getOrPut`), write `gop.key_ptr.*`, and free keys on `deinit` via `keyIterator()`.

### Memory, ownership, and lifetimes (critical)

- `Cache(V).ItemRef` is ref-counted; always `defer ref.deinit()` for refs returned by cache ops.
- If you call `retain()`, ensure a matching `release()`.
- Don’t let pointers to stack locals escape; allocate or require caller-owned storage.
- Keep pointers returned by map `getPtr`/`getOrPut` scoped; use `get` to copy values that must outlive mutations.

### Public API design

- Avoid `anytype` / `anyopaque` in public APIs.
- Prefer comptime validation via `@compileError`.
- Preserve `CacheUnmanaged` (no allocator) vs `Cache` (stores allocator) separation.

### Type layout conventions

In all cache-owned/returned types in both `multi_threaded` and `single_threaded` families:

- Put all **fields** first, at the top of the type.
- Place any `const` declarations (such as constants needed by methods) immediately after the fields.
- Put all **public functions** immediately after the fields and consts.
- Put all **private functions** at the very bottom of the type.

### Concurrency

- Shards use `std.Thread.RwLock`; lock then immediately `defer unlock`; keep hold-times minimal.
- Prefer shared locks for read-only paths (`lockShared`).
- Avoid locking multiple shards simultaneously.
- Don’t call user callbacks while holding shard locks.

### Atomics

- Follow existing memory order choices (`.acquire`, `.release`, `.acq_rel`, `.monotonic`).

### Debugging and logging

- Avoid `std.debug.print` in library code; keep output in tests/examples only.
- Prefer `std.log` for structured diagnostics; keep it opt-in.
- Use `std.debug.assert` for debug-only invariants (not user-visible errors).

## Tests

- Add/update tests in `src/tests.zig` for behavior changes (use `std.testing.expect*`).
- Keep tests deterministic; avoid long sleeps; prefer `ttl_ns = 0`/small TTLs.
- Use `std.testing.allocator` to catch leaks.
- Always deinit `ItemRef`s and free lists returned from `snapshot`/`filter`.

## References

- To run and browse local stdlib documentation: `zig std` (see CLI output for the web address)
- https://www.openmymind.net/
