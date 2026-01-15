//! `cache-zig`: in-memory cache with TTL and size-based eviction.
//!
//! This package exposes two explicit families:
//!
//! - `cache_zig.multi_threaded.*`: sharded + concurrent implementation.
//! - `cache_zig.single_threaded.*`: single-threaded implementation.
//!
//! `cache_zig.multi_threaded.*` is unavailable under `-fsingle-threaded`.
//!
//! ## Keys
//! All caches are generic over key and value types: `Cache(K, V, ...)`.
//!
//! By default, the cache *owns keys*: `set(key, ...)` clones the key and stores
//! it inside the cache entry. Keys are freed automatically when the entry is
//! deleted/evicted (or when the cache is deinitialized).
//!
//! **No manual key free is required.** The only required cleanup is calling
//! `ItemRef.deinit()` on any `ItemRef` returned from cache operations.
//!
//! ### Acceptable key types (default)
//! The default key ownership + hashing/equality implementations support:
//! - Plain value keys: integers, enums, bools, etc.
//! - Slices: e.g. `[]const u8` (cloned and compared/hashed by content).
//! - Arrays, optionals, structs, and tagged unions composed of supported types.
//!
//! The defaults reject:
//! - Raw pointer keys (`*T`, `[*]T`, `?*T`, etc) because ownership and deep
//!   hashing are ambiguous.
//! - Untagged unions.
//!
//! If you need these, provide custom key ownership and/or hash/eql behavior.
//!
//! ### Returned keys and lifetimes
//! `ItemRef.key()` returns the stored key value.
//!
//! Keys are treated as **logically immutable** once inserted.
//! - For value keys (e.g. `u64`), `ItemRef.key()` returns a copy.
//! - For slice-like keys (e.g. `[]const u8`), the returned slice points to
//!   memory owned by the cache entry and is only valid while you keep the
//!   `ItemRef` alive.
//!
//! Do not mutate returned keys via `@constCast` or other unsafe tricks.
//! Mutating a key would break hashing/equality invariants and is unsupported.

const builtin = @import("builtin");

pub const Config = @import("config.zig").Config;
pub const Policy = @import("policy.zig").Policy;
pub const weigher = @import("weigher.zig");

pub const multi_threaded = @import("multi_threaded/root.zig");
pub const single_threaded = @import("single_threaded/root.zig");

test {
    if (builtin.single_threaded) {
        _ = @import("single_threaded/tests.zig");
    } else {
        _ = @import("single_threaded/tests.zig");
        _ = @import("multi_threaded/tests.zig");
    }
}
