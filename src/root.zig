//! `cache-zig`: sharded in-memory cache with TTL and size-based eviction.
//!
//! Key properties:
//!
//! - Sharded key/value storage for concurrent access.
//! - TTL stored per item (expired entries are treated as misses by default).
//! - Size-based eviction when over `Config.max_weight`.
//! - Compile-time eviction policy selection.
//! - Custom weigher types (no `anyopaque` in public APIs).
//!
//! # Example
//!
//! ```zig
//! const std = @import("std");
//! const cache_zig = @import("cache_zig");
//!
//! pub fn main() !void {
//!     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//!     defer _ = gpa.deinit();
//!     const alloc = gpa.allocator();
//!
//!     var cfg = cache_zig.Config{ .max_weight = 10_000 };
//!     var cache = try cache_zig.SampledLruCache(u64).init(alloc, cfg);
//!     defer cache.deinit();
//!
//!     // set/get return an ItemRef which must be deinit'd.
//!     var set_ref = try cache.set("k", 123, 60 * std.time.ns_per_s);
//!     defer set_ref.deinit();
//!
//!     var get_ref = cache.get("k") orelse return error.Miss;
//!     defer get_ref.deinit();
//!     try std.testing.expectEqual(@as(u64, 123), get_ref.value().*);
//! }
//! ```
//!
//! # ItemRef lifetime
//!
//! `Cache(V).ItemRef` is a reference-counted handle to the underlying item.
//! Always `defer item_ref.deinit()`.
//!
//! - `get`/`peek`/`set` return an `ItemRef` that does NOT remove the item from the cache.
//! - `delete` removes the key from the cache and returns an `ItemRef` for the removed item;
//!   when the last `ItemRef` is deinit'd, the item memory is freed.

const std = @import("std");

pub const Config = @import("config.zig").Config;
pub const weigher = @import("weigher.zig");

pub const EvictionPolicy = @import("cache.zig").EvictionPolicy;
pub const CacheUnmanaged = @import("cache.zig").CacheUnmanaged;
pub const Cache = @import("cache.zig").Cache;

pub const SampledLruCache = @import("cache.zig").SampledLruCache;
pub const SampledLruCacheWithWeigher = @import("cache.zig").SampledLruCacheWithWeigher;
pub const SampledLhdCache = @import("cache.zig").SampledLhdCache;
pub const StableLruCache = @import("cache.zig").StableLruCache;
pub const StableLruCacheWithWeigher = @import("cache.zig").StableLruCacheWithWeigher;
pub const StableLhdCache = @import("cache.zig").StableLhdCache;

test {
    _ = @import("tests.zig");
}
