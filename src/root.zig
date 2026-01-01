//! `cache-zig`: sharded in-memory cache with TTL and LRU-ish eviction.
//!
//! This is a Zig port of the `cache-rs` implementation:
//! - Sharded key/value storage for concurrency.
//! - Single worker thread maintaining global LRU ordering and evicting by weight.
//! - TTL stored per item (expiration is not automatically enforced on `get`).
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
//!     var cache = try cache_zig.Cache(u64).init(alloc, cache_zig.Config{});
//!     defer cache.deinit();
//!
//!     _ = try cache.set("k", 123, 60 * std.time.ns_per_s);
//!     var item = (cache.get("k") orelse return error.Miss);
//!     defer item.deinit();
//!     try std.testing.expectEqual(@as(u64, 123), item.value().*);
//! }
//! ```

const std = @import("std");

pub const Config = @import("config.zig").Config;
pub const Cache = @import("cache.zig").Cache;

test {
    _ = @import("tests.zig");
}
