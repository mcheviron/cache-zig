//! `cache-zig`: in-memory cache with TTL and size-based eviction.
//!
//! This package exposes two explicit families:
//!
//! - `cache_zig.multi_threaded.*`: sharded + concurrent implementation.
//! - `cache_zig.single_threaded.*`: single-threaded implementation.
//!
//! `cache_zig.multi_threaded.*` is unavailable under `-fsingle-threaded`.

const builtin = @import("builtin");

pub const Config = @import("config.zig").Config;
pub const EvictionPolicy = @import("eviction_policy.zig").EvictionPolicy;
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
