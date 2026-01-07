const builtin = @import("builtin");

pub const Config = @import("../config.zig").Config;
pub const Policy = @import("../policy.zig").Policy;

pub const weigher = @import("../weigher.zig");
const CacheImpl = @import("cache.zig");

fn guard() void {
    if (builtin.single_threaded) {
        @compileError("cache_zig.multi_threaded cannot be used when compiling with -fsingle-threaded; use cache_zig.single_threaded instead");
    }
}

pub fn CacheUnmanaged(comptime V: type, comptime policy: Policy, comptime Weigher: type) type {
    guard();
    return CacheImpl.CacheUnmanaged(V, policy, Weigher);
}

pub fn Cache(comptime V: type, comptime policy: Policy, comptime Weigher: type) type {
    guard();
    return CacheImpl.Cache(V, policy, Weigher);
}

pub fn SampledLruCache(comptime V: type) type {
    guard();
    return CacheImpl.SampledLruCache(V);
}

pub fn SampledLruCacheWithWeigher(comptime V: type, comptime Weigher: type) type {
    guard();
    return CacheImpl.SampledLruCacheWithWeigher(V, Weigher);
}

pub fn SampledLhdCache(comptime V: type, comptime Weigher: type) type {
    guard();
    return CacheImpl.SampledLhdCache(V, Weigher);
}

pub fn StableLruCache(comptime V: type) type {
    guard();
    return CacheImpl.StableLruCache(V);
}

pub fn StableLruCacheWithWeigher(comptime V: type, comptime Weigher: type) type {
    guard();
    return CacheImpl.StableLruCacheWithWeigher(V, Weigher);
}

pub fn StableLhdCache(comptime V: type, comptime Weigher: type) type {
    guard();
    return CacheImpl.StableLhdCache(V, Weigher);
}
