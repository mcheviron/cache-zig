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

pub fn CacheUnmanaged(comptime K: type, comptime V: type, comptime policy: Policy, comptime Weigher: type) type {
    guard();
    return CacheImpl.CacheUnmanaged(K, V, policy, Weigher);
}

pub fn Cache(comptime K: type, comptime V: type, comptime policy: Policy, comptime Weigher: type) type {
    guard();
    return CacheImpl.Cache(K, V, policy, Weigher);
}

pub fn SampledLruCache(comptime K: type, comptime V: type) type {
    guard();
    return CacheImpl.SampledLruCache(K, V);
}

pub fn SampledLruCacheWithWeigher(comptime K: type, comptime V: type, comptime Weigher: type) type {
    guard();
    return CacheImpl.SampledLruCacheWithWeigher(K, V, Weigher);
}

pub fn SampledLhdCache(comptime K: type, comptime V: type, comptime Weigher: type) type {
    guard();
    return CacheImpl.SampledLhdCache(K, V, Weigher);
}

pub fn StableLruCache(comptime K: type, comptime V: type) type {
    guard();
    return CacheImpl.StableLruCache(K, V);
}

pub fn StableLruCacheWithWeigher(comptime K: type, comptime V: type, comptime Weigher: type) type {
    guard();
    return CacheImpl.StableLruCacheWithWeigher(K, V, Weigher);
}

pub fn StableLhdCache(comptime K: type, comptime V: type, comptime Weigher: type) type {
    guard();
    return CacheImpl.StableLhdCache(K, V, Weigher);
}
