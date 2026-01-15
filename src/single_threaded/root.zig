pub const Policy = @import("../policy.zig").Policy;

pub const weigher = @import("../weigher.zig");
const CacheMod = @import("cache.zig");

pub const Config = CacheMod.Config;

pub fn CacheUnmanaged(comptime K: type, comptime V: type, comptime policy: Policy, comptime Weigher: type) type {
    return CacheMod.CacheUnmanaged(K, V, policy, Weigher);
}

pub fn Cache(comptime K: type, comptime V: type, comptime policy: Policy, comptime Weigher: type) type {
    return CacheMod.Cache(K, V, policy, Weigher);
}

pub fn SampledLruCache(comptime K: type, comptime V: type) type {
    return CacheMod.SampledLruCache(K, V);
}

pub fn SampledLruCacheWithWeigher(comptime K: type, comptime V: type, comptime Weigher: type) type {
    return CacheMod.SampledLruCacheWithWeigher(K, V, Weigher);
}

pub fn SampledLhdCache(comptime K: type, comptime V: type, comptime Weigher: type) type {
    return CacheMod.SampledLhdCache(K, V, Weigher);
}

pub fn StableLruCache(comptime K: type, comptime V: type) type {
    return CacheMod.StableLruCache(K, V);
}

pub fn StableLruCacheWithWeigher(comptime K: type, comptime V: type, comptime Weigher: type) type {
    return CacheMod.StableLruCacheWithWeigher(K, V, Weigher);
}

pub fn StableLhdCache(comptime K: type, comptime V: type, comptime Weigher: type) type {
    return CacheMod.StableLhdCache(K, V, Weigher);
}
