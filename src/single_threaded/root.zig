pub const Policy = @import("../policy.zig").Policy;

pub const weigher = @import("../weigher.zig");
const CacheMod = @import("cache.zig");

pub const Config = CacheMod.Config;

pub fn CacheUnmanaged(comptime V: type, comptime policy: Policy, comptime Weigher: type) type {
    return CacheMod.CacheUnmanaged(V, policy, Weigher);
}

pub fn Cache(comptime V: type, comptime policy: Policy, comptime Weigher: type) type {
    return CacheMod.Cache(V, policy, Weigher);
}

pub fn SampledLruCache(comptime V: type) type {
    return CacheMod.SampledLruCache(V);
}

pub fn SampledLruCacheWithWeigher(comptime V: type, comptime Weigher: type) type {
    return CacheMod.SampledLruCacheWithWeigher(V, Weigher);
}

pub fn SampledLhdCache(comptime V: type, comptime Weigher: type) type {
    return CacheMod.SampledLhdCache(V, Weigher);
}

pub fn StableLruCache(comptime V: type) type {
    return CacheMod.StableLruCache(V);
}

pub fn StableLruCacheWithWeigher(comptime V: type, comptime Weigher: type) type {
    return CacheMod.StableLruCacheWithWeigher(V, Weigher);
}

pub fn StableLhdCache(comptime V: type, comptime Weigher: type) type {
    return CacheMod.StableLhdCache(V, Weigher);
}
