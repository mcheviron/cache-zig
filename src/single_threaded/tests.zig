const std = @import("std");

const SampledLruCache = @import("cache.zig").SampledLruCache;
const SampledLruCacheWithWeigher = @import("cache.zig").SampledLruCacheWithWeigher;
const SampledLhdCache = @import("cache.zig").SampledLhdCache;
const StableLruCache = @import("cache.zig").StableLruCache;
const StableLruCacheWithWeigher = @import("cache.zig").StableLruCacheWithWeigher;
const StableLhdCache = @import("cache.zig").StableLhdCache;

const Config = @import("cache.zig").Config;

const Key = []const u8;

const Weigher10 = struct {
    pub fn weigh(_: @This(), _: Key, _: *const u64) usize {
        return 10;
    }
};

test "set/get/delete roundtrip" {
    const alloc = std.testing.allocator;
    var cache = try SampledLruCache(Key, u64).init(alloc, Config{ .max_weight = 10_000 });
    defer cache.deinit();

    var item = try cache.set("a", 123, 60 * std.time.ns_per_s);
    defer item.deinit();
    try std.testing.expectEqualStrings("a", item.key());
    try std.testing.expectEqual(@as(u64, 123), item.value().*);

    var got = (cache.get("a") orelse return error.Miss);
    defer got.deinit();
    try std.testing.expectEqual(@as(u64, 123), got.value().*);

    const borrowed = cache.getBorrowed("a") orelse return error.Miss;
    try std.testing.expectEqual(@as(u64, 123), borrowed.*);

    var deleted = (cache.delete("a") orelse return error.Miss);
    defer deleted.deinit();
    try std.testing.expectEqualStrings("a", deleted.key());

    if (cache.peek("a")) |it| {
        defer it.deinit();
        return error.ExpectedMiss;
    }
}

test "get missing returns null" {
    const alloc = std.testing.allocator;
    var cache = try SampledLruCache(Key, u8).init(alloc, Config{ .max_weight = 10_000 });
    defer cache.deinit();

    try std.testing.expect(cache.get("missing") == null);
    try std.testing.expect(cache.getBorrowed("missing") == null);
}

test "delete missing returns null" {
    const alloc = std.testing.allocator;
    var cache = try SampledLruCache(Key, u8).init(alloc, Config{ .max_weight = 10_000 });
    defer cache.deinit();

    try std.testing.expect(cache.delete("missing") == null);
}

test "replace preserves ttl" {
    const alloc = std.testing.allocator;
    var cache = try SampledLruCache(Key, u8).init(alloc, Config{ .max_weight = 10_000 });
    defer cache.deinit();

    try std.testing.expect((try cache.replace("missing", 1)) == null);

    {
        var item = try cache.set("a", 1, 5 * std.time.ns_per_s);
        defer item.deinit();
    }

    var before = (cache.get("a") orelse return error.Miss);
    defer before.deinit();
    const before_ttl = before.ttlNs();

    var replaced = (try cache.replace("a", 2)) orelse return error.Miss;
    defer replaced.deinit();

    var after = (cache.get("a") orelse return error.Miss);
    defer after.deinit();
    const after_ttl = after.ttlNs();

    try std.testing.expect(after_ttl <= before_ttl);
    try std.testing.expectEqual(@as(u8, 2), after.value().*);
}

test "extend adds to existing ttl" {
    const alloc = std.testing.allocator;
    var cache = try SampledLruCache(Key, u8).init(alloc, Config{ .max_weight = 10_000 });
    defer cache.deinit();

    {
        var item = try cache.set("a", 1, 2 * std.time.ns_per_s);
        defer item.deinit();
    }

    try std.testing.expect(cache.extend("a", 1 * std.time.ns_per_s));

    var got = cache.get("a") orelse return error.Miss;
    defer got.deinit();

    try std.testing.expect(got.ttlNs() > (2 * std.time.ns_per_s + 400 * std.time.ns_per_ms));
}

test "sampled LHD evicts low density" {
    const alloc = std.testing.allocator;
    var cache = try SampledLhdCache(Key, u64, Weigher10).init(alloc, Config{ .max_weight = 30, .sample_size = 8, .enable_tiny_lfu = false });
    defer cache.deinit();

    {
        var a = try cache.set("a", 1, 60 * std.time.ns_per_s);
        defer a.deinit();
        var b = try cache.set("b", 2, 60 * std.time.ns_per_s);
        defer b.deinit();
        var c = try cache.set("c", 3, 60 * std.time.ns_per_s);
        defer c.deinit();
    }

    // Give "b" extra hits so it should survive.
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        var b = cache.get("b") orelse return error.Miss;
        b.deinit();
    }

    // Add "d" to force eviction.
    {
        var d = try cache.set("d", 4, 60 * std.time.ns_per_s);
        defer d.deinit();
    }

    // At least one of a/c should be gone, b should likely remain.
    if (cache.peek("b")) |it| {
        it.deinit();
    } else {
        return error.Miss;
    }
}

test "stable LRU compiles and basic ops" {
    const alloc = std.testing.allocator;
    var cache = try StableLruCache(Key, u64).init(alloc, Config{ .max_weight = 18, .gets_per_promote = 2, .enable_tiny_lfu = false });
    defer cache.deinit();

    {
        var a = try cache.set("a", 1, 60 * std.time.ns_per_s);
        defer a.deinit();
        var b = try cache.set("b", 2, 60 * std.time.ns_per_s);
        defer b.deinit();
    }

    // Touch a so b is more likely LRU.
    {
        var a = cache.get("a") orelse return error.Miss;
        defer a.deinit();
    }

    {
        var c = try cache.set("c", 3, 60 * std.time.ns_per_s);
        defer c.deinit();
        try std.testing.expectEqual(.inserted, c.status);
        try std.testing.expect(c.item != null);
    }

    try std.testing.expect(cache.len() <= 2);
}

test "stable LHD compiles and basic ops" {
    const alloc = std.testing.allocator;
    var cache = try StableLhdCache(Key, u64, Weigher10).init(alloc, Config{ .max_weight = 20, .sample_size = 8 });
    defer cache.deinit();

    {
        var a = try cache.set("a", 1, 60 * std.time.ns_per_s);
        defer a.deinit();
        var b = try cache.set("b", 2, 60 * std.time.ns_per_s);
        defer b.deinit();
        var c = try cache.set("c", 3, 60 * std.time.ns_per_s);
        defer c.deinit();
    }

    {
        var d = try cache.set("d", 4, 60 * std.time.ns_per_s);
        defer d.deinit();
    }

    try std.testing.expect(cache.len() <= 2);
}

test "tiny lfu rejects cold candidate" {
    const alloc = std.testing.allocator;
    var cache = try SampledLruCacheWithWeigher(Key, u64, Weigher10).init(alloc, Config{ .max_weight = 20, .sample_size = 8 });
    defer cache.deinit();

    {
        var a = try cache.set("a", 1, 60 * std.time.ns_per_s);
        defer a.deinit();
        var b = try cache.set("b", 2, 60 * std.time.ns_per_s);
        defer b.deinit();
    }

    // Touch a so b becomes the deterministic LRU victim.
    {
        var a = cache.get("a") orelse return error.Miss;
        a.deinit();
    }

    {
        var c = try cache.set("c", 3, 60 * std.time.ns_per_s);
        defer c.deinit();
        try std.testing.expectEqual(.rejected, c.status);
        try std.testing.expect(c.item == null);
    }

    if (cache.peek("a")) |it| {
        it.deinit();
    } else {
        return error.Miss;
    }

    if (cache.peek("b")) |it| {
        it.deinit();
    } else {
        return error.Miss;
    }

    try std.testing.expect(cache.peek("c") == null);
}

test "stable LRU SLRU evicts probation first" {
    const alloc = std.testing.allocator;

    var cache = try StableLruCacheWithWeigher(Key, u64, Weigher10).init(
        alloc,
        Config{ .max_weight = 30, .gets_per_promote = 1, .stable_lru_window_percent = 0, .stable_lru_protected_percent = 50, .enable_tiny_lfu = false },
    );
    defer cache.deinit();

    {
        var a = try cache.set("a", 1, 60 * std.time.ns_per_s);
        defer a.deinit();
        var b = try cache.set("b", 2, 60 * std.time.ns_per_s);
        defer b.deinit();
        var c = try cache.set("c", 3, 60 * std.time.ns_per_s);
        defer c.deinit();
    }

    // Move a and b to protected; protected cap (15) forces demotion of a.
    {
        var a = cache.get("a") orelse return error.Miss;
        a.deinit();
        var b = cache.get("b") orelse return error.Miss;
        b.deinit();
    }

    {
        var d = try cache.set("d", 4, 60 * std.time.ns_per_s);
        defer d.deinit();
    }

    // Eviction should come from probation first (c).
    try std.testing.expect(cache.peek("c") == null);

    {
        var a = cache.peek("a") orelse return error.Miss;
        defer a.deinit();
    }
    {
        var b = cache.peek("b") orelse return error.Miss;
        defer b.deinit();
    }
    {
        var d = cache.peek("d") orelse return error.Miss;
        defer d.deinit();
    }
}

test "stable LRU window admits probation" {
    const alloc = std.testing.allocator;

    var cache = try StableLruCacheWithWeigher(Key, u64, Weigher10).init(
        alloc,
        Config{ .max_weight = 20, .stable_lru_window_percent = 50, .stable_lru_protected_percent = 0, .enable_tiny_lfu = false },
    );
    defer cache.deinit();

    {
        var a = try cache.set("a", 1, 60 * std.time.ns_per_s);
        defer a.deinit();
        var b = try cache.set("b", 2, 60 * std.time.ns_per_s);
        defer b.deinit();
        var c = try cache.set("c", 3, 60 * std.time.ns_per_s);
        defer c.deinit();
    }

    try std.testing.expect(cache.peek("a") == null);

    if (cache.peek("b")) |it| {
        it.deinit();
    } else {
        return error.Miss;
    }

    if (cache.peek("c")) |it| {
        it.deinit();
    } else {
        return error.Miss;
    }
}

test "tiny lfu admits frequent candidate" {
    const alloc = std.testing.allocator;
    var cache = try SampledLruCacheWithWeigher(Key, u64, Weigher10).init(alloc, Config{ .max_weight = 20, .sample_size = 8 });
    defer cache.deinit();

    {
        var a = try cache.set("a", 1, 60 * std.time.ns_per_s);
        defer a.deinit();
        var b = try cache.set("b", 2, 60 * std.time.ns_per_s);
        defer b.deinit();
    }

    // Touch a so b becomes the deterministic LRU victim.
    {
        var a = cache.get("a") orelse return error.Miss;
        a.deinit();
    }

    // Warm the candidate key in the sketch.
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        _ = cache.get("c");
    }

    {
        var c = try cache.set("c", 3, 60 * std.time.ns_per_s);
        defer c.deinit();
    }

    if (cache.peek("a")) |it| {
        it.deinit();
    } else {
        return error.Miss;
    }

    try std.testing.expect(cache.peek("b") == null);

    if (cache.peek("c")) |it| {
        it.deinit();
    } else {
        return error.Miss;
    }
}

test "snapshot OOM releases retained refs" {
    const alloc = std.testing.allocator;
    var cache = try SampledLruCache(Key, u8).init(alloc, Config{ .max_weight = 10_000 });
    defer cache.deinit();

    {
        var a = try cache.set("a", 1, 60 * std.time.ns_per_s);
        defer a.deinit();
    }

    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, cache.snapshot(failing.allocator()));
}

test "filter OOM releases retained refs" {
    const alloc = std.testing.allocator;
    var cache = try SampledLruCache(Key, u8).init(alloc, Config{ .max_weight = 10_000 });
    defer cache.deinit();

    {
        var a = try cache.set("a", 1, 60 * std.time.ns_per_s);
        defer a.deinit();
    }

    const PredCtx = struct {
        pub fn pred(_: @This(), _: []const u8, _: *const u8) bool {
            return true;
        }
    };

    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, cache.filter(failing.allocator(), PredCtx, .{}));
}
