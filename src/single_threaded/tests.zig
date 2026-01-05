const std = @import("std");

const SampledLruCache = @import("cache.zig").SampledLruCache;
const SampledLhdCache = @import("cache.zig").SampledLhdCache;
const StableLruCache = @import("cache.zig").StableLruCache;
const StableLhdCache = @import("cache.zig").StableLhdCache;

const Config = @import("cache.zig").Config;

const Weigher10 = struct {
    pub fn weigh(_: @This(), _: []const u8, _: *const u64) usize {
        return 10;
    }
};

test "set/get/delete roundtrip" {
    const alloc = std.testing.allocator;
    var cache = try SampledLruCache(u64).init(alloc, Config{ .max_weight = 10_000 });
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
    var cache = try SampledLruCache(u8).init(alloc, Config{ .max_weight = 10_000 });
    defer cache.deinit();

    try std.testing.expect(cache.get("missing") == null);
    try std.testing.expect(cache.getBorrowed("missing") == null);
}

test "delete missing returns null" {
    const alloc = std.testing.allocator;
    var cache = try SampledLruCache(u8).init(alloc, Config{ .max_weight = 10_000 });
    defer cache.deinit();

    try std.testing.expect(cache.delete("missing") == null);
}

test "replace preserves ttl" {
    const alloc = std.testing.allocator;
    var cache = try SampledLruCache(u8).init(alloc, Config{ .max_weight = 10_000 });
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

test "sampled LHD evicts low density" {
    const alloc = std.testing.allocator;
    var cache = try SampledLhdCache(u64, Weigher10).init(alloc, Config{ .max_weight = 30, .sample_size = 8 });
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
    var cache = try StableLruCache(u64).init(alloc, Config{ .max_weight = 18, .gets_per_promote = 2 });
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
    }

    try std.testing.expect(cache.len() <= 2);
}

test "stable LHD compiles and basic ops" {
    const alloc = std.testing.allocator;
    var cache = try StableLhdCache(u64, Weigher10).init(alloc, Config{ .max_weight = 20, .sample_size = 8 });
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
