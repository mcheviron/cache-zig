const std = @import("std");
const Cache = @import("cache.zig").Cache;
const Config = @import("config.zig").Config;

test "set/get/delete roundtrip" {
    const alloc = std.testing.allocator;
    var cache = try Cache(u64).init(alloc, Config{ .max_weight = 10_000 });
    defer cache.deinit();

    var item = try cache.set("a", 123, 60 * std.time.ns_per_s);
    defer item.deinit();
    try std.testing.expectEqualStrings("a", item.key());
    try std.testing.expectEqual(@as(u64, 123), item.value().*);

    var got = (cache.get("a") orelse return error.Miss);
    defer got.deinit();
    try std.testing.expectEqual(@as(u64, 123), got.value().*);

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
    var cache = try Cache(u8).init(alloc, Config{ .max_weight = 10_000 });
    defer cache.deinit();

    try std.testing.expect(cache.get("missing") == null);
}

test "delete missing returns null" {
    const alloc = std.testing.allocator;
    var cache = try Cache(u8).init(alloc, Config{ .max_weight = 10_000 });
    defer cache.deinit();

    try std.testing.expect(cache.delete("missing") == null);
}

test "extend missing returns false" {
    const alloc = std.testing.allocator;
    var cache = try Cache(u8).init(alloc, Config{ .max_weight = 10_000 });
    defer cache.deinit();

    try std.testing.expect(!cache.extend("missing", 1 * std.time.ns_per_s));
}

test "extend updates ttl" {
    const alloc = std.testing.allocator;
    var cache = try Cache(u8).init(alloc, Config{ .max_weight = 10_000 });
    defer cache.deinit();

    {
        var a = try cache.set("a", 1, 1 * std.time.ns_per_ms);
        defer a.deinit();
    }

    std.Thread.sleep(5 * std.time.ns_per_ms);

    try std.testing.expect(cache.extend("a", 60 * std.time.ns_per_s));

    var got = (cache.get("a") orelse return error.Miss);
    defer got.deinit();
    try std.testing.expect(!got.isExpired());
}

test "get returns expired item" {
    const alloc = std.testing.allocator;
    var cache = try Cache(u8).init(alloc, Config{ .max_weight = 10_000 });
    defer cache.deinit();

    var item = try cache.set("a", 1, 1 * std.time.ns_per_ms);
    defer item.deinit();

    std.Thread.sleep(5 * std.time.ns_per_ms);

    var got = (cache.get("a") orelse return error.Miss);
    defer got.deinit();
    try std.testing.expect(got.isExpired());
}

test "replace preserves ttl" {
    const alloc = std.testing.allocator;
    var cache = try Cache(u8).init(alloc, Config{ .max_weight = 10_000 });
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

test "snapshot and filter" {
    const alloc = std.testing.allocator;
    var cache = try Cache(u8).init(alloc, Config{ .max_weight = 10_000 });
    defer cache.deinit();

    {
        var a = try cache.set("a", 1, 60 * std.time.ns_per_s);
        defer a.deinit();
        var b = try cache.set("b", 2, 60 * std.time.ns_per_s);
        defer b.deinit();
    }

    var snap = try cache.snapshot(alloc);
    defer {
        for (snap.items) |it| it.deinit();
        snap.deinit(alloc);
    }

    try std.testing.expectEqual(@as(usize, 2), snap.items.len);

    const Pred = struct {
        pub fn pred(_: *@This(), item: *Cache(u8).Item) bool {
            return std.mem.eql(u8, item.key, "b");
        }
    };

    var pred = Pred{};

    var only_b = try cache.filter(alloc, Cache(u8).ItemPredicate.init(&pred));
    defer {
        for (only_b.items) |it| it.deinit();
        only_b.deinit(alloc);
    }

    try std.testing.expectEqual(@as(usize, 1), only_b.items.len);
    try std.testing.expectEqualStrings("b", only_b.items[0].key());
}

test "clear removes entries" {
    const alloc = std.testing.allocator;
    var cache = try Cache(u8).init(alloc, Config{ .max_weight = 10_000 });
    defer cache.deinit();

    {
        var a = try cache.set("a", 1, 60 * std.time.ns_per_s);
        defer a.deinit();
    }

    cache.clear();

    try std.testing.expect(cache.peek("a") == null);
}

test "evicts oldest when over weight" {
    const alloc = std.testing.allocator;
    const cfg = Config{ .shards = 2, .max_weight = 30, .items_to_prune = 10, .sample_size = 1024 };

    var cache = try Cache(u64).init(alloc, cfg);
    defer cache.deinit();

    {
        var k1 = try cache.set("k1", 1, 60 * std.time.ns_per_s);
        defer k1.deinit();
        var k2 = try cache.set("k2", 2, 60 * std.time.ns_per_s);
        defer k2.deinit();
        var k3 = try cache.set("k3", 3, 60 * std.time.ns_per_s);
        defer k3.deinit();
    }

    if (cache.get("k2")) |k2| {
        defer k2.deinit();
    }

    {
        var k4 = try cache.set("k4", 4, 60 * std.time.ns_per_s);
        defer k4.deinit();
    }

    try std.testing.expect(cache.peek("k1") == null);
}
