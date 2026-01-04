const std = @import("std");

pub fn Bytes(comptime V: type) type {
    return struct {
        pub fn weigh(_: @This(), key: []const u8, _: *const V) usize {
            return key.len + @sizeOf(V);
        }
    };
}

pub fn Items(comptime V: type) type {
    return struct {
        pub fn weigh(_: @This(), _: []const u8, _: *const V) usize {
            return 1;
        }
    };
}

test "weigher bytes/items" {
    const V = u64;
    const v: V = 123;

    try std.testing.expectEqual(@as(usize, 1 + @sizeOf(V)), Bytes(V).weigh(.{}, "k", &v));
    try std.testing.expectEqual(@as(usize, 1), Items(V).weigh(.{}, "k", &v));
}
