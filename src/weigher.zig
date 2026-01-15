const std = @import("std");

pub fn Bytes(comptime K: type, comptime V: type) type {
    return struct {
        pub fn weigh(_: @This(), key: K, _: *const V) usize {
            return keyLen(K, key) + @sizeOf(V);
        }

        fn keyLen(comptime T: type, value: T) usize {
            const info = @typeInfo(T);
            if (info == .pointer and info.pointer.size == .slice and info.pointer.child == u8) {
                return value.len;
            }
            @compileError("weigher.Bytes requires key type to be a u8 slice");
        }
    };
}

pub fn Items(comptime K: type, comptime V: type) type {
    return struct {
        pub fn weigh(_: @This(), _: K, _: *const V) usize {
            return 1;
        }
    };
}

test "weigher bytes/items" {
    const K = []const u8;
    const V = u64;
    const v: V = 123;

    try std.testing.expectEqual(@as(usize, 1 + @sizeOf(V)), Bytes(K, V).weigh(.{}, "k", &v));
    try std.testing.expectEqual(@as(usize, 1), Items(K, V).weigh(.{}, "k", &v));
}
