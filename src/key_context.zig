const std = @import("std");

/// Default hashing/equality context for cache keys.
///
/// `std.hash_map.AutoContext(K)` uses `std.hash.autoHash`, which rejects slices.
/// This context uses `std.hash.autoHashStrat` with `.Deep` so slices are hashed
/// by content.
///
/// For equality, `std.meta.eql` treats slices as pointer+len; we implement a
/// deep equality that compares slice elements.
///
/// This default context supports the same shapes as `key_ops.Auto(K)`:
/// - plain values
/// - slices (content-hashed/compared)
/// - arrays/optionals/structs/tagged unions composed of supported types
///
/// It rejects raw pointers and untagged unions; provide a custom context for
/// those.
pub fn AutoContext(comptime K: type) type {
    return struct {
        pub fn hash(_: @This(), key: K) u64 {
            var hasher = std.hash.Wyhash.init(0);
            std.hash.autoHashStrat(&hasher, key, .Deep);
            return hasher.final();
        }

        pub fn eql(_: @This(), a: K, b: K) bool {
            return eqlValue(K, a, b);
        }
    };
}

fn eqlValue(comptime T: type, a: T, b: T) bool {
    switch (@typeInfo(T)) {
        .pointer => |info| {
            switch (info.size) {
                .slice => {
                    if (a.len != b.len) return false;
                    for (a, 0..) |elem, i| {
                        if (!eqlValue(info.child, elem, b[i])) return false;
                    }
                    return true;
                },
                else => @compileError("key_context.AutoContext does not support pointer keys; provide a custom Context"),
            }
        },
        .array => {
            if (a.len != b.len) return false;
            for (a, 0..) |elem, i| {
                if (!eqlValue(@TypeOf(elem), elem, b[i])) return false;
            }
            return true;
        },
        .optional => {
            if (a == null and b == null) return true;
            if (a == null or b == null) return false;
            return eqlValue(@TypeOf(a.?), a.?, b.?);
        },
        .@"struct" => |info| {
            inline for (info.fields) |field| {
                if (field.is_comptime) continue;
                if (!eqlValue(field.type, @field(a, field.name), @field(b, field.name))) return false;
            }
            return true;
        },
        .@"union" => |info| {
            if (info.tag_type) |Tag| {
                const ta: Tag = std.meta.activeTag(a);
                const tb: Tag = std.meta.activeTag(b);
                if (ta != tb) return false;
                inline for (info.fields) |field| {
                    if (field.is_comptime) continue;
                    if (std.mem.eql(u8, field.name, @tagName(ta))) {
                        return eqlValue(field.type, @field(a, field.name), @field(b, field.name));
                    }
                }
                unreachable;
            }
            @compileError("key_context.AutoContext does not support untagged union keys; provide a custom Context");
        },
        else => return a == b,
    }
}
