const std = @import("std");

const Allocator = std.mem.Allocator;

/// Key ownership helpers.
///
/// The cache owns keys by default, which requires the ability to clone and
/// deinitialize keys.
///
/// `Auto(K)` deep-clones many common value and container shapes:
/// - Plain value types (ints/enums/bools/etc) are copied.
/// - Slices are cloned element-by-element (the slice backing memory is owned by
///   the cache).
/// - Arrays/optionals/structs/tagged unions are deep-cloned recursively.
///
/// `Auto(K)` rejects:
/// - Raw pointers (`*T`, `[*]T`, `?*T`, etc): ownership is ambiguous.
/// - Untagged unions.
///
/// For these, provide a custom KeyOps with `clone`/`deinit`.
pub fn Auto(comptime K: type) type {
    return struct {
        pub fn clone(allocator: Allocator, key: K) Allocator.Error!K {
            return cloneValue(K, allocator, key);
        }

        pub fn deinit(allocator: Allocator, key: *K) void {
            deinitValue(K, allocator, key);
        }
    };
}

fn cloneValue(comptime T: type, allocator: Allocator, value: T) Allocator.Error!T {
    switch (@typeInfo(T)) {
        .optional => |info| {
            return if (value) |v|
                @as(T, try cloneValue(info.child, allocator, v))
            else
                null;
        },

        .array => |info| {
            var out: T = undefined;
            errdefer deinitValue(T, allocator, &out);

            for (value, 0..) |elem, i| {
                out[i] = try cloneValue(info.child, allocator, elem);
            }
            return out;
        },

        .pointer => |info| {
            if (info.size != .slice) {
                @compileError("key_ops.Auto does not support pointer keys; provide a custom KeyOps");
            }

            var out = try allocator.alloc(info.child, value.len);
            errdefer {
                for (out) |*elem| deinitValue(info.child, allocator, elem);
                allocator.free(out);
            }

            for (value, 0..) |elem, i| {
                out[i] = try cloneValue(info.child, allocator, elem);
            }

            return out;
        },

        .@"struct" => |info| {
            var out: T = value;
            errdefer deinitValue(T, allocator, &out);

            inline for (info.fields) |field| {
                if (field.is_comptime) continue;
                @field(out, field.name) = try cloneValue(field.type, allocator, @field(value, field.name));
            }

            return out;
        },

        .@"union" => |info| {
            const tag_type = info.tag_type orelse {
                @compileError("key_ops.Auto does not support untagged union keys; provide a custom KeyOps");
            };

            const tag: tag_type = std.meta.activeTag(value);
            inline for (info.fields) |field| {
                if (field.is_comptime) continue;
                if (std.mem.eql(u8, field.name, @tagName(tag))) {
                    const payload = @field(value, field.name);
                    const cloned = try cloneValue(field.type, allocator, payload);
                    return @unionInit(T, field.name, cloned);
                }
            }

            unreachable;
        },

        else => return value,
    }
}

fn deinitValue(comptime T: type, allocator: Allocator, value: *T) void {
    switch (@typeInfo(T)) {
        .optional => |info| {
            if (value.*) |v| {
                var tmp = v;
                deinitValue(info.child, allocator, &tmp);
                value.* = null;
            }
        },

        .array => |info| {
            for (value.*) |*elem| {
                deinitValue(info.child, allocator, elem);
            }
        },

        .pointer => |info| {
            if (info.size != .slice) {
                @compileError("key_ops.Auto does not support pointer keys; provide a custom KeyOps");
            }

            const slice = value.*;
            for (slice) |elem| {
                var tmp = elem;
                deinitValue(info.child, allocator, &tmp);
            }
            allocator.free(@constCast(slice));
            value.* = &[_]info.child{};
        },

        .@"struct" => |info| {
            inline for (info.fields) |field| {
                if (field.is_comptime) continue;
                deinitValue(field.type, allocator, &@field(value.*, field.name));
            }
        },

        .@"union" => |info| {
            const tag_type = info.tag_type orelse {
                @compileError("key_ops.Auto does not support untagged union keys; provide a custom KeyOps");
            };

            const tag: tag_type = std.meta.activeTag(value.*);
            inline for (info.fields) |field| {
                if (field.is_comptime) continue;
                if (std.mem.eql(u8, field.name, @tagName(tag))) {
                    var payload = @field(value.*, field.name);
                    deinitValue(field.type, allocator, &payload);
                    return;
                }
            }
        },

        else => {},
    }
}
