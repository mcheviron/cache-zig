const std = @import("std");

fn nowNs() i64 {
    const ns: i128 = std.time.nanoTimestamp();
    if (ns > std.math.maxInt(i64)) return std.math.maxInt(i64);
    if (ns < std.math.minInt(i64)) return std.math.minInt(i64);
    return @intCast(ns);
}

fn addTtl(now: i64, ttl_ns: u64) i64 {
    const ttl: i64 = if (ttl_ns > @as(u64, std.math.maxInt(i64))) std.math.maxInt(i64) else @intCast(ttl_ns);
    return std.math.add(i64, now, ttl) catch std.math.maxInt(i64);
}

fn maybeDeinitValue(comptime V: type, allocator: std.mem.Allocator, value: *V) void {
    switch (@typeInfo(V)) {
        .@"struct", .@"union", .@"enum", .@"opaque" => {
            if (@hasDecl(V, "deinit")) {
                value.deinit(allocator);
            }
        },
        else => {},
    }
}

/// Cache item stored by `Cache(V)`.
///
/// This is an internal object allocated on the heap. `Cache(V).ItemRef` is the
/// public reference-counted handle to an item.
pub fn Item(comptime V: type) type {
    return struct {
        const Self = @This();

        key: []u8,
        value: V,
        weight: usize,

        expires_at_ns: std.atomic.Value(i64),
        last_access: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
        live: std.atomic.Value(u8) = std.atomic.Value(u8).init(1),
        ref_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(1),

        /// Allocate a new item.
        pub fn create(allocator: std.mem.Allocator, key: []const u8, value: V, ttl_ns: u64, weight: usize) !*Self {
            const owned_key = try allocator.dupe(u8, key);
            const item = try allocator.create(Self);
            item.* = .{
                .key = owned_key,
                .value = value,
                .weight = weight,
                .expires_at_ns = std.atomic.Value(i64).init(addTtl(nowNs(), ttl_ns)),
            };
            return item;
        }

        /// Reset an existing item in-place (used by reuse pools if added later).
        pub fn reset(self: *Self, allocator: std.mem.Allocator, key: []const u8, value: V, ttl_ns: u64, weight: usize) !void {
            allocator.free(self.key);
            maybeDeinitValue(V, allocator, &self.value);

            self.key = try allocator.dupe(u8, key);
            self.value = value;
            self.weight = weight;

            self.expires_at_ns.store(addTtl(nowNs(), ttl_ns), .release);
            self.last_access.store(0, .release);
            self.live.store(1, .release);
        }

        /// Increment the reference count.
        pub fn retain(self: *Self) void {
            _ = self.ref_count.fetchAdd(1, .monotonic);
        }

        /// Decrement the reference count and free when it reaches 0.
        pub fn release(self: *Self, allocator: std.mem.Allocator) void {
            const prev = self.ref_count.fetchSub(1, .acq_rel);
            if (prev == 1) {
                allocator.free(self.key);
                maybeDeinitValue(V, allocator, &self.value);
                allocator.destroy(self);
            }
        }

        /// Mark the item as dead (deleted/evicted).
        pub fn markDead(self: *Self) void {
            self.live.store(0, .release);
        }

        /// Report whether the item is still live.
        pub fn isLive(self: *Self) bool {
            return self.live.load(.acquire) == 1;
        }

        /// Report whether the item is expired.
        pub fn isExpired(self: *Self) bool {
            return self.expires_at_ns.load(.acquire) < nowNs();
        }

        /// Remaining TTL in nanoseconds.
        pub fn ttlNs(self: *Self) u64 {
            const expires = self.expires_at_ns.load(.acquire);
            const now = nowNs();
            if (expires <= now) return 0;
            return @intCast(@as(i64, expires - now));
        }

        /// Set expiration to now+ttl_ns.
        pub fn extend(self: *Self, ttl_ns: u64) void {
            self.expires_at_ns.store(addTtl(nowNs(), ttl_ns), .release);
        }

        /// Update the access tick.
        pub fn touch(self: *Self, tick: u64) void {
            self.last_access.store(tick, .release);
        }

        /// Read the access tick.
        pub fn lastAccess(self: *Self) u64 {
            return self.last_access.load(.acquire);
        }
    };
}
