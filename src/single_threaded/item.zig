const std = @import("std");

pub fn Item(comptime V: type, comptime stable_lru: bool) type {
    return struct {
        const Self = @This();

        key: []u8,
        value: V,
        weight: usize,

        expires_at_ns: i64,
        created_at_tick: u64 = 0,
        hits: u64 = 0,
        last_access_tick: u64 = 0,
        live: bool = true,
        ref_count: usize = 1,
        items_index: usize = 0,

        lru_node: if (stable_lru) std.DoublyLinkedList.Node else void = if (stable_lru) .{} else {},
        promotions: if (stable_lru) usize else void = if (stable_lru) 0 else {},
        slru_is_protected: if (stable_lru) bool else void = if (stable_lru) false else {},

        pub fn create(allocator: std.mem.Allocator, key: []const u8, value: V, ttl_ns: u64, weight: usize) !*Self {
            const owned_key = try allocator.dupe(u8, key);
            errdefer allocator.free(owned_key);

            const item = try allocator.create(Self);
            item.* = .{
                .key = owned_key,
                .value = value,
                .weight = weight,
                .expires_at_ns = addTtl(nowNs(), ttl_ns),
            };
            return item;
        }

        pub fn retain(self: *Self) void {
            self.ref_count += 1;
        }

        pub fn release(self: *Self, allocator: std.mem.Allocator) void {
            self.ref_count -= 1;
            if (self.ref_count == 0) {
                allocator.free(self.key);
                maybeDeinitValue(allocator, &self.value);
                allocator.destroy(self);
            }
        }

        pub fn markDead(self: *Self) void {
            self.live = false;
        }

        pub fn isLive(self: *Self) bool {
            return self.live;
        }

        pub fn isExpired(self: *Self) bool {
            return self.expires_at_ns < nowNs();
        }

        pub fn ttlNs(self: *Self) u64 {
            const expires = self.expires_at_ns;
            const now = nowNs();
            if (expires <= now) return 0;
            return @intCast(@as(i64, expires - now));
        }

        pub fn extend(self: *Self, ttl_ns: u64) void {
            self.expires_at_ns = addTtl(nowNs(), ttl_ns);
        }

        pub fn setCreatedAtTick(self: *Self, tick: u64) void {
            self.created_at_tick = tick;
        }

        pub fn createdAtTick(self: *Self) u64 {
            return self.created_at_tick;
        }

        pub fn hitCount(self: *Self) u64 {
            return self.hits;
        }

        pub fn recordHit(self: *Self, tick: u64) void {
            self.hits += 1;
            self.touch(tick);
        }

        pub fn touch(self: *Self, tick: u64) void {
            self.last_access_tick = tick;
        }

        pub fn lastAccessTick(self: *Self) u64 {
            return self.last_access_tick;
        }

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

        fn maybeDeinitValue(allocator: std.mem.Allocator, value: *V) void {
            switch (@typeInfo(V)) {
                .@"struct", .@"union", .@"enum", .@"opaque" => {
                    if (@hasDecl(V, "deinit")) {
                        value.deinit(allocator);
                    }
                },
                else => {},
            }
        }
    };
}
