const std = @import("std");

/// Cache item stored by `Cache(V)`.
///
/// This is an internal object allocated on the heap. `Cache(V).ItemRef` is the
/// public reference-counted handle to an item.
pub fn Item(comptime K: type, comptime V: type, comptime KeyOps: type) type {
    return struct {
        const Self = @This();

        key: K,
        value: V,
        weight: usize,

        expires_at_ns: std.atomic.Value(i64),
        created_at_tick: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
        hits: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
        last_access_tick: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
        live: std.atomic.Value(bool) = std.atomic.Value(bool).init(true),
        ref_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(1),

        /// Allocate a new item.
        pub fn create(allocator: std.mem.Allocator, key: K, value: V, ttl_ns: u64, weight: usize) !*Self {
            var owned_key = try KeyOps.clone(allocator, key);
            errdefer KeyOps.deinit(allocator, &owned_key);

            const item = try allocator.create(Self);
            item.* = .{
                .key = owned_key,
                .value = value,
                .weight = weight,
                .expires_at_ns = std.atomic.Value(i64).init(addTtl(nowNs(), ttl_ns)),
                .created_at_tick = std.atomic.Value(u64).init(0),
                .hits = std.atomic.Value(u64).init(0),
                .last_access_tick = std.atomic.Value(u64).init(0),
                .live = std.atomic.Value(bool).init(true),
                .ref_count = std.atomic.Value(usize).init(1),
            };
            return item;
        }

        /// Reset an existing item in-place (used by reuse pools if added later).
        pub fn reset(self: *Self, allocator: std.mem.Allocator, key: K, value: V, ttl_ns: u64, weight: usize) !void {
            KeyOps.deinit(allocator, &self.key);
            maybeDeinitValue(allocator, &self.value);

            self.key = try KeyOps.clone(allocator, key);
            self.value = value;
            self.weight = weight;

            self.expires_at_ns.store(addTtl(nowNs(), ttl_ns), .release);
            self.created_at_tick.store(0, .release);
            self.hits.store(0, .release);
            self.last_access_tick.store(0, .release);
            self.live.store(true, .release);
        }

        /// Increment the reference count.
        pub fn retain(self: *Self) void {
            _ = self.ref_count.fetchAdd(1, .monotonic);
        }

        /// Decrement the reference count and free when it reaches 0.
        pub fn release(self: *Self, allocator: std.mem.Allocator) void {
            const prev = self.ref_count.fetchSub(1, .acq_rel);
            if (prev == 1) {
                KeyOps.deinit(allocator, &self.key);
                maybeDeinitValue(allocator, &self.value);
                allocator.destroy(self);
            }
        }

        /// Mark the item as dead (deleted/evicted).
        pub fn markDead(self: *Self) void {
            self.live.store(false, .release);
        }

        /// Report whether the item is still live.
        pub fn isLive(self: *Self) bool {
            return self.live.load(.acquire);
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

        /// Set the creation tick (used for hit-density eviction).
        pub fn setCreatedAtTick(self: *Self, tick: u64) void {
            self.created_at_tick.store(tick, .release);
        }

        /// Read the creation tick.
        pub fn createdAtTick(self: *Self) u64 {
            return self.created_at_tick.load(.acquire);
        }

        /// Number of successful hits recorded for this item.
        pub fn hitCount(self: *Self) u64 {
            return self.hits.load(.acquire);
        }

        /// Record a cache hit and update last access tick.
        pub fn recordHit(self: *Self, tick: u64) void {
            _ = self.hits.fetchAdd(1, .acq_rel);
            self.touch(tick);
        }

        /// Update the access tick.
        pub fn touch(self: *Self, tick: u64) void {
            self.last_access_tick.store(tick, .release);
        }

        /// Read the access tick.
        pub fn lastAccessTick(self: *Self) u64 {
            return self.last_access_tick.load(.acquire);
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
