const std = @import("std");

const Config = @import("config.zig").Config;
const ItemMod = @import("item.zig");
const ShardMod = @import("shard.zig");

fn _shardIndex(key: []const u8, mask: usize) usize {
    var h = std.hash.Fnv1a_64.init();
    h.update(key);
    const digest = h.final();
    return @as(usize, @intCast(digest)) & mask;
}

/// Construct a cache type specialized for value type `V`.
///
/// The cache stores items by string key (`[]const u8` in Zig terms) and a
/// compile-time value type `V`.
///
/// Public methods return `ItemRef`, a reference-counted handle which must be
/// `deinit()`'d by the caller.
///
/// TTL note:
///
/// By default, expired items are treated as cache misses.
/// Set `Config.treat_expired_as_miss=false` to allow `get`/`peek` to return
/// expired items.
///
/// Eviction:
///
/// When the cache exceeds `Config.max_weight`, eviction samples candidates
/// across shards and evicts using the configured eviction policy.
pub fn Cache(comptime V: type) type {
    return struct {
        const Self = @This();

        /// Internal item type.
        pub const Item = ItemMod.Item(V);
        const Shard = ShardMod.Shard(*Item);

        /// Optional weight function.
        ///
        /// If provided, this controls the weight used for eviction.
        ///
        /// # Example
        ///
        /// ```zig
        /// const cache_zig = @import("cache_zig");
        ///
        /// const W = struct {
        ///     pub fn weigh(_: *@This(), _: []const u8, _: *const u64) usize {
        ///         return 10;
        ///     }
        /// };
        ///
        /// const Gen = struct {
        ///     fn call(ctx: *anyopaque, key: []const u8, value: *const u64) usize {
        ///         const w: *W = @ptrCast(@alignCast(ctx));
        ///         return w.weigh(key, value);
        ///     }
        /// };
        ///
        /// var w = W{};
        /// const weigher = cache_zig.Cache(u64).Weigher{ .ctx = &w, .callFn = Gen.call };
        /// _ = weigher;
        /// ```
        pub const Weigher = struct {
            ctx: *anyopaque,
            callFn: *const fn (ctx: *anyopaque, key: []const u8, value: *const V) usize,

            /// Call the weigher.
            pub fn call(self: Weigher, key: []const u8, value: *const V) usize {
                return self.callFn(self.ctx, key, value);
            }
        };

        /// Predicate interface for `filter`.
        ///
        /// # Example
        ///
        /// ```zig
        /// const cache_zig = @import("cache_zig");
        ///
        /// const Pred = struct {
        ///     pub fn pred(_: *@This(), item: *cache_zig.Cache(u64).Item) bool {
        ///         return item.value == 1;
        ///     }
        /// };
        ///
        /// var p = Pred{};
        /// const pred = cache_zig.Cache(u64).ItemPredicate.init(&p);
        /// _ = pred;
        /// ```
        pub const ItemPredicate = struct {
            ctx: *anyopaque,
            callFn: *const fn (ctx: *anyopaque, item: *Item) bool,

            /// Invoke the predicate.
            pub fn call(self: ItemPredicate, item: *Item) bool {
                return self.callFn(self.ctx, item);
            }

            /// Convenience initializer.
            ///
            /// Expects `ptr` to be a single-item pointer where the pointed-to type
            /// declares `pub fn pred(self: <ptr type>, item: *Item) bool`.
            pub fn init(ptr: anytype) ItemPredicate {
                const T = @TypeOf(ptr);
                const ti = @typeInfo(T);
                if (ti != .pointer or ti.pointer.size != .one) {
                    @compileError("ItemPredicate.init expects a single-item pointer");
                }

                const Gen = struct {
                    fn _call(ctx: *anyopaque, item: *Item) bool {
                        const self_ptr: T = @ptrCast(@alignCast(ctx));
                        return ti.pointer.child.pred(self_ptr, item);
                    }
                };

                return .{ .ctx = ptr, .callFn = Gen._call };
            }
        };

        /// Handle to an item stored in the cache.
        ///
        /// This is a reference-counted handle. Always call `deinit()`.
        /// Dropping an ItemRef does not delete the key from the cache; deletion
        /// happens via `delete` or eviction.
        pub const ItemRef = struct {
            item: *Item,
            allocator: std.mem.Allocator,

            /// Release this reference.
            ///
            /// Always call this (usually via `defer ref.deinit()`).
            pub fn deinit(self: ItemRef) void {
                self.item.release(self.allocator);
            }

            /// Get the key bytes.
            ///
            /// # Example
            ///
            /// ```zig
            /// const std = @import("std");
            /// const cache_zig = @import("cache_zig");
            ///
            /// var cache = try cache_zig.Cache(u64).init(std.testing.allocator, .{});
            /// defer cache.deinit();
            ///
            /// var set_ref = try cache.set("k", 1, 60 * std.time.ns_per_s);
            /// defer set_ref.deinit();
            ///
            /// var ref = cache.get("k") orelse return error.Miss;
            /// defer ref.deinit();
            /// try std.testing.expectEqualStrings("k", ref.key());
            /// ```
            pub fn key(self: ItemRef) []const u8 {
                return self.item.key;
            }

            /// Get a pointer to the stored value.
            ///
            /// # Example
            ///
            /// ```zig
            /// const std = @import("std");
            /// const cache_zig = @import("cache_zig");
            ///
            /// var cache = try cache_zig.Cache(u64).init(std.testing.allocator, .{});
            /// defer cache.deinit();
            ///
            /// var set_ref = try cache.set("k", 123, 60 * std.time.ns_per_s);
            /// defer set_ref.deinit();
            ///
            /// var ref = cache.get("k") orelse return error.Miss;
            /// defer ref.deinit();
            /// try std.testing.expectEqual(@as(u64, 123), ref.value().*);
            /// ```
            pub fn value(self: ItemRef) *const V {
                return &self.item.value;
            }

            /// Whether this item is expired.
            ///
            /// # Example
            ///
            /// ```zig
            /// const std = @import("std");
            /// const cache_zig = @import("cache_zig");
            ///
            /// var cache = try cache_zig.Cache(u64).init(std.testing.allocator, .{});
            /// defer cache.deinit();
            ///
            /// var set_ref = try cache.set("k", 1, 1);
            /// defer set_ref.deinit();
            ///
            /// var ref = cache.peek("k") orelse return error.Miss;
            /// defer ref.deinit();
            /// _ = ref.isExpired();
            /// ```
            pub fn isExpired(self: ItemRef) bool {
                return self.item.isExpired();
            }

            /// Remaining TTL in nanoseconds.
            ///
            /// # Example
            ///
            /// ```zig
            /// const std = @import("std");
            /// const cache_zig = @import("cache_zig");
            ///
            /// var cache = try cache_zig.Cache(u64).init(std.testing.allocator, .{});
            /// defer cache.deinit();
            ///
            /// var set_ref = try cache.set("k", 1, 60 * std.time.ns_per_s);
            /// defer set_ref.deinit();
            ///
            /// var ref = cache.peek("k") orelse return error.Miss;
            /// defer ref.deinit();
            /// try std.testing.expect(ref.ttlNs() > 0);
            /// ```
            pub fn ttlNs(self: ItemRef) u64 {
                return self.item.ttlNs();
            }

            /// Extend TTL (sets expiration to now + ttl_ns).
            ///
            /// # Example
            ///
            /// ```zig
            /// const std = @import("std");
            /// const cache_zig = @import("cache_zig");
            ///
            /// var cache = try cache_zig.Cache(u64).init(std.testing.allocator, .{});
            /// defer cache.deinit();
            ///
            /// var set_ref = try cache.set("k", 1, 1);
            /// defer set_ref.deinit();
            ///
            /// var ref = cache.peek("k") orelse return error.Miss;
            /// defer ref.deinit();
            /// ref.extend(60 * std.time.ns_per_s);
            /// ```
            pub fn extend(self: ItemRef, ttl_ns: u64) void {
                self.item.extend(ttl_ns);
            }
        };

        allocator: std.mem.Allocator,
        config: Config,
        shard_mask: usize,
        shards: []Shard,

        access_clock: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
        total_weight: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        rng_state: std.atomic.Value(u64) = std.atomic.Value(u64).init(0x9e3779b97f4a7c15),

        weigher: ?Weigher,

        /// Initialize a cache.
        ///
        /// `config_in.build()` is called internally.
        ///
        /// # Example
        ///
        /// ```zig
        /// const std = @import("std");
        /// const cache_zig = @import("cache_zig");
        ///
        /// var cache = try cache_zig.Cache(u64).init(std.testing.allocator, .{});
        /// defer cache.deinit();
        /// ```
        pub fn init(allocator: std.mem.Allocator, config_in: Config) !Self {
            const cfg = try config_in.build();
            const shards = try allocator.alloc(Shard, cfg.shards);
            for (shards) |*s| s.* = .{};

            return .{
                .allocator = allocator,
                .config = cfg,
                .shard_mask = cfg.shards - 1,
                .shards = shards,
                .weigher = null,
            };
        }

        /// Initialize a cache with a custom weigher.
        ///
        /// # Example
        ///
        /// ```zig
        /// const std = @import("std");
        /// const cache_zig = @import("cache_zig");
        ///
        /// const W = struct {
        ///     pub fn weigh(_: *@This(), _: []const u8, _: *const u64) usize {
        ///         return 10;
        ///     }
        /// };
        ///
        /// const Gen = struct {
        ///     fn call(ctx: *anyopaque, key: []const u8, value: *const u64) usize {
        ///         const w: *W = @ptrCast(@alignCast(ctx));
        ///         return w.weigh(key, value);
        ///     }
        /// };
        ///
        /// var w = W{};
        /// const weigher = cache_zig.Cache(u64).Weigher{ .ctx = &w, .callFn = Gen.call };
        ///
        /// var cache = try cache_zig.Cache(u64).initWithWeigher(std.testing.allocator, .{}, weigher);
        /// defer cache.deinit();
        /// ```
        pub fn initWithWeigher(allocator: std.mem.Allocator, config_in: Config, weigher: Weigher) !Self {
            var c = try Self.init(allocator, config_in);
            c.weigher = weigher;
            return c;
        }

        /// Deinitialize the cache and free all items.
        ///
        /// After calling `deinit`, all `ItemRef`s that still exist are invalid.
        ///
        /// # Example
        ///
        /// ```zig
        /// const std = @import("std");
        /// const cache_zig = @import("cache_zig");
        ///
        /// var cache = try cache_zig.Cache(u64).init(std.testing.allocator, .{});
        /// cache.deinit();
        /// ```
        pub fn deinit(self: *Self) void {
            for (self.shards) |*shard| {
                shard.lock.lock();
                for (shard.items.items) |item| {
                    item.markDead();
                    item.release(self.allocator);
                }
                shard.map.deinit(self.allocator);
                shard.items.deinit(self.allocator);
                shard.pos.deinit(self.allocator);
                shard.lock.unlock();
            }
            self.allocator.free(self.shards);
        }

        /// Get an item and update its access metadata.
        ///
        /// Returns `null` on miss (including expired entries when
        /// `Config.treat_expired_as_miss=true`).
        ///
        /// # Example
        ///
        /// ```zig
        /// const std = @import("std");
        /// const cache_zig = @import("cache_zig");
        ///
        /// var cache = try cache_zig.Cache(u64).init(std.testing.allocator, .{});
        /// defer cache.deinit();
        ///
        /// var set_ref = try cache.set("k", 1, 60 * std.time.ns_per_s);
        /// defer set_ref.deinit();
        ///
        /// var ref = cache.get("k") orelse return error.Miss;
        /// defer ref.deinit();
        /// _ = ref;
        /// ```
        pub fn get(self: *Self, key: []const u8) ?ItemRef {
            const shard = &self.shards[_shardIndex(key, self.shard_mask)];
            const item = shard.get(key) orelse return null;

            item.retain();

            if (self.config.treat_expired_as_miss and item.isExpired()) {
                item.release(self.allocator);
                return null;
            }

            if (item.isLive() and !item.isExpired()) {
                item.recordHit(self.nextTick());
            }

            return .{ .item = item, .allocator = self.allocator };
        }

        /// Get an item without updating access metadata.
        ///
        /// # Example
        ///
        /// ```zig
        /// const std = @import("std");
        /// const cache_zig = @import("cache_zig");
        ///
        /// var cache = try cache_zig.Cache(u64).init(std.testing.allocator, .{});
        /// defer cache.deinit();
        ///
        /// var set_ref = try cache.set("k", 1, 60 * std.time.ns_per_s);
        /// defer set_ref.deinit();
        ///
        /// var ref = cache.peek("k") orelse return error.Miss;
        /// defer ref.deinit();
        /// _ = ref;
        /// ```
        pub fn peek(self: *Self, key: []const u8) ?ItemRef {
            const shard = &self.shards[_shardIndex(key, self.shard_mask)];
            const item = shard.get(key) orelse return null;
            item.retain();

            if (self.config.treat_expired_as_miss and item.isExpired()) {
                item.release(self.allocator);
                return null;
            }

            return .{ .item = item, .allocator = self.allocator };
        }

        /// Insert or update a key/value with TTL (in nanoseconds).
        ///
        /// Returns an `ItemRef` pointing at the stored value. This does not remove
        /// the item from the cache.
        ///
        /// # Example
        ///
        /// ```zig
        /// const std = @import("std");
        /// const cache_zig = @import("cache_zig");
        ///
        /// var cache = try cache_zig.Cache(u64).init(std.testing.allocator, .{});
        /// defer cache.deinit();
        ///
        /// var ref = try cache.set("k", 123, 1 * std.time.ns_per_s);
        /// defer ref.deinit();
        /// try std.testing.expectEqual(@as(u64, 123), ref.value().*);
        /// ```
        pub fn set(self: *Self, key: []const u8, value: V, ttl_ns: u64) !ItemRef {
            const weight = self.weigh(key, &value);
            const item = try Item.create(self.allocator, key, value, ttl_ns, weight);
            const tick = self.nextTick();
            item.setCreatedTick(tick);
            item.touch(tick);

            // Shard owns one ref.
            item.retain();

            const shard = &self.shards[_shardIndex(key, self.shard_mask)];
            if (try shard.set(self.allocator, item.key, item)) |old| {
                old.markDead();
                _ = self.total_weight.fetchSub(old.weight, .acq_rel);
                old.release(self.allocator); // drop shard ref
            }

            _ = self.total_weight.fetchAdd(item.weight, .acq_rel);
            try self.evictIfNeeded();

            // Caller ref.
            return .{ .item = item, .allocator = self.allocator };
        }

        /// Replace a value while preserving TTL.
        /// Returns null if the key does not exist.
        ///
        /// # Example
        ///
        /// ```zig
        /// const std = @import("std");
        /// const cache_zig = @import("cache_zig");
        ///
        /// var cache = try cache_zig.Cache(u64).init(std.testing.allocator, .{});
        /// defer cache.deinit();
        ///
        /// var set_ref = try cache.set("k", 1, 60 * std.time.ns_per_s);
        /// defer set_ref.deinit();
        ///
        /// const replaced = try cache.replace("k", 2) orelse return error.Miss;
        /// defer replaced.deinit();
        /// try std.testing.expectEqual(@as(u64, 2), replaced.value().*);
        /// ```
        pub fn replace(self: *Self, key: []const u8, value: V) !?ItemRef {
            const existing = self._peekRaw(key) orelse return null;
            defer existing.deinit();

            const ttl = existing.ttlNs();
            return try self.set(key, value, ttl);
        }

        /// Delete a key and return the removed item.
        /// Returns null if missing.
        ///
        /// The returned `ItemRef` holds the removed value until `deinit()`.
        ///
        /// # Example
        ///
        /// ```zig
        /// const std = @import("std");
        /// const cache_zig = @import("cache_zig");
        ///
        /// var cache = try cache_zig.Cache(u64).init(std.testing.allocator, .{});
        /// defer cache.deinit();
        ///
        /// var set_ref = try cache.set("k", 1, 60 * std.time.ns_per_s);
        /// defer set_ref.deinit();
        ///
        /// var deleted = cache.delete("k") orelse return error.Miss;
        /// defer deleted.deinit();
        /// try std.testing.expect(cache.peek("k") == null);
        /// ```
        pub fn delete(self: *Self, key: []const u8) ?ItemRef {
            const shard = &self.shards[_shardIndex(key, self.shard_mask)];
            const item = shard.delete(self.allocator, key) orelse return null;

            item.markDead();
            self.total_weight.store(self.total_weight.load(.acquire) -| item.weight, .release);

            // Transfer shard ref to caller.
            return .{ .item = item, .allocator = self.allocator };
        }

        /// Clear all items.
        ///
        /// # Example
        ///
        /// ```zig
        /// const std = @import("std");
        /// const cache_zig = @import("cache_zig");
        ///
        /// var cache = try cache_zig.Cache(u64).init(std.testing.allocator, .{});
        /// defer cache.deinit();
        ///
        /// cache.clear();
        /// try std.testing.expect(cache.isEmpty());
        /// ```
        pub fn clear(self: *Self) void {
            for (self.shards) |*shard| {
                shard.clear(self.allocator);
            }
            self.total_weight.store(0, .release);
        }

        /// Count of stored items.
        ///
        /// # Example
        ///
        /// ```zig
        /// const std = @import("std");
        /// const cache_zig = @import("cache_zig");
        ///
        /// var cache = try cache_zig.Cache(u64).init(std.testing.allocator, .{});
        /// defer cache.deinit();
        ///
        /// try std.testing.expectEqual(@as(usize, 0), cache.itemCount());
        /// ```
        pub fn itemCount(self: *Self) usize {
            var n: usize = 0;
            for (self.shards) |*s| {
                n += s.len();
            }
            return n;
        }

        /// Alias for itemCount.
        ///
        /// # Example
        ///
        /// ```zig
        /// const std = @import("std");
        /// const cache_zig = @import("cache_zig");
        ///
        /// var cache = try cache_zig.Cache(u64).init(std.testing.allocator, .{});
        /// defer cache.deinit();
        /// try std.testing.expectEqual(cache.itemCount(), cache.len());
        /// ```
        pub fn len(self: *Self) usize {
            return self.itemCount();
        }

        /// Whether cache is empty.
        ///
        /// # Example
        ///
        /// ```zig
        /// const std = @import("std");
        /// const cache_zig = @import("cache_zig");
        ///
        /// var cache = try cache_zig.Cache(u64).init(std.testing.allocator, .{});
        /// defer cache.deinit();
        /// try std.testing.expect(cache.isEmpty());
        /// ```
        pub fn isEmpty(self: *Self) bool {
            return self.len() == 0;
        }

        /// Extend an item TTL. Returns `false` if missing.
        ///
        /// This sets expiration to `now + ttl_ns`.
        ///
        /// # Example
        ///
        /// ```zig
        /// const std = @import("std");
        /// const cache_zig = @import("cache_zig");
        ///
        /// var cache = try cache_zig.Cache(u64).init(std.testing.allocator, .{});
        /// defer cache.deinit();
        ///
        /// var set_ref = try cache.set("k", 1, 1);
        /// defer set_ref.deinit();
        ///
        /// try std.testing.expect(cache.extend("k", 60 * std.time.ns_per_s));
        /// ```
        pub fn extend(self: *Self, key: []const u8, ttl_ns: u64) bool {
            const ref = self._peekRaw(key) orelse return false;
            defer ref.deinit();

            ref.extend(ttl_ns);
            ref.item.touch(self.nextTick());
            return true;
        }

        fn _peekRaw(self: *Self, key: []const u8) ?ItemRef {
            const shard = &self.shards[_shardIndex(key, self.shard_mask)];
            const item = shard.get(key) orelse return null;
            item.retain();
            return .{ .item = item, .allocator = self.allocator };
        }

        /// Snapshot all items.
        ///
        /// Returned ItemRefs must be deinit'd by the caller.
        ///
        /// # Example
        ///
        /// ```zig
        /// const std = @import("std");
        /// const cache_zig = @import("cache_zig");
        ///
        /// var cache = try cache_zig.Cache(u64).init(std.testing.allocator, .{});
        /// defer cache.deinit();
        ///
        /// var list = try cache.snapshot(std.testing.allocator);
        /// defer {
        ///     for (list.items) |ref| ref.deinit();
        ///     list.deinit(std.testing.allocator);
        /// }
        /// _ = list;
        /// ```
        pub fn snapshot(self: *Self, allocator: std.mem.Allocator) !std.ArrayList(ItemRef) {
            var list: std.ArrayList(ItemRef) = .empty;

            var tmp: std.ArrayList(*Item) = .empty;
            defer tmp.deinit(allocator);

            for (self.shards) |*shard| {
                try shard.snapshot(allocator, &tmp);
            }

            for (tmp.items) |item| {
                item.retain();
                try list.append(allocator, .{ .item = item, .allocator = self.allocator });
            }

            return list;
        }

        /// Filter items by predicate.
        ///
        /// Returned ItemRefs must be deinit'd by the caller.
        ///
        /// # Example
        ///
        /// ```zig
        /// const std = @import("std");
        /// const cache_zig = @import("cache_zig");
        ///
        /// const Pred = struct {
        ///     pub fn pred(_: *@This(), item: *cache_zig.Cache(u64).Item) bool {
        ///         return item.value == 1;
        ///     }
        /// };
        ///
        /// var cache = try cache_zig.Cache(u64).init(std.testing.allocator, .{});
        /// defer cache.deinit();
        ///
        /// var a = try cache.set("a", 1, 60 * std.time.ns_per_s);
        /// defer a.deinit();
        /// var b = try cache.set("b", 2, 60 * std.time.ns_per_s);
        /// defer b.deinit();
        ///
        /// var p = Pred{};
        /// var list = try cache.filter(std.testing.allocator, cache_zig.Cache(u64).ItemPredicate.init(&p));
        /// defer {
        ///     for (list.items) |ref| ref.deinit();
        ///     list.deinit(std.testing.allocator);
        /// }
        /// _ = list;
        /// ```
        pub fn filter(self: *Self, allocator: std.mem.Allocator, pred: ItemPredicate) !std.ArrayList(ItemRef) {
            var list: std.ArrayList(ItemRef) = .empty;

            var tmp: std.ArrayList(*Item) = .empty;
            defer tmp.deinit(allocator);

            for (self.shards) |*shard| {
                try shard.snapshot(allocator, &tmp);
            }

            for (tmp.items) |item| {
                if (pred.call(item)) {
                    item.retain();
                    try list.append(allocator, .{ .item = item, .allocator = self.allocator });
                }
            }

            return list;
        }

        fn weigh(self: *Self, key: []const u8, value: *const V) usize {
            if (self.weigher) |w| return w.call(key, value);
            return key.len + @sizeOf(V);
        }

        fn nextTick(self: *Self) u64 {
            return self.access_clock.fetchAdd(1, .acq_rel) + 1;
        }

        fn nextRand(self: *Self) u64 {
            var x = self.rng_state.load(.acquire);
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            x *%= 0x2545F4914F6CDD1D;
            self.rng_state.store(x, .release);
            return x;
        }

        fn pickEvictionCandidate(self: *Self) ?*Item {
            if (self.len() <= self.config.sample_size) {
                return switch (self.config.eviction_policy) {
                    .sampled_lhd => self.scanLeastHitDense(),
                    else => self.scanOldest(),
                };
            }

            return switch (self.config.eviction_policy) {
                .sampled_lhd => self.pickSampledLhd() orelse self.scanLeastHitDense(),
                else => self.pickSampledLru() orelse self.scanOldest(),
            };
        }

        fn pickSampledLru(self: *Self) ?*Item {
            var best: ?*Item = null;
            var best_tick: u64 = std.math.maxInt(u64);

            var i: usize = 0;
            while (i < self.config.sample_size) : (i += 1) {
                const shard = &self.shards[self.nextRand() % self.shards.len];
                const item = shard.sampleAtRetained(@intCast(self.nextRand())) orelse continue;

                const tick = item.lastAccess();
                if (best == null or tick < best_tick) {
                    if (best) |b| b.release(self.allocator);
                    best = item;
                    best_tick = tick;
                } else {
                    item.release(self.allocator);
                }
            }

            return best;
        }

        fn scanOldest(self: *Self) ?*Item {
            var best: ?*Item = null;
            var best_tick: u64 = std.math.maxInt(u64);

            for (self.shards) |*shard| {
                shard.lock.lockShared();
                for (shard.items.items) |item| {
                    const tick = item.lastAccess();
                    if (best == null or tick < best_tick) {
                        item.retain();
                        if (best) |b| b.release(self.allocator);
                        best = item;
                        best_tick = tick;
                    }
                }
                shard.lock.unlockShared();
            }

            return best;
        }

        fn pickSampledLhd(self: *Self) ?*Item {
            const now_tick = @max(self.access_clock.load(.acquire), 1);

            var best: ?*Item = null;

            var i: usize = 0;
            while (i < self.config.sample_size) : (i += 1) {
                const shard = &self.shards[self.nextRand() % self.shards.len];
                const item = shard.sampleAtRetained(@intCast(self.nextRand())) orelse continue;

                if (best == null or self.lhdIsBetterCandidate(item, best.?, now_tick)) {
                    if (best) |b| b.release(self.allocator);
                    best = item;
                } else {
                    item.release(self.allocator);
                }
            }

            return best;
        }

        fn scanLeastHitDense(self: *Self) ?*Item {
            const now_tick = @max(self.access_clock.load(.acquire), 1);
            var best: ?*Item = null;

            for (self.shards) |*shard| {
                shard.lock.lockShared();
                for (shard.items.items) |item| {
                    if (best == null or self.lhdIsBetterCandidate(item, best.?, now_tick)) {
                        item.retain();
                        if (best) |b| b.release(self.allocator);
                        best = item;
                    }
                }
                shard.lock.unlockShared();
            }

            return best;
        }

        fn lhdIsBetterCandidate(self: *Self, candidate: *Item, current: *Item, now_tick: u64) bool {
            _ = self;

            const cand_hits, const cand_denom = lhdStats(candidate, now_tick);
            const cur_hits, const cur_denom = lhdStats(current, now_tick);

            const cand_lhs: u128 = @as(u128, cand_hits) * cur_denom;
            const cur_lhs: u128 = @as(u128, cur_hits) * cand_denom;

            if (cand_lhs != cur_lhs) {
                return cand_lhs < cur_lhs;
            }

            if (candidate.weight != current.weight) {
                return candidate.weight > current.weight;
            }

            return candidate.lastAccess() < current.lastAccess();
        }

        fn lhdStats(item: *Item, now_tick: u64) struct { u64, u128 } {
            const created = item.createdTick();
            const age: u64 = @max(now_tick -| created, 1);
            const weight: u128 = @max(@as(u128, item.weight), 1);
            const denom: u128 = @as(u128, age) * weight;
            return .{ item.hitCount(), @max(denom, 1) };
        }

        fn evictIfNeeded(self: *Self) !void {
            var evicted: usize = 0;
            while (self.total_weight.load(.acquire) > self.config.max_weight and evicted < self.config.items_to_prune) {
                const candidate = self.pickEvictionCandidate() orelse return;
                defer candidate.release(self.allocator);

                candidate.markDead();

                const shard = &self.shards[_shardIndex(candidate.key, self.shard_mask)];
                if (shard.deleteIfSame(self.allocator, candidate.key, candidate)) |removed| {
                    _ = self.total_weight.fetchSub(removed.weight, .acq_rel);
                    removed.release(self.allocator); // drop shard ref
                }

                evicted += 1;
            }
        }
    };
}
