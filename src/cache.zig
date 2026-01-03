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
///
/// Costing:
///
/// Cost is used for both capacity (`max_weight`) and LHD ranking.
///
/// - If `Config.cost_fn` is set, it is used.
/// - Otherwise cost uses `Config.default_cost_mode`.
///
/// For LHD policies (`sampled_lhd` / `stable_lhd`), `Config.cost_fn` is required.
pub fn Cache(comptime V: type) type {
    return struct {
        const Self = @This();

        /// Internal item type.
        pub const Item = ItemMod.Item(V);
        const Shard = ShardMod.Shard(*Item);

        fn validateFilterPredicate(comptime PredContext: type) void {
            if (!@hasDecl(PredContext, "pred")) {
                @compileError("filter() predicate must declare pub fn pred(self: PredContext, key: []const u8, value: *const V) bool");
            }

            const fn_info = switch (@typeInfo(@TypeOf(PredContext.pred))) {
                .@"fn" => |f| f,
                else => @compileError("filter() predicate 'pred' must be a function"),
            };

            const params = fn_info.params;
            if (params.len != 3) {
                @compileError("filter() predicate must take (self, key, value)");
            }

            const p0 = params[0].type orelse @compileError("filter() predicate missing self type");
            if (p0 != PredContext) {
                @compileError("filter() predicate first parameter must be PredContext (pass the predicate context by value)");
            }

            const p1 = params[1].type orelse @compileError("filter() predicate missing key type");
            if (p1 != []const u8) {
                @compileError("filter() predicate key parameter must be []const u8");
            }

            const p2 = params[2].type orelse @compileError("filter() predicate missing value type");
            if (p2 != *const V) {
                @compileError("filter() predicate value parameter must be *const V");
            }

            const ret = fn_info.return_type orelse @compileError("filter() predicate missing return type");
            if (ret != bool) {
                @compileError("filter() predicate must return bool");
            }
        }

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

        const StableLru = struct {
            const Entry = struct {
                node: std.DoublyLinkedList.Node = .{},
                key: []const u8,
                item: *Item,
                promotions: usize = 0,
            };

            list: std.DoublyLinkedList = .{},
            index: std.StringHashMapUnmanaged(*Entry) = .{},

            fn deinit(self: *StableLru, allocator: std.mem.Allocator) void {
                self.clear(allocator);
                self.index.deinit(allocator);
            }

            fn clear(self: *StableLru, allocator: std.mem.Allocator) void {
                while (self.list.pop()) |n| {
                    const entry: *Entry = @fieldParentPtr("node", n);
                    _ = self.index.fetchRemove(entry.key);
                    entry.item.release(allocator);
                    allocator.destroy(entry);
                }
            }

            fn promote(self: *StableLru, allocator: std.mem.Allocator, item: *Item, gets_per_promote: usize) !void {
                const key = item.key;
                const threshold = @max(gets_per_promote, 1);

                if (self.index.get(key)) |entry| {
                    entry.item.release(allocator);
                    entry.item = item;

                    entry.promotions += 1;
                    if (entry.promotions >= threshold) {
                        entry.promotions = 0;
                        self.list.remove(&entry.node);
                        self.list.prepend(&entry.node);
                    }

                    if (entry.key.ptr != key.ptr) {
                        _ = self.index.fetchRemove(entry.key);
                        entry.key = key;
                        try self.index.put(allocator, entry.key, entry);
                    }

                    return;
                }

                const entry = try allocator.create(Entry);
                entry.* = .{ .key = key, .item = item, .promotions = 0 };

                try self.index.put(allocator, entry.key, entry);
                self.list.prepend(&entry.node);
            }

            fn removeIfSame(self: *StableLru, allocator: std.mem.Allocator, item: *Item) void {
                const entry = self.index.get(item.key) orelse return;
                if (entry.item != item) return;

                _ = self.index.fetchRemove(entry.key);
                self.list.remove(&entry.node);
                entry.item.release(allocator);
                allocator.destroy(entry);
            }

            const Pop = struct { key: []const u8, item: *Item };

            fn popLru(self: *StableLru, allocator: std.mem.Allocator) ?Pop {
                const node = self.list.pop() orelse return null;
                const entry: *Entry = @fieldParentPtr("node", node);
                _ = self.index.fetchRemove(entry.key);

                const out: Pop = .{ .key = entry.key, .item = entry.item };
                allocator.destroy(entry);
                return out;
            }
        };

        const StableWorker = struct {
            const Policy = enum { stable_lru, stable_lhd };

            const Event = union(enum) {
                promote: *Item,
                delete: *Item,
                evict: *std.Thread.ResetEvent,
                clear: *std.Thread.ResetEvent,
            };

            allocator: std.mem.Allocator,
            cache: *Self,
            policy: Policy,

            lock: std.Thread.Mutex = .{},
            cond: std.Thread.Condition = .{},
            buf: []Event,
            head: usize = 0,
            tail: usize = 0,
            len: usize = 0,
            stopping: bool = false,

            thread: std.Thread,
            lru: StableLru = .{},

            fn init(cache: *Self, policy: Policy) !*StableWorker {
                const allocator = cache.allocator;
                const cap = cache.config.promote_buffer + cache.config.delete_buffer + 8;

                const w = try allocator.create(StableWorker);
                w.* = .{
                    .allocator = allocator,
                    .cache = cache,
                    .policy = policy,
                    .buf = try allocator.alloc(Event, @max(cap, 8)),
                    .thread = undefined,
                };

                w.thread = try std.Thread.spawn(.{}, StableWorker.run, .{w});
                return w;
            }

            fn deinit(self: *StableWorker) void {
                self.lock.lock();
                self.stopping = true;
                self.cond.broadcast();
                self.lock.unlock();

                self.thread.join();

                while (self.len > 0) {
                    const ev = self._popUnsafe();
                    switch (ev) {
                        .promote => |it| it.release(self.allocator),
                        .delete => |it| it.release(self.allocator),
                        .evict => |ack| ack.set(),
                        .clear => |ack| ack.set(),
                    }
                }

                self.lru.deinit(self.allocator);
                self.allocator.free(self.buf);
                self.allocator.destroy(self);
            }

            fn tryEnqueuePromote(self: *StableWorker, item: *Item) void {
                item.retain();
                self.lock.lock();
                defer self.lock.unlock();

                if (self.stopping or self.len == self.buf.len) {
                    item.release(self.allocator);
                    return;
                }

                self._pushUnsafe(.{ .promote = item });
                self.cond.signal();
            }

            fn enqueuePromote(self: *StableWorker, item: *Item) void {
                item.retain();
                self.lock.lock();
                defer self.lock.unlock();

                while (!self.stopping and self.len == self.buf.len) {
                    self.cond.wait(&self.lock);
                }

                if (self.stopping) {
                    item.release(self.allocator);
                    return;
                }

                self._pushUnsafe(.{ .promote = item });
                self.cond.signal();
            }

            fn enqueueDelete(self: *StableWorker, item: *Item) void {
                item.retain();
                self.lock.lock();
                defer self.lock.unlock();

                while (!self.stopping and self.len == self.buf.len) {
                    self.cond.wait(&self.lock);
                }

                if (self.stopping) {
                    item.release(self.allocator);
                    return;
                }

                self._pushUnsafe(.{ .delete = item });
                self.cond.signal();
            }

            fn enqueueClear(self: *StableWorker) void {
                var ack = std.Thread.ResetEvent{};
                ack.reset();

                self.lock.lock();
                while (!self.stopping and self.len == self.buf.len) {
                    self.cond.wait(&self.lock);
                }

                if (self.stopping) {
                    self.lock.unlock();
                    return;
                }

                self._pushUnsafe(.{ .clear = &ack });
                self.cond.signal();
                self.lock.unlock();

                ack.wait();
            }

            fn enqueueEvict(self: *StableWorker) void {
                var ack = std.Thread.ResetEvent{};
                ack.reset();

                self.lock.lock();
                while (!self.stopping and self.len == self.buf.len) {
                    self.cond.wait(&self.lock);
                }

                if (self.stopping) {
                    self.lock.unlock();
                    return;
                }

                self._pushUnsafe(.{ .evict = &ack });
                self.cond.signal();
                self.lock.unlock();

                ack.wait();
            }

            fn _pushUnsafe(self: *StableWorker, ev: Event) void {
                self.buf[self.tail] = ev;
                self.tail = (self.tail + 1) % self.buf.len;
                self.len += 1;
            }

            fn _popUnsafe(self: *StableWorker) Event {
                const ev = self.buf[self.head];
                self.head = (self.head + 1) % self.buf.len;
                self.len -= 1;
                return ev;
            }

            fn run(self: *StableWorker) void {
                while (true) {
                    self.lock.lock();
                    while (!self.stopping and self.len == 0) {
                        self.cond.wait(&self.lock);
                    }

                    if (self.stopping) {
                        self.lock.unlock();
                        return;
                    }

                    const ev = self._popUnsafe();
                    self.cond.signal();
                    self.lock.unlock();

                    var run_evict = true;
                    switch (ev) {
                        .promote => |it| {
                            if (self.policy == .stable_lru) {
                                self.lru.promote(self.allocator, it, self.cache.config.gets_per_promote) catch {
                                    it.release(self.allocator);
                                };
                            } else {
                                it.release(self.allocator);
                            }
                        },
                        .delete => |it| {
                            if (self.policy == .stable_lru) {
                                self.lru.removeIfSame(self.allocator, it);
                            }
                            it.release(self.allocator);
                        },
                        .evict => |ack| {
                            run_evict = false;
                            self.evictIfNeeded();
                            ack.set();
                        },
                        .clear => |ack| {
                            self.lru.clear(self.allocator);
                            ack.set();
                        },
                    }

                    if (run_evict) {
                        self.evictIfNeeded();
                    }
                }
            }

            fn evictIfNeeded(self: *StableWorker) void {
                var evicted: usize = 0;
                while (self.cache.total_weight.load(.acquire) > self.cache.config.max_weight and evicted < self.cache.config.items_to_prune) {
                    switch (self.policy) {
                        .stable_lru => {
                            const pop = self.lru.popLru(self.allocator) orelse return;
                            defer pop.item.release(self.allocator);

                            pop.item.markDead();

                            const shard = &self.cache.shards[_shardIndex(pop.key, self.cache.shard_mask)];
                            if (shard.deleteIfSame(self.allocator, pop.key, pop.item)) |removed| {
                                _ = self.cache.total_weight.fetchSub(removed.weight, .acq_rel);
                                removed.release(self.allocator);
                            }
                        },
                        .stable_lhd => {
                            const candidate = self.cache.scanLeastHitDense() orelse return;
                            defer candidate.release(self.allocator);

                            candidate.markDead();

                            const shard = &self.cache.shards[_shardIndex(candidate.key, self.cache.shard_mask)];
                            if (shard.deleteIfSame(self.allocator, candidate.key, candidate)) |removed| {
                                _ = self.cache.total_weight.fetchSub(removed.weight, .acq_rel);
                                removed.release(self.allocator);
                            }
                        },
                    }

                    evicted += 1;
                }
            }
        };

        const Eviction = union(enum) {
            sampled_lru,
            sampled_lhd,
            stable_lru: ?*StableWorker,
            stable_lhd: ?*StableWorker,

            fn init(_: *Self, policy: Config.EvictionPolicy) !Eviction {
                return switch (policy) {
                    .sampled_lhd => .sampled_lhd,
                    .stable_lru => .{ .stable_lru = null },
                    .stable_lhd => .{ .stable_lhd = null },
                    else => .sampled_lru,
                };
            }

            fn deinit(self: *Eviction) void {
                switch (self.*) {
                    .stable_lru => |w_opt| if (w_opt) |w| w.deinit(),
                    .stable_lhd => |w_opt| if (w_opt) |w| w.deinit(),
                    else => {},
                }
            }

            fn ensureStarted(self: *Eviction, cache: *Self) !void {
                switch (self.*) {
                    .stable_lru => |*w_opt| {
                        if (w_opt.* == null) {
                            w_opt.* = try StableWorker.init(cache, .stable_lru);
                        }
                    },
                    .stable_lhd => |*w_opt| {
                        if (w_opt.* == null) {
                            w_opt.* = try StableWorker.init(cache, .stable_lhd);
                        }
                    },
                    else => {},
                }
            }

            fn enqueueEvict(self: *Eviction) void {
                switch (self.*) {
                    .stable_lru => |w_opt| if (w_opt) |w| w.enqueueEvict(),
                    .stable_lhd => |w_opt| if (w_opt) |w| w.enqueueEvict(),
                    else => {},
                }
            }

            fn onGet(self: *Eviction, item: *Item) void {
                switch (self.*) {
                    .stable_lru => |w_opt| if (w_opt) |w| w.tryEnqueuePromote(item),
                    .stable_lhd => |w_opt| if (w_opt) |w| w.tryEnqueuePromote(item),
                    else => {},
                }
            }

            fn onSet(self: *Eviction, item: *Item) void {
                switch (self.*) {
                    .stable_lru => |w_opt| if (w_opt) |w| w.enqueuePromote(item),
                    .stable_lhd => |w_opt| if (w_opt) |w| w.enqueuePromote(item),
                    else => {},
                }
            }

            fn onDelete(self: *Eviction, item: *Item) void {
                switch (self.*) {
                    .stable_lru => |w_opt| if (w_opt) |w| w.enqueueDelete(item),
                    .stable_lhd => |w_opt| if (w_opt) |w| w.enqueueDelete(item),
                    else => {},
                }
            }

            fn onClear(self: *Eviction) void {
                switch (self.*) {
                    .stable_lru => |w_opt| if (w_opt) |w| w.enqueueClear(),
                    .stable_lhd => |w_opt| if (w_opt) |w| w.enqueueClear(),
                    else => {},
                }
            }

            fn isStable(self: Eviction) bool {
                return switch (self) {
                    .stable_lru, .stable_lhd => true,
                    else => false,
                };
            }
        };

        allocator: std.mem.Allocator,
        config: Config,
        shard_mask: usize,
        shards: []Shard,

        access_clock: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
        total_weight: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        rng_state: std.atomic.Value(u64) = std.atomic.Value(u64).init(0x9e3779b97f4a7c15),

        // Costing lives in `self.config`.

        eviction: Eviction,

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
        pub const InitError = std.mem.Allocator.Error;

        pub fn init(allocator: std.mem.Allocator, comptime config_in: Config) InitError!Self {
            comptime var cfg = config_in;

            comptime {
                if (cfg.shards == 0 or (cfg.shards & (cfg.shards - 1)) != 0) {
                    @compileError("Config.shards must be a power of two > 0");
                }

                cfg.items_to_prune = @max(cfg.items_to_prune, 1);
                cfg.sample_size = @max(cfg.sample_size, 1);
                cfg.gets_per_promote = @max(cfg.gets_per_promote, 1);
                cfg.promote_buffer = @max(cfg.promote_buffer, 1);
                cfg.delete_buffer = @max(cfg.delete_buffer, 1);

                if ((cfg.eviction_policy == .sampled_lhd or cfg.eviction_policy == .stable_lhd) and cfg.cost_fn == null) {
                    @compileError("LHD eviction policies require Config.cost_fn");
                }
            }

            const shards = try allocator.alloc(Shard, cfg.shards);
            for (shards) |*s| s.* = .{};

            var c = Self{
                .allocator = allocator,
                .config = cfg,
                .shard_mask = cfg.shards - 1,
                .shards = shards,
                .eviction = undefined,
            };

            c.eviction = try Eviction.init(&c, cfg.eviction_policy);
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
            self.eviction.deinit();

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
                self.eviction.onGet(item);
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
            item.setCreatedAtTick(tick);
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
            if (self.eviction.isStable()) {
                try self.eviction.ensureStarted(self);
            }
            self.eviction.onSet(item);
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

            self.eviction.onDelete(item);

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
            self.eviction.onClear();
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
        ///     needle: []const u8,
        ///
        ///     pub fn pred(self: @This(), key: []const u8, _: *const u64) bool {
        ///         return std.mem.eql(u8, key, self.needle);
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
        /// var list = try cache.filter(std.testing.allocator, Pred, Pred{ .needle = "a" });
        /// defer {
        ///     for (list.items) |ref| ref.deinit();
        ///     list.deinit(std.testing.allocator);
        /// }
        /// _ = list;
        /// ```
        pub fn filter(self: *Self, allocator: std.mem.Allocator, comptime PredContext: type, pred_ctx: PredContext) !std.ArrayList(ItemRef) {
            comptime Self.validateFilterPredicate(PredContext);
            var list: std.ArrayList(ItemRef) = .empty;

            var tmp: std.ArrayList(*Item) = .empty;
            defer tmp.deinit(allocator);

            for (self.shards) |*shard| {
                try shard.snapshot(allocator, &tmp);
            }

            for (tmp.items) |item| {
                if (PredContext.pred(pred_ctx, item.key, &item.value)) {
                    item.retain();
                    try list.append(allocator, .{ .item = item, .allocator = self.allocator });
                }
            }

            return list;
        }

        fn weigh(self: *Self, key: []const u8, value: *const V) usize {
            if (self.config.cost_fn) |f| {
                return f(self.config.cost_ctx, key, @ptrCast(value));
            }

            return switch (self.config.default_cost_mode) {
                .bytes => key.len + @sizeOf(V),
                .items => 1,
            };
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
                return switch (self.eviction) {
                    .sampled_lhd => self.scanLeastHitDense(),
                    else => self.scanOldest(),
                };
            }

            return switch (self.eviction) {
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

                const tick = item.lastAccessTick();
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
                    const tick = item.lastAccessTick();
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

            return candidate.lastAccessTick() < current.lastAccessTick();
        }

        fn lhdStats(item: *Item, now_tick: u64) struct { u64, u128 } {
            const created = item.createdAtTick();
            const age: u64 = @max(now_tick -| created, 1);
            const weight: u128 = @max(@as(u128, item.weight), 1);
            const denom: u128 = @as(u128, age) * weight;
            return .{ item.hitCount(), @max(denom, 1) };
        }

        fn evictIfNeeded(self: *Self) !void {
            if (self.eviction.isStable()) {
                try self.eviction.ensureStarted(self);
                self.eviction.enqueueEvict();
                return;
            }

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
