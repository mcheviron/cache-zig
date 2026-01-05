const std = @import("std");

const EvictionPolicy = @import("../eviction_policy.zig").EvictionPolicy;
const MultiThreadedConfig = @import("../config.zig").Config;
const weigher_mod = @import("../weigher.zig");

const ItemMod = @import("item.zig");
const StoreMod = @import("store.zig");

pub const Config = struct {
    max_weight: usize = 10_000,
    items_to_prune: usize = 16,
    sample_size: usize = 32,
    treat_expired_as_miss: bool = true,
    gets_per_promote: usize = 4,

    pub fn build(self: Config) Config {
        var out = self;
        out.items_to_prune = @max(out.items_to_prune, 1);
        out.sample_size = @max(out.sample_size, 1);
        out.gets_per_promote = @max(out.gets_per_promote, 1);
        return out;
    }

    pub fn fromMultiThreaded(cfg: MultiThreadedConfig) Config {
        return (Config{
            .max_weight = cfg.max_weight,
            .items_to_prune = cfg.items_to_prune,
            .sample_size = cfg.sample_size,
            .treat_expired_as_miss = cfg.treat_expired_as_miss,
            .gets_per_promote = cfg.gets_per_promote,
        }).build();
    }
};

/// Unmanaged cache implementation (does not store an allocator).
///
/// All mutating operations require an explicit `allocator`.
pub fn CacheUnmanaged(
    comptime V: type,
    comptime policy: EvictionPolicy,
    comptime Weigher: type,
) type {
    const stable_lru = policy == .stable_lru;
    const Item = ItemMod.Item(V, stable_lru);
    const Store = StoreMod.Store(Item);

    return struct {
        config: Config,
        weigher: Weigher,
        store: Store = .{},

        access_clock: u64 = 0,
        total_weight: usize = 0,
        rng_state: u64 = 0x9e3779b97f4a7c15,

        lru_list: if (stable_lru) std.DoublyLinkedList else void = if (stable_lru) .{} else {},

        /// Initializes the cache with an explicit `weigher`.
        pub fn init(allocator: std.mem.Allocator, config_in: Config, weigher: Weigher) !@This() {
            _ = allocator;
            comptime validateWeigher(V, Weigher);
            return .{ .config = config_in.build(), .weigher = weigher };
        }

        /// Releases cache-owned references.
        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            if (comptime stable_lru) {
                while (self.lru_list.pop()) |n| {
                    const item: *Item = @fieldParentPtr("lru_node", n);
                    item.promotions = 0;
                }
            }
            self.store.deinit(allocator);
        }

        /// Returns the number of cached items.
        pub fn len(self: *@This()) usize {
            return self.store.len();
        }

        /// Returns true when the cache is empty.
        pub fn isEmpty(self: *@This()) bool {
            return self.len() == 0;
        }

        /// Returns the number of cached items.
        pub fn itemCount(self: *@This()) usize {
            return self.len();
        }

        /// Returns an `ItemRef` for `key` and records a hit.
        pub fn get(self: *@This(), allocator: std.mem.Allocator, key: []const u8) ?ItemRef {
            const item = self.store.get(key) orelse return null;
            item.retain();

            if (self.config.treat_expired_as_miss and item.isExpired()) {
                item.release(allocator);
                return null;
            }

            if (item.isLive() and !item.isExpired()) {
                item.recordHit(self.nextTick());
                if (comptime stable_lru) {
                    self.maybePromote(item);
                }
            }

            return .{ .item = item, .allocator = allocator };
        }

        /// Fast-path borrowed pointer.
        ///
        /// Valid only until the next mutating operation (`set`/`replace`/`delete`/`clear`).
        pub fn getBorrowed(self: *@This(), key: []const u8) ?*const V {
            const item = self.store.get(key) orelse return null;

            if (self.config.treat_expired_as_miss and item.isExpired()) return null;

            if (item.isLive() and !item.isExpired()) {
                item.recordHit(self.nextTick());
                if (comptime stable_lru) {
                    self.maybePromote(item);
                }
            }

            return &item.value;
        }

        /// Returns an `ItemRef` for `key` without recording a hit.
        pub fn peek(self: *@This(), allocator: std.mem.Allocator, key: []const u8) ?ItemRef {
            const item = self.store.get(key) orelse return null;
            item.retain();

            if (self.config.treat_expired_as_miss and item.isExpired()) {
                item.release(allocator);
                return null;
            }

            return .{ .item = item, .allocator = allocator };
        }

        /// Inserts `key` → `value` with TTL and returns an `ItemRef`.
        pub fn set(self: *@This(), allocator: std.mem.Allocator, key: []const u8, value: V, ttl_ns: u64) !ItemRef {
            const weight = self.weigher.weigh(key, &value);
            const item = try Item.create(allocator, key, value, ttl_ns, weight);
            errdefer item.release(allocator);

            const tick = self.nextTick();
            item.setCreatedAtTick(tick);
            item.touch(tick);

            if (try self.store.put(allocator, item)) |old| {
                if (comptime stable_lru) {
                    self.unlinkLru(old);
                }

                old.markDead();
                self.total_weight -|= old.weight;
                old.release(allocator);
            }

            self.total_weight += item.weight;

            if (comptime stable_lru) {
                self.linkLruFront(item);
            }

            // Take a user-owned ref before eviction can drop the cache-owned ref.
            item.retain();
            const ref: ItemRef = .{ .item = item, .allocator = allocator };

            if (comptime policy == .stable_lru) {
                self.evictStableLru(allocator);
            } else {
                self.evictIfNeeded(allocator);
            }

            return ref;
        }

        /// Replaces an existing key’s value (preserving TTL).
        pub fn replace(self: *@This(), allocator: std.mem.Allocator, key: []const u8, value: V) !?ItemRef {
            const existing = self.peek(allocator, key) orelse return null;
            defer existing.deinit();
            const ttl = existing.ttlNs();
            return try self.set(allocator, key, value, ttl);
        }

        /// Deletes an item and returns its `ItemRef`.
        pub fn delete(self: *@This(), allocator: std.mem.Allocator, key: []const u8) ?ItemRef {
            const item = self.store.delete(key) orelse return null;

            if (comptime stable_lru) {
                self.unlinkLru(item);
            }

            item.markDead();
            self.total_weight -|= item.weight;

            return .{ .item = item, .allocator = allocator };
        }

        /// Removes all items from the cache.
        pub fn clear(self: *@This(), allocator: std.mem.Allocator) void {
            if (comptime stable_lru) {
                while (self.lru_list.pop()) |n| {
                    const item: *Item = @fieldParentPtr("lru_node", n);
                    item.promotions = 0;
                }
            }

            self.store.deinit(allocator);
            self.* = .{ .config = self.config, .weigher = self.weigher };
        }

        /// Extends an item's TTL by `ttl_ns`.
        pub fn extend(self: *@This(), allocator: std.mem.Allocator, key: []const u8, ttl_ns: u64) bool {
            const ref = self.peek(allocator, key) orelse return false;
            defer ref.deinit();

            ref.extend(ttl_ns);
            ref.item.touch(self.nextTick());
            return true;
        }

        /// Returns a snapshot of current items.
        pub fn snapshot(self: *@This(), allocator: std.mem.Allocator) !std.ArrayList(ItemRef) {
            var list: std.ArrayList(ItemRef) = .empty;
            for (self.store.items.items) |item| {
                item.retain();
                try list.append(allocator, .{ .item = item, .allocator = allocator });
            }
            return list;
        }

        /// Returns items matching `PredContext.pred(pred_ctx, key, value)`.
        pub fn filter(self: *@This(), allocator: std.mem.Allocator, comptime PredContext: type, pred_ctx: PredContext) !std.ArrayList(ItemRef) {
            comptime validateFilterPredicate(V, PredContext);
            var list: std.ArrayList(ItemRef) = .empty;

            for (self.store.items.items) |item| {
                if (PredContext.pred(pred_ctx, item.key, &item.value)) {
                    item.retain();
                    try list.append(allocator, .{ .item = item, .allocator = allocator });
                }
            }

            return list;
        }

        /// Ref-counted handle to a cache item.
        ///
        /// Always `defer ref.deinit();` for values returned by cache operations.
        pub const ItemRef = struct {
            item: *Item,
            allocator: std.mem.Allocator,

            /// Releases this reference.
            pub fn deinit(self: ItemRef) void {
                self.item.release(self.allocator);
            }

            /// Returns the owned cache key.
            pub fn key(self: ItemRef) []const u8 {
                return self.item.key;
            }

            /// Returns a pointer to the cached value.
            pub fn value(self: ItemRef) *const V {
                return &self.item.value;
            }

            /// Returns true when the item TTL has elapsed.
            pub fn isExpired(self: ItemRef) bool {
                return self.item.isExpired();
            }

            /// Returns the configured TTL for this item.
            pub fn ttlNs(self: ItemRef) u64 {
                return self.item.ttlNs();
            }

            /// Extends this item’s TTL by `ttl_ns`.
            pub fn extend(self: ItemRef, ttl_ns: u64) void {
                self.item.extend(ttl_ns);
            }
        };

        fn validateFilterPredicate(comptime V_check: type, comptime PredContext: type) void {
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
            if (p2 != *const V_check) {
                @compileError("filter() predicate value parameter must be *const V");
            }

            const ret = fn_info.return_type orelse @compileError("filter() predicate missing return type");
            if (ret != bool) {
                @compileError("filter() predicate must return bool");
            }
        }

        fn nextTick(self: *@This()) u64 {
            self.access_clock += 1;
            return self.access_clock;
        }

        fn nextRand(self: *@This()) u64 {
            var x = self.rng_state;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            x *%= 0x2545F4914F6CDD1D;
            self.rng_state = x;
            return x;
        }

        fn evictStableLru(self: *@This(), allocator: std.mem.Allocator) void {
            if (self.total_weight <= self.config.max_weight) return;

            var evicted: usize = 0;
            while (self.total_weight > self.config.max_weight and evicted < self.config.items_to_prune) : (evicted += 1) {
                const n = self.lru_list.pop() orelse return;
                const item: *Item = @fieldParentPtr("lru_node", n);

                _ = self.store.delete(item.key);
                item.markDead();
                self.total_weight -|= item.weight;
                item.release(allocator);
            }
        }

        fn evictIfNeeded(self: *@This(), allocator: std.mem.Allocator) void {
            if (self.total_weight <= self.config.max_weight) return;

            var evicted: usize = 0;
            while (self.total_weight > self.config.max_weight and evicted < self.config.items_to_prune) : (evicted += 1) {
                const victim = self.pickEvictionCandidate() orelse return;

                _ = self.store.delete(victim.key);
                victim.markDead();
                self.total_weight -|= victim.weight;
                victim.release(allocator);
            }
        }

        fn pickEvictionCandidate(self: *@This()) ?*Item {
            const n = self.len();
            if (n == 0) return null;

            if (n <= self.config.sample_size) {
                return if (comptime policy.isLhd()) self.scanLeastHitDense() else self.scanOldest();
            }

            return if (comptime policy.isLhd()) self.pickSampledLhd() else self.pickSampledLru();
        }

        fn pickSampledLru(self: *@This()) ?*Item {
            var best: ?*Item = null;
            var best_tick: u64 = std.math.maxInt(u64);

            var i: usize = 0;
            while (i < self.config.sample_size) : (i += 1) {
                const item = self.store.items.items[@intCast(self.nextRand() % self.store.items.items.len)];
                const tick = item.lastAccessTick();
                if (best == null or tick < best_tick) {
                    best = item;
                    best_tick = tick;
                }
            }

            return best;
        }

        fn scanOldest(self: *@This()) ?*Item {
            var best: ?*Item = null;
            var best_tick: u64 = std.math.maxInt(u64);

            for (self.store.items.items) |item| {
                const tick = item.lastAccessTick();
                if (best == null or tick < best_tick) {
                    best = item;
                    best_tick = tick;
                }
            }

            return best;
        }

        fn pickSampledLhd(self: *@This()) ?*Item {
            const now_tick = @max(self.access_clock, 1);
            var best: ?*Item = null;

            var i: usize = 0;
            while (i < self.config.sample_size) : (i += 1) {
                const item = self.store.items.items[@intCast(self.nextRand() % self.store.items.items.len)];
                if (best == null or self.lhdIsBetterCandidate(item, best.?, now_tick)) {
                    best = item;
                }
            }

            return best;
        }

        fn scanLeastHitDense(self: *@This()) ?*Item {
            const now_tick = @max(self.access_clock, 1);
            var best: ?*Item = null;

            for (self.store.items.items) |item| {
                if (best == null or self.lhdIsBetterCandidate(item, best.?, now_tick)) {
                    best = item;
                }
            }

            return best;
        }

        fn lhdIsBetterCandidate(self: *@This(), candidate: *Item, current: *Item, now_tick: u64) bool {
            _ = self;

            const age_cand = @as(u128, @intCast(@max(now_tick -| candidate.createdAtTick(), 1)));
            const age_cur = @as(u128, @intCast(@max(now_tick -| current.createdAtTick(), 1)));

            const weight_cand = @as(u128, @intCast(@max(candidate.weight, 1)));
            const weight_cur = @as(u128, @intCast(@max(current.weight, 1)));

            const denom_cand = age_cand * weight_cand;
            const denom_cur = age_cur * weight_cur;

            const hits_cand = @as(u128, @intCast(candidate.hitCount()));
            const hits_cur = @as(u128, @intCast(current.hitCount()));

            if (hits_cand * denom_cur != hits_cur * denom_cand) {
                return hits_cand * denom_cur < hits_cur * denom_cand;
            }

            if (candidate.weight != current.weight) {
                return candidate.weight > current.weight;
            }

            return candidate.lastAccessTick() < current.lastAccessTick();
        }

        fn linkLruFront(self: *@This(), item: *Item) void {
            if (comptime stable_lru) {
                self.lru_list.prepend(&item.lru_node);
                item.promotions = 0;
            }
        }

        fn unlinkLru(self: *@This(), item: *Item) void {
            if (comptime stable_lru) {
                if (item.lru_node.prev != null or item.lru_node.next != null or self.lru_list.first == &item.lru_node) {
                    self.lru_list.remove(&item.lru_node);
                }
                item.promotions = 0;
            }
        }

        fn maybePromote(self: *@This(), item: *Item) void {
            if (comptime stable_lru) {
                item.promotions += 1;
                const threshold = @max(self.config.gets_per_promote, 1);
                if (item.promotions >= threshold) {
                    item.promotions = 0;
                    self.lru_list.remove(&item.lru_node);
                    self.lru_list.prepend(&item.lru_node);
                }
            }
        }

        fn validateWeigher(comptime V_check: type, comptime Weigher_check: type) void {
            if (!@hasDecl(Weigher_check, "weigh")) {
                @compileError("Weigher must declare pub fn weigh(self: Weigher, key: []const u8, value: *const V) usize");
            }

            const fn_info = switch (@typeInfo(@TypeOf(Weigher_check.weigh))) {
                .@"fn" => |f| f,
                else => @compileError("Weigher.weigh must be a function"),
            };

            const params = fn_info.params;
            if (params.len != 3) {
                @compileError("Weigher.weigh must take (self, key, value)");
            }

            const p0 = params[0].type orelse @compileError("Weigher.weigh missing self type");
            if (p0 != Weigher_check) {
                @compileError("Weigher.weigh first parameter must be Weigher (pass context by value)");
            }

            const p1 = params[1].type orelse @compileError("Weigher.weigh missing key type");
            if (p1 != []const u8) {
                @compileError("Weigher.weigh key parameter must be []const u8");
            }

            const p2 = params[2].type orelse @compileError("Weigher.weigh missing value type");
            if (p2 != *const V_check) {
                @compileError("Weigher.weigh value parameter must be *const V");
            }

            const ret = fn_info.return_type orelse @compileError("Weigher.weigh missing return type");
            if (ret != usize) {
                @compileError("Weigher.weigh must return usize");
            }
        }
    };
}

/// Managed cache wrapper that stores an allocator.
///
/// This is a convenience API around `CacheUnmanaged`.
///
/// Example:
/// ```zig
/// const std = @import("std");
/// const cache_zig = @import("cache_zig");
///
/// var cache = try cache_zig.single_threaded.SampledLruCache(u64).init(std.testing.allocator, .{});
/// defer cache.deinit();
///
/// var set_ref = try cache.set("k", 123, 0);
/// defer set_ref.deinit();
///
/// var get_ref = cache.get("k") orelse return error.Miss;
/// defer get_ref.deinit();
/// ```
pub fn Cache(comptime V: type, comptime policy: EvictionPolicy, comptime Weigher: type) type {
    const Unmanaged = CacheUnmanaged(V, policy, Weigher);

    return struct {
        unmanaged: Unmanaged,
        allocator: std.mem.Allocator,

        /// Initializes the cache using the default `Weigher` (must be zero-sized).
        pub fn init(allocator: std.mem.Allocator, config_in: Config) !@This() {
            if (@sizeOf(Weigher) != 0) {
                @compileError("Weigher must be specified; call initWeigher(allocator, config, weigher)");
            }
            return initWeigher(allocator, config_in, .{});
        }

        /// Initializes the cache using an explicit `weigher`.
        pub fn initWeigher(allocator: std.mem.Allocator, config_in: Config, weigher: Weigher) !@This() {
            return .{ .unmanaged = try Unmanaged.init(allocator, config_in, weigher), .allocator = allocator };
        }

        /// Releases all cache-owned items.
        pub fn deinit(self: *@This()) void {
            self.unmanaged.deinit(self.allocator);
        }

        /// Returns an `ItemRef` for `key` and records a cache hit.
        pub fn get(self: *@This(), key: []const u8) ?Unmanaged.ItemRef {
            return self.unmanaged.get(self.allocator, key);
        }

        /// Fast-path borrowed pointer.
        ///
        /// Valid only until the next mutating operation (`set`/`replace`/`delete`/`clear`).
        pub fn getBorrowed(self: *@This(), key: []const u8) ?*const V {
            return self.unmanaged.getBorrowed(key);
        }

        /// Returns an `ItemRef` for `key` without recording a hit.
        pub fn peek(self: *@This(), key: []const u8) ?Unmanaged.ItemRef {
            return self.unmanaged.peek(self.allocator, key);
        }

        /// Inserts `key` → `value` with TTL and returns an `ItemRef`.
        pub fn set(self: *@This(), key: []const u8, value: V, ttl_ns: u64) !Unmanaged.ItemRef {
            return self.unmanaged.set(self.allocator, key, value, ttl_ns);
        }

        /// Replaces the value for an existing key (preserving TTL).
        pub fn replace(self: *@This(), key: []const u8, value: V) !?Unmanaged.ItemRef {
            return self.unmanaged.replace(self.allocator, key, value);
        }

        /// Deletes an item and returns its `ItemRef`.
        pub fn delete(self: *@This(), key: []const u8) ?Unmanaged.ItemRef {
            return self.unmanaged.delete(self.allocator, key);
        }

        /// Removes all items from the cache.
        pub fn clear(self: *@This()) void {
            self.unmanaged.clear(self.allocator);
        }

        /// Returns the number of cached items.
        pub fn itemCount(self: *@This()) usize {
            return self.unmanaged.itemCount();
        }

        /// Returns `itemCount()`.
        pub fn len(self: *@This()) usize {
            return self.unmanaged.len();
        }

        /// Returns true when the cache is empty.
        pub fn isEmpty(self: *@This()) bool {
            return self.unmanaged.isEmpty();
        }

        /// Extends an item's TTL by `ttl_ns`.
        pub fn extend(self: *@This(), key: []const u8, ttl_ns: u64) bool {
            return self.unmanaged.extend(self.allocator, key, ttl_ns);
        }

        /// Returns a snapshot of current items.
        pub fn snapshot(self: *@This(), allocator: std.mem.Allocator) !std.ArrayList(Unmanaged.ItemRef) {
            return self.unmanaged.snapshot(allocator);
        }

        /// Returns items matching `PredContext.pred(pred_ctx, key, value)`.
        pub fn filter(self: *@This(), allocator: std.mem.Allocator, comptime PredContext: type, pred_ctx: PredContext) !std.ArrayList(Unmanaged.ItemRef) {
            return self.unmanaged.filter(allocator, PredContext, pred_ctx);
        }
    };
}

pub fn SampledLruCache(comptime V: type) type {
    return Cache(V, .sampled_lru, weigher_mod.Bytes(V));
}

pub fn SampledLruCacheWithWeigher(comptime V: type, comptime Weigher: type) type {
    return Cache(V, .sampled_lru, Weigher);
}

pub fn SampledLhdCache(comptime V: type, comptime Weigher: type) type {
    return Cache(V, .sampled_lhd, Weigher);
}

pub fn StableLruCache(comptime V: type) type {
    return Cache(V, .stable_lru, weigher_mod.Bytes(V));
}

pub fn StableLruCacheWithWeigher(comptime V: type, comptime Weigher: type) type {
    return Cache(V, .stable_lru, Weigher);
}

pub fn StableLhdCache(comptime V: type, comptime Weigher: type) type {
    return Cache(V, .stable_lhd, Weigher);
}
