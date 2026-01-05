const std = @import("std");

const Config = @import("../config.zig").Config;
const EvictionPolicy = @import("../eviction_policy.zig").EvictionPolicy;
const weigher_mod = @import("../weigher.zig");
const ItemMod = @import("item.zig");
const ShardMod = @import("shard.zig");

fn shardIndex(key: []const u8, mask: usize) usize {
    var h = std.hash.Fnv1a_64.init();
    h.update(key);
    const digest = h.final();
    return @as(usize, @intCast(digest)) & mask;
}

fn StableWorker(comptime CacheT: type, comptime stable_policy: EvictionPolicy) type {
    if (stable_policy != .stable_lru and stable_policy != .stable_lhd) {
        @compileError("StableWorker requires a stable eviction policy");
    }

    return struct {
        const Self = @This();
        const Item = CacheT.Item;

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

        const Event = union(enum) {
            promote: *Item,
            delete: *Item,
            evict: *std.Thread.ResetEvent,
            clear: *std.Thread.ResetEvent,
        };

        allocator: std.mem.Allocator,
        cache: *CacheT,

        lock: std.Thread.Mutex = .{},
        cond: std.Thread.Condition = .{},
        buf: []Event,
        head: usize = 0,
        tail: usize = 0,
        len: usize = 0,
        stopping: bool = false,

        thread: std.Thread,
        lru: StableLru = .{},

        fn init(allocator: std.mem.Allocator, cache: *CacheT) !*Self {
            const cap = cache.config.promote_buffer + cache.config.delete_buffer + 8;

            const w = try allocator.create(Self);
            w.* = .{
                .allocator = allocator,
                .cache = cache,
                .buf = try allocator.alloc(Event, @max(cap, 8)),
                .thread = undefined,
            };

            w.thread = try std.Thread.spawn(.{}, Self.run, .{w});
            return w;
        }

        fn deinit(self: *Self) void {
            self.lock.lock();
            self.stopping = true;
            self.cond.broadcast();
            self.lock.unlock();

            self.thread.join();

            while (self.len > 0) {
                const ev = self.popUnsafe();
                switch (ev) {
                    .promote => |it| it.release(self.allocator),
                    .delete => |it| it.release(self.allocator),
                    .evict => |ack| ack.set(),
                    .clear => |ack| ack.set(),
                }
            }

            if (comptime stable_policy == .stable_lru) {
                self.lru.deinit(self.allocator);
            }
            self.allocator.free(self.buf);
            self.allocator.destroy(self);
        }

        fn tryEnqueuePromote(self: *Self, item: *Item) void {
            item.retain();
            self.lock.lock();
            defer self.lock.unlock();

            if (self.stopping or self.len == self.buf.len) {
                item.release(self.allocator);
                return;
            }

            self.pushUnsafe(.{ .promote = item });
            self.cond.signal();
        }

        fn enqueuePromote(self: *Self, item: *Item) void {
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

            self.pushUnsafe(.{ .promote = item });
            self.cond.signal();
        }

        fn enqueueDelete(self: *Self, item: *Item) void {
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

            self.pushUnsafe(.{ .delete = item });
            self.cond.signal();
        }

        fn enqueueClear(self: *Self) void {
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

            self.pushUnsafe(.{ .clear = &ack });
            self.cond.signal();
            self.lock.unlock();

            ack.wait();
        }

        fn enqueueEvict(self: *Self) void {
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

            self.pushUnsafe(.{ .evict = &ack });
            self.cond.signal();
            self.lock.unlock();

            ack.wait();
        }

        fn pushUnsafe(self: *Self, ev: Event) void {
            self.buf[self.tail] = ev;
            self.tail = (self.tail + 1) % self.buf.len;
            self.len += 1;
        }

        fn popUnsafe(self: *Self) Event {
            const ev = self.buf[self.head];
            self.head = (self.head + 1) % self.buf.len;
            self.len -= 1;
            return ev;
        }

        fn run(self: *Self) void {
            while (true) {
                self.lock.lock();
                while (!self.stopping and self.len == 0) {
                    self.cond.wait(&self.lock);
                }

                if (self.stopping) {
                    self.lock.unlock();
                    return;
                }

                const ev = self.popUnsafe();
                self.cond.signal();
                self.lock.unlock();

                var run_evict = true;
                switch (ev) {
                    .promote => |it| {
                        if (comptime stable_policy == .stable_lru) {
                            self.lru.promote(self.allocator, it, self.cache.config.gets_per_promote) catch {
                                it.release(self.allocator);
                            };
                        } else {
                            it.release(self.allocator);
                        }
                    },
                    .delete => |it| {
                        if (comptime stable_policy == .stable_lru) {
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
                        if (comptime stable_policy == .stable_lru) {
                            self.lru.clear(self.allocator);
                        }
                        ack.set();
                    },
                }

                if (run_evict) {
                    self.evictIfNeeded();
                }
            }
        }

        fn evictIfNeeded(self: *Self) void {
            var evicted: usize = 0;
            while (self.cache.total_weight.load(.acquire) > self.cache.config.max_weight and evicted < self.cache.config.items_to_prune) {
                if (comptime stable_policy == .stable_lru) {
                    const pop = self.lru.popLru(self.allocator) orelse return;
                    defer pop.item.release(self.allocator);

                    pop.item.markDead();

                    const shard = &self.cache.shards[shardIndex(pop.key, self.cache.shard_mask)];
                    if (shard.deleteIfSame(self.allocator, pop.key, pop.item)) |removed| {
                        _ = self.cache.total_weight.fetchSub(removed.weight, .acq_rel);
                        removed.release(self.allocator);
                    }
                } else {
                    const candidate = self.cache.scanLeastHitDense(self.allocator) orelse return;
                    defer candidate.release(self.allocator);

                    candidate.markDead();

                    const shard = &self.cache.shards[shardIndex(candidate.key, self.cache.shard_mask)];
                    if (shard.deleteIfSame(self.allocator, candidate.key, candidate)) |removed| {
                        _ = self.cache.total_weight.fetchSub(removed.weight, .acq_rel);
                        removed.release(self.allocator);
                    }
                }

                evicted += 1;
            }
        }
    };
}

pub fn CacheUnmanaged(
    comptime V: type,
    comptime policy: EvictionPolicy,
    comptime Weigher: type,
) type {
    const ItemT = ItemMod.Item(V);
    const ShardT = ShardMod.Shard(*ItemT);

    return struct {
        config: Config,
        weigher: Weigher,
        shard_mask: usize,
        shards: []ShardT,

        access_clock: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
        total_weight: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        rng_state: std.atomic.Value(u64) = std.atomic.Value(u64).init(0x9e3779b97f4a7c15),

        worker: if (policy.isStable()) ?*StableWorker(@This(), policy) else void = if (policy.isStable()) null else {},

        pub fn init(allocator: std.mem.Allocator, config_in: Config, weigher: Weigher) InitError!@This() {
            comptime validateWeigher(V, Weigher);

            const cfg = try config_in.build();

            const shards = try allocator.alloc(ShardT, cfg.shard_count);
            for (shards) |*s| s.* = .{};

            return .{
                .config = cfg,
                .weigher = weigher,
                .shard_mask = cfg.shard_count - 1,
                .shards = shards,
            };
        }

        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            if (comptime policy.isStable()) {
                if (self.worker) |w| w.deinit();
            }

            for (self.shards) |*shard| {
                shard.lock.lock();
                defer shard.lock.unlock();

                for (shard.items.items) |item| {
                    item.markDead();
                    item.release(allocator);
                }
                shard.map.deinit(allocator);
                shard.items.deinit(allocator);
                shard.pos.deinit(allocator);
            }
            allocator.free(self.shards);
        }

        pub fn get(self: *@This(), allocator: std.mem.Allocator, key: []const u8) ?ItemRef {
            const shard = &self.shards[shardIndex(key, self.shard_mask)];
            const item = shard.get(key) orelse return null;

            item.retain();

            if (self.config.treat_expired_as_miss and item.isExpired()) {
                item.release(allocator);
                return null;
            }

            if (item.isLive() and !item.isExpired()) {
                item.recordHit(self.nextTick());
                if (comptime policy.isStable()) {
                    if (self.worker) |w| w.tryEnqueuePromote(item);
                }
            }

            return .{ .item = item, .allocator = allocator };
        }

        pub fn peek(self: *@This(), allocator: std.mem.Allocator, key: []const u8) ?ItemRef {
            const shard = &self.shards[shardIndex(key, self.shard_mask)];
            const item = shard.get(key) orelse return null;
            item.retain();

            if (self.config.treat_expired_as_miss and item.isExpired()) {
                item.release(allocator);
                return null;
            }

            return .{ .item = item, .allocator = allocator };
        }

        pub fn set(self: *@This(), allocator: std.mem.Allocator, key: []const u8, value: V, ttl_ns: u64) !ItemRef {
            const weight = self.weigh(key, &value);
            const item = try ItemT.create(allocator, key, value, ttl_ns, weight);
            const tick = self.nextTick();
            item.setCreatedAtTick(tick);
            item.touch(tick);

            // Shard owns one ref.
            item.retain();

            const shard = &self.shards[shardIndex(key, self.shard_mask)];
            if (try shard.set(allocator, item.key, item)) |old| {
                old.markDead();
                _ = self.total_weight.fetchSub(old.weight, .acq_rel);
                old.release(allocator);
            }

            _ = self.total_weight.fetchAdd(item.weight, .acq_rel);

            if (comptime policy.isStable()) {
                try self.ensureWorkerStarted(allocator);
                self.worker.?.enqueuePromote(item);
                self.worker.?.enqueueEvict();
            } else {
                try self.evictIfNeeded(allocator);
            }

            return .{ .item = item, .allocator = allocator };
        }

        pub fn replace(self: *@This(), allocator: std.mem.Allocator, key: []const u8, value: V) !?ItemRef {
            const existing = self.peekRaw(allocator, key) orelse return null;
            defer existing.deinit();

            const ttl = existing.ttlNs();
            return try self.set(allocator, key, value, ttl);
        }

        pub fn delete(self: *@This(), allocator: std.mem.Allocator, key: []const u8) ?ItemRef {
            const shard = &self.shards[shardIndex(key, self.shard_mask)];
            const item = shard.delete(allocator, key) orelse return null;

            if (comptime policy.isStable()) {
                if (self.worker) |w| w.enqueueDelete(item);
            }

            item.markDead();
            self.total_weight.store(self.total_weight.load(.acquire) -| item.weight, .release);

            return .{ .item = item, .allocator = allocator };
        }

        pub fn clear(self: *@This(), allocator: std.mem.Allocator) void {
            if (comptime policy.isStable()) {
                if (self.worker) |w| w.enqueueClear();
            }

            for (self.shards) |*shard| {
                shard.clear(allocator);
            }
            self.total_weight.store(0, .release);
        }

        pub fn itemCount(self: *@This()) usize {
            var n: usize = 0;
            for (self.shards) |*s| {
                n += s.len();
            }
            return n;
        }

        pub fn len(self: *@This()) usize {
            return self.itemCount();
        }

        pub fn isEmpty(self: *@This()) bool {
            return self.len() == 0;
        }

        pub fn extend(self: *@This(), allocator: std.mem.Allocator, key: []const u8, ttl_ns: u64) bool {
            const ref = self.peekRaw(allocator, key) orelse return false;
            defer ref.deinit();

            ref.extend(ttl_ns);
            ref.item.touch(self.nextTick());
            return true;
        }

        fn peekRaw(self: *@This(), allocator: std.mem.Allocator, key: []const u8) ?ItemRef {
            const shard = &self.shards[shardIndex(key, self.shard_mask)];
            const item = shard.get(key) orelse return null;
            item.retain();
            return .{ .item = item, .allocator = allocator };
        }

        pub fn snapshot(self: *@This(), allocator: std.mem.Allocator) !std.ArrayList(ItemRef) {
            var list: std.ArrayList(ItemRef) = .empty;

            var tmp: std.ArrayList(*ItemT) = .empty;
            defer tmp.deinit(allocator);

            for (self.shards) |*shard| {
                try shard.snapshot(allocator, &tmp);
            }

            for (tmp.items) |item| {
                item.retain();
                try list.append(allocator, .{ .item = item, .allocator = allocator });
            }

            return list;
        }

        pub fn filter(self: *@This(), allocator: std.mem.Allocator, comptime PredContext: type, pred_ctx: PredContext) !std.ArrayList(ItemRef) {
            comptime validateFilterPredicate(PredContext);
            var list: std.ArrayList(ItemRef) = .empty;

            var tmp: std.ArrayList(*ItemT) = .empty;
            defer tmp.deinit(allocator);

            for (self.shards) |*shard| {
                try shard.snapshot(allocator, &tmp);
            }

            for (tmp.items) |item| {
                if (PredContext.pred(pred_ctx, item.key, &item.value)) {
                    item.retain();
                    try list.append(allocator, .{ .item = item, .allocator = allocator });
                }
            }

            return list;
        }

        pub const InitError = std.mem.Allocator.Error || Config.BuildError;

        pub const Item = ItemT;

        /// Ref-counted handle to a cache item.
        ///
        /// Always `defer ref.deinit();` for values returned by cache operations.
        pub const ItemRef = struct {
            item: *ItemT,
            allocator: std.mem.Allocator,

            pub fn deinit(self: ItemRef) void {
                self.item.release(self.allocator);
            }

            pub fn key(self: ItemRef) []const u8 {
                return self.item.key;
            }

            pub fn value(self: ItemRef) *const V {
                return &self.item.value;
            }

            pub fn isExpired(self: ItemRef) bool {
                return self.item.isExpired();
            }

            pub fn ttlNs(self: ItemRef) u64 {
                return self.item.ttlNs();
            }

            pub fn extend(self: ItemRef, ttl_ns: u64) void {
                self.item.extend(ttl_ns);
            }
        };

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

        fn weigh(self: *@This(), key: []const u8, value: *const V) usize {
            return self.weigher.weigh(key, value);
        }

        fn ensureWorkerStarted(self: *@This(), allocator: std.mem.Allocator) !void {
            if (comptime policy.isStable()) {
                if (self.worker == null) {
                    self.worker = try StableWorker(@This(), policy).init(allocator, self);
                }
            }
        }

        fn nextTick(self: *@This()) u64 {
            return self.access_clock.fetchAdd(1, .acq_rel) + 1;
        }

        fn nextRand(self: *@This()) u64 {
            var x = self.rng_state.load(.acquire);
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            x *%= 0x2545F4914F6CDD1D;
            self.rng_state.store(x, .release);
            return x;
        }

        fn pickEvictionCandidate(self: *@This(), allocator: std.mem.Allocator) ?*ItemT {
            if (self.len() <= self.config.sample_size) {
                return if (comptime policy.isLhd())
                    self.scanLeastHitDense(allocator)
                else
                    self.scanOldest(allocator);
            }

            return if (comptime policy.isLhd())
                (self.pickSampledLhd(allocator) orelse self.scanLeastHitDense(allocator))
            else
                (self.pickSampledLru(allocator) orelse self.scanOldest(allocator));
        }

        fn pickSampledLru(self: *@This(), allocator: std.mem.Allocator) ?*ItemT {
            var best: ?*Item = null;
            var best_tick: u64 = std.math.maxInt(u64);

            var i: usize = 0;
            while (i < self.config.sample_size) : (i += 1) {
                const shard = &self.shards[self.nextRand() % self.shards.len];
                const item = shard.sampleAtRetained(@intCast(self.nextRand())) orelse continue;

                const tick = item.lastAccessTick();
                if (best == null or tick < best_tick) {
                    if (best) |b| b.release(allocator);
                    best = item;
                    best_tick = tick;
                } else {
                    item.release(allocator);
                }
            }

            return best;
        }

        fn scanOldest(self: *@This(), allocator: std.mem.Allocator) ?*ItemT {
            var best: ?*Item = null;
            var best_tick: u64 = std.math.maxInt(u64);

            for (self.shards) |*shard| {
                shard.lock.lockShared();
                for (shard.items.items) |item| {
                    const tick = item.lastAccessTick();
                    if (best == null or tick < best_tick) {
                        item.retain();
                        if (best) |b| b.release(allocator);
                        best = item;
                        best_tick = tick;
                    }
                }
                shard.lock.unlockShared();
            }

            return best;
        }

        fn pickSampledLhd(self: *@This(), allocator: std.mem.Allocator) ?*ItemT {
            const now_tick = @max(self.access_clock.load(.acquire), 1);

            var best: ?*Item = null;

            var i: usize = 0;
            while (i < self.config.sample_size) : (i += 1) {
                const shard = &self.shards[self.nextRand() % self.shards.len];
                const item = shard.sampleAtRetained(@intCast(self.nextRand())) orelse continue;

                if (best == null or self.lhdIsBetterCandidate(item, best.?, now_tick)) {
                    if (best) |b| b.release(allocator);
                    best = item;
                } else {
                    item.release(allocator);
                }
            }

            return best;
        }

        fn scanLeastHitDense(self: *@This(), allocator: std.mem.Allocator) ?*ItemT {
            const now_tick = @max(self.access_clock.load(.acquire), 1);
            var best: ?*Item = null;

            for (self.shards) |*shard| {
                shard.lock.lockShared();
                for (shard.items.items) |item| {
                    if (best == null or self.lhdIsBetterCandidate(item, best.?, now_tick)) {
                        item.retain();
                        if (best) |b| b.release(allocator);
                        best = item;
                    }
                }
                shard.lock.unlockShared();
            }

            return best;
        }

        fn lhdIsBetterCandidate(self: *@This(), candidate: *ItemT, current: *ItemT, now_tick: u64) bool {
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

        fn evictIfNeeded(self: *@This(), allocator: std.mem.Allocator) !void {
            var evicted: usize = 0;
            while (self.total_weight.load(.acquire) > self.config.max_weight and evicted < self.config.items_to_prune) {
                const candidate = self.pickEvictionCandidate(allocator) orelse return;
                defer candidate.release(allocator);

                candidate.markDead();

                const shard = &self.shards[shardIndex(candidate.key, self.shard_mask)];
                if (shard.deleteIfSame(allocator, candidate.key, candidate)) |removed| {
                    _ = self.total_weight.fetchSub(removed.weight, .acq_rel);
                    removed.release(allocator);
                }

                evicted += 1;
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
/// var cache = try cache_zig.multi_threaded.SampledLruCache(u64).init(std.testing.allocator, .{});
/// defer cache.deinit();
///
/// var set_ref = try cache.set("k", 123, 0);
/// defer set_ref.deinit();
///
/// var get_ref = cache.get("k") orelse return error.Miss;
/// defer get_ref.deinit();
/// ```
pub fn Cache(
    comptime V: type,
    comptime policy: EvictionPolicy,
    comptime Weigher: type,
) type {
    const Unmanaged = CacheUnmanaged(V, policy, Weigher);

    return struct {
        unmanaged: Unmanaged,
        allocator: std.mem.Allocator,

        /// Initializes the cache using the default `Weigher` (must be zero-sized).
        pub fn init(allocator: std.mem.Allocator, config_in: Config) Unmanaged.InitError!@This() {
            if (@sizeOf(Weigher) != 0) {
                @compileError("Weigher must be specified; call initWeigher(allocator, config, weigher)");
            }
            return initWeigher(allocator, config_in, .{});
        }

        /// Initializes the cache using an explicit `weigher`.
        pub fn initWeigher(allocator: std.mem.Allocator, config_in: Config, weigher: Weigher) Unmanaged.InitError!@This() {
            return .{
                .unmanaged = try Unmanaged.init(allocator, config_in, weigher),
                .allocator = allocator,
            };
        }

        /// Releases all cache-owned items.
        pub fn deinit(self: *@This()) void {
            self.unmanaged.deinit(self.allocator);
        }

        /// Returns an `ItemRef` for `key` and records a cache hit.
        pub fn get(self: *@This(), key: []const u8) ?Unmanaged.ItemRef {
            return self.unmanaged.get(self.allocator, key);
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
            var list: std.ArrayList(Unmanaged.ItemRef) = .empty;

            var tmp: std.ArrayList(*Unmanaged.Item) = .empty;
            defer tmp.deinit(allocator);

            for (self.unmanaged.shards) |*shard| {
                try shard.snapshot(allocator, &tmp);
            }

            for (tmp.items) |item| {
                item.retain();
                try list.append(allocator, .{ .item = item, .allocator = self.allocator });
            }

            return list;
        }

        /// Returns items matching `PredContext.pred(pred_ctx, key, value)`.
        pub fn filter(self: *@This(), allocator: std.mem.Allocator, comptime PredContext: type, pred_ctx: PredContext) !std.ArrayList(Unmanaged.ItemRef) {
            comptime Unmanaged.validateFilterPredicate(PredContext);
            var list: std.ArrayList(Unmanaged.ItemRef) = .empty;

            var tmp: std.ArrayList(*Unmanaged.Item) = .empty;
            defer tmp.deinit(allocator);

            for (self.unmanaged.shards) |*shard| {
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
