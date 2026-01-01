const std = @import("std");

const Config = @import("config.zig").Config;
const ItemMod = @import("item.zig");
const ShardMod = @import("shard.zig");

fn shardIndex(key: []const u8, mask: usize) usize {
    var h = std.hash.Fnv1a_64.init();
    h.update(key);
    const digest = h.final();
    return @as(usize, @intCast(digest)) & mask;
}

pub fn Cache(comptime V: type) type {
    return struct {
        const Self = @This();

        pub const Item = ItemMod.Item(V);
        const Shard = ShardMod.Shard(*Item);

        pub const Weigher = struct {
            ctx: *anyopaque,
            callFn: *const fn (ctx: *anyopaque, key: []const u8, value: *const V) usize,

            pub fn call(self: Weigher, key: []const u8, value: *const V) usize {
                return self.callFn(self.ctx, key, value);
            }
        };

        pub const ItemPredicate = struct {
            ctx: *anyopaque,
            callFn: *const fn (ctx: *anyopaque, item: *Item) bool,

            pub fn call(self: ItemPredicate, item: *Item) bool {
                return self.callFn(self.ctx, item);
            }

            pub fn init(ptr: anytype) ItemPredicate {
                const T = @TypeOf(ptr);
                const ti = @typeInfo(T);
                if (ti != .pointer or ti.pointer.size != .one) {
                    @compileError("ItemPredicate.init expects a single-item pointer");
                }

                const Gen = struct {
                    fn call(ctx: *anyopaque, item: *Item) bool {
                        const self_ptr: T = @ptrCast(@alignCast(ctx));
                        return ti.pointer.child.pred(self_ptr, item);
                    }
                };

                return .{ .ctx = ptr, .callFn = Gen.call };
            }
        };

        pub const ItemRef = struct {
            item: *Item,
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

        allocator: std.mem.Allocator,
        config: Config,
        shard_mask: usize,
        shards: []Shard,
        access_clock: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
        total_weight: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        rng_state: std.atomic.Value(u64) = std.atomic.Value(u64).init(0x9e3779b97f4a7c15),
        weigher: ?Weigher,

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

        pub fn initWithWeigher(allocator: std.mem.Allocator, config_in: Config, weigher: Weigher) !Self {
            var c = try Self.init(allocator, config_in);
            c.weigher = weigher;
            return c;
        }

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

        pub fn get(self: *Self, key: []const u8) ?ItemRef {
            const shard = &self.shards[shardIndex(key, self.shard_mask)];
            const item = shard.get(key) orelse return null;

            item.retain();

            if (item.isLive() and !item.isExpired()) {
                item.touch(self.nextTick());
            }

            return .{ .item = item, .allocator = self.allocator };
        }

        pub fn peek(self: *Self, key: []const u8) ?ItemRef {
            const shard = &self.shards[shardIndex(key, self.shard_mask)];
            const item = shard.get(key) orelse return null;
            item.retain();
            return .{ .item = item, .allocator = self.allocator };
        }

        pub fn set(self: *Self, key: []const u8, value: V, ttl_ns: u64) !ItemRef {
            const weight = self.weigh(key, &value);
            const item = try Item.create(self.allocator, key, value, ttl_ns, weight);
            item.touch(self.nextTick());

            // Shard owns one ref.
            item.retain();

            const shard = &self.shards[shardIndex(key, self.shard_mask)];
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

        pub fn replace(self: *Self, key: []const u8, value: V) !?ItemRef {
            const existing = self.peek(key) orelse return null;
            defer existing.deinit();

            const ttl = existing.ttlNs();
            return try self.set(key, value, ttl);
        }

        pub fn delete(self: *Self, key: []const u8) ?ItemRef {
            const shard = &self.shards[shardIndex(key, self.shard_mask)];
            const item = shard.delete(self.allocator, key) orelse return null;

            item.markDead();
            self.total_weight.store(self.total_weight.load(.acquire) -| item.weight, .release);

            // Transfer shard ref to caller.
            return .{ .item = item, .allocator = self.allocator };
        }

        pub fn clear(self: *Self) void {
            for (self.shards) |*shard| {
                shard.clear(self.allocator);
            }
            self.total_weight.store(0, .release);
        }

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

        fn itemCount(self: *Self) usize {
            var n: usize = 0;
            for (self.shards) |*s| {
                n += s.len();
            }
            return n;
        }

        fn pickEvictionCandidate(self: *Self) ?*Item {
            if (self.itemCount() <= self.config.sample_size) {
                return self.scanOldest();
            }

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

            return best orelse self.scanOldest();
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

        fn evictIfNeeded(self: *Self) !void {
            var evicted: usize = 0;
            while (self.total_weight.load(.acquire) > self.config.max_weight and evicted < self.config.items_to_prune) {
                const candidate = self.pickEvictionCandidate() orelse return;
                defer candidate.release(self.allocator);

                candidate.markDead();

                const shard = &self.shards[shardIndex(candidate.key, self.shard_mask)];
                if (shard.deleteIfSame(self.allocator, candidate.key, candidate)) |removed| {
                    _ = self.total_weight.fetchSub(removed.weight, .acq_rel);
                    removed.release(self.allocator); // drop shard ref
                }

                evicted += 1;
            }
        }
    };
}
