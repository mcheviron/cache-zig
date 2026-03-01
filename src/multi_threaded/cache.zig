const std = @import("std");

const Config = @import("../config.zig").Config;
const Policy = @import("../policy.zig").Policy;
const weigher_mod = @import("../weigher.zig");
const FrequencySketch = @import("../frequency_sketch.zig").FrequencySketch;
const key_context = @import("../key_context.zig");
const key_ops = @import("../key_ops.zig");
const ItemMod = @import("item.zig");
const ShardMod = @import("shard.zig");

fn shardIndex(hash: u64, mask: usize) usize {
    return @as(usize, @intCast(hash)) & mask;
}

fn isByteSlice(comptime K: type) bool {
    const info = @typeInfo(K);
    return info == .pointer and info.pointer.size == .slice and info.pointer.child == u8;
}

fn StableWorker(comptime CacheT: type, comptime stable_policy: Policy) type {
    if (stable_policy != .stable_lru and stable_policy != .stable_lhd) {
        @compileError("StableWorker requires a stable eviction policy");
    }

    return struct {
        const Self = @This();
        const K = CacheT.Key;
        const Context = CacheT.KeyContext;
        const Item = CacheT.Item;

        const StableSlru = struct {
            const Entry = struct {
                node: std.DoublyLinkedList.Node = .{},
                key: K,
                item: *Item,
                promotions: usize = 0,
                is_window: bool = false,
                is_protected: bool = false,
            };

            ctx: Context = .{},
            window: std.DoublyLinkedList = .{},
            window_weight: usize = 0,
            probation: std.DoublyLinkedList = .{},
            protected: std.DoublyLinkedList = .{},
            protected_weight: usize = 0,
            index: std.hash_map.HashMapUnmanaged(K, *Entry, Context, 80) = .{},

            fn deinit(self: *StableSlru, allocator: std.mem.Allocator) void {
                self.clear(allocator);
                self.index.deinit(allocator);
            }

            fn clear(self: *StableSlru, allocator: std.mem.Allocator) void {
                while (self.window.pop()) |n| {
                    const entry: *Entry = @fieldParentPtr("node", n);
                    _ = self.index.fetchRemoveContext(entry.key, self.ctx);
                    entry.item.release(allocator);
                    allocator.destroy(entry);
                }

                while (self.probation.pop()) |n| {
                    const entry: *Entry = @fieldParentPtr("node", n);
                    _ = self.index.fetchRemoveContext(entry.key, self.ctx);
                    entry.item.release(allocator);
                    allocator.destroy(entry);
                }

                while (self.protected.pop()) |n| {
                    const entry: *Entry = @fieldParentPtr("node", n);
                    _ = self.index.fetchRemoveContext(entry.key, self.ctx);
                    entry.item.release(allocator);
                    allocator.destroy(entry);
                }

                self.window_weight = 0;
                self.protected_weight = 0;
            }

            fn windowMaxWeight(_: *StableSlru, max_weight: usize, window_percent: u8) usize {
                if (window_percent == 0) return 0;
                const pct: usize = window_percent;
                const raw = (max_weight * pct) / 100;
                return @max(raw, 1);
            }

            fn mainMaxWeight(self: *StableSlru, max_weight: usize, window_percent: u8) usize {
                const window_max = self.windowMaxWeight(max_weight, window_percent);
                return max_weight -| window_max;
            }

            fn protectedMaxWeight(self: *StableSlru, max_weight: usize, window_percent: u8, protected_percent: u8) usize {
                if (protected_percent == 0) return 0;
                const main_max = self.mainMaxWeight(max_weight, window_percent);
                if (main_max == 0) return 0;
                const pct: usize = protected_percent;
                const raw = (main_max * pct) / 100;
                return @max(raw, 1);
            }

            fn ensureProtectedLimit(self: *StableSlru, max_weight: usize, window_percent: u8, protected_percent: u8) void {
                const protected_max = self.protectedMaxWeight(max_weight, window_percent, protected_percent);
                while (self.protected_weight > protected_max) {
                    const node = self.protected.pop() orelse break;
                    const entry: *Entry = @fieldParentPtr("node", node);

                    entry.is_window = false;
                    entry.is_protected = false;
                    self.protected_weight -|= entry.item.weight;
                    self.probation.prepend(&entry.node);
                }
            }

            fn upsert(self: *StableSlru, allocator: std.mem.Allocator, item: *Item, max_weight: usize, window_percent: u8, protected_percent: u8) !void {
                const key = item.key;

                if (self.index.getContext(key, self.ctx)) |entry| {
                    // Remove+reinsert so the map never points at freed key memory.
                    const old_key = entry.key;
                    _ = self.index.fetchRemoveContext(old_key, self.ctx);

                    if (entry.is_window) {
                        self.window_weight -|= entry.item.weight;
                        self.window_weight += item.weight;
                    } else if (entry.is_protected) {
                        self.protected_weight -|= entry.item.weight;
                        self.protected_weight += item.weight;
                        self.ensureProtectedLimit(max_weight, window_percent, protected_percent);
                    }

                    entry.item.release(allocator);
                    entry.item = item;
                    entry.key = key;
                    entry.promotions = 0;

                    try self.index.putContext(allocator, entry.key, entry, self.ctx);
                    return;
                }

                const entry = try allocator.create(Entry);
                entry.* = .{ .key = key, .item = item, .promotions = 0, .is_window = true, .is_protected = false };
                try self.index.putContext(allocator, entry.key, entry, self.ctx);
                self.window_weight += item.weight;
                self.window.prepend(&entry.node);
            }

            fn recordHit(self: *StableSlru, allocator: std.mem.Allocator, item: *Item, gets_per_promote: usize, max_weight: usize, window_percent: u8, protected_percent: u8) !void {
                const key = item.key;
                const threshold = @max(gets_per_promote, 1);

                const entry = self.index.getContext(key, self.ctx) orelse {
                    try self.upsert(allocator, item, max_weight, window_percent, protected_percent);
                    return;
                };

                // Remove+reinsert to keep map key tied to current owned key.
                const old_key = entry.key;
                _ = self.index.fetchRemoveContext(old_key, self.ctx);
                entry.key = key;
                try self.index.putContext(allocator, entry.key, entry, self.ctx);

                entry.item.release(allocator);
                entry.item = item;

                if (entry.is_window) {
                    self.window.remove(&entry.node);
                    self.window.prepend(&entry.node);
                    return;
                }

                entry.promotions += 1;
                if (entry.promotions < threshold) return;

                entry.promotions = 0;

                if (entry.is_protected) {
                    self.protected.remove(&entry.node);
                    self.protected.prepend(&entry.node);
                } else {
                    self.probation.remove(&entry.node);
                    entry.is_window = false;
                    entry.is_protected = true;
                    self.protected_weight += item.weight;
                    self.protected.prepend(&entry.node);
                    self.ensureProtectedLimit(max_weight, window_percent, protected_percent);
                }
            }

            fn removeIfSame(self: *StableSlru, allocator: std.mem.Allocator, item: *Item) void {
                const entry = self.index.getContext(item.key, self.ctx) orelse return;
                if (entry.item != item) return;

                self.removeEntry(allocator, entry);
            }

            const Pop = struct { key: K, item: *Item };

            fn popProbationLruEntry(self: *StableSlru) ?*Entry {
                const node = self.probation.pop() orelse return null;
                return @fieldParentPtr("node", node);
            }

            fn popWindowLruEntry(self: *StableSlru) ?*Entry {
                const node = self.window.pop() orelse return null;
                const entry: *Entry = @fieldParentPtr("node", node);
                entry.is_window = false;
                self.window_weight -|= entry.item.weight;
                return entry;
            }

            fn popEvictionVictim(self: *StableSlru, allocator: std.mem.Allocator) ?Pop {
                const node = self.probation.pop() orelse self.protected.pop() orelse self.window.pop() orelse return null;
                const entry: *Entry = @fieldParentPtr("node", node);
                _ = self.index.fetchRemoveContext(entry.key, self.ctx);

                if (entry.is_window) {
                    self.window_weight -|= entry.item.weight;
                }
                if (entry.is_protected) {
                    self.protected_weight -|= entry.item.weight;
                }

                entry.is_window = false;
                entry.is_protected = false;

                const out: Pop = .{ .key = entry.key, .item = entry.item };
                allocator.destroy(entry);
                return out;
            }

            fn removeEntry(self: *StableSlru, allocator: std.mem.Allocator, entry: *Entry) void {
                _ = self.index.fetchRemoveContext(entry.key, self.ctx);
                if (entry.is_window) {
                    self.window.remove(&entry.node);
                    self.window_weight -|= entry.item.weight;
                } else if (entry.is_protected) {
                    self.protected.remove(&entry.node);
                    self.protected_weight -|= entry.item.weight;
                } else {
                    self.probation.remove(&entry.node);
                }

                entry.item.release(allocator);
                allocator.destroy(entry);
            }
        };

        const Event = union(enum) {
            promote: *Item,
            insert: struct { item: *Item, replaced: bool },
            delete: *Item,
            evict: *std.Thread.ResetEvent,
            clear: *std.Thread.ResetEvent,
            sync: *std.Thread.ResetEvent,
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
        lru: StableSlru = .{},

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
                    .insert => |ins| ins.item.release(self.allocator),
                    .delete => |it| it.release(self.allocator),
                    .evict => |ack| ack.set(),
                    .clear => |ack| ack.set(),
                    .sync => |ack| ack.set(),
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

        fn enqueueInsert(self: *Self, item: *Item, replaced: bool) void {
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

            self.pushUnsafe(.{ .insert = .{ .item = item, .replaced = replaced } });
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

        fn enqueueSync(self: *Self) void {
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

            self.pushUnsafe(.{ .sync = &ack });
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
            const event_batch_cap: usize = 64;
            var pending: [event_batch_cap]Event = undefined;

            while (true) {
                self.lock.lock();
                while (!self.stopping and self.len == 0) {
                    self.cond.wait(&self.lock);
                }

                if (self.stopping) {
                    self.lock.unlock();
                    return;
                }

                const target_batch = @min(
                    @max(self.cache.config.eviction_batch_size * 2, 1),
                    event_batch_cap,
                );
                var batch_len: usize = 0;
                while (self.len > 0 and batch_len < target_batch) : (batch_len += 1) {
                    pending[batch_len] = self.popUnsafe();
                }
                if (self.len < self.buf.len) {
                    self.cond.broadcast();
                }
                self.lock.unlock();

                var batch_run_evict = false;
                var i: usize = 0;
                while (i < batch_len) : (i += 1) {
                    const ev = pending[i];
                    var run_evict = true;
                    switch (ev) {
                        .promote => |it| {
                            if (comptime stable_policy == .stable_lru) {
                                if (self.isCurrentLiveItem(it)) {
                                    self.lru.recordHit(
                                        self.allocator,
                                        it,
                                        self.cache.config.gets_per_promote,
                                        self.cache.config.max_weight,
                                        self.cache.config.stable_lru_window_percent,
                                        self.cache.config.stable_lru_protected_percent,
                                    ) catch {
                                        it.release(self.allocator);
                                    };
                                } else {
                                    it.release(self.allocator);
                                }
                            } else {
                                it.release(self.allocator);
                            }
                        },
                        .insert => |ins| {
                            if (comptime stable_policy == .stable_lru) {
                                if (self.isCurrentLiveItem(ins.item)) {
                                    const window_percent = self.cache.config.stable_lru_window_percent;
                                    const protected_percent = self.cache.config.stable_lru_protected_percent;

                                    blk: {
                                        self.lru.upsert(
                                            self.allocator,
                                            ins.item,
                                            self.cache.config.max_weight,
                                            window_percent,
                                            protected_percent,
                                        ) catch {
                                            ins.item.release(self.allocator);
                                            break :blk;
                                        };

                                        self.ensureWindowLimit();
                                    }
                                } else {
                                    self.reconcileStableLruKey(ins.item.key);
                                    ins.item.release(self.allocator);
                                }
                            } else {
                                if (!self.isCurrentLiveItem(ins.item)) {
                                    ins.item.release(self.allocator);
                                } else {
                                    const keep = if (self.cache.config.enable_tiny_lfu and !ins.replaced)
                                        self.applyTinyLfuAdmissionStableLhd(ins.item)
                                    else
                                        true;

                                    _ = keep;
                                    ins.item.release(self.allocator);
                                }
                            }
                        },
                        .delete => |it| {
                            if (comptime stable_policy == .stable_lru) {
                                self.lru.removeIfSame(self.allocator, it);
                                self.reconcileStableLruKey(it.key);
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
                        .sync => |ack| {
                            run_evict = false;
                            ack.set();
                        },
                    }

                    batch_run_evict = batch_run_evict or run_evict;
                }

                if (batch_run_evict) {
                    self.evictIfNeeded();
                }
            }
        }

        fn isCurrentLiveItem(self: *Self, item: *Item) bool {
            if (!item.isLive()) return false;

            const key_hash = self.cache.ctx.hash(item.key);
            const shard = self.cache.shardForHash(key_hash);
            shard.lock.lockShared();
            defer shard.lock.unlockShared();

            const current = shard.map.getContext(item.key, self.cache.ctx) orelse return false;
            return current == item and current.isLive();
        }

        fn currentLiveItemForKey(self: *Self, key: K) ?*Item {
            const key_hash = self.cache.ctx.hash(key);
            const shard = self.cache.shardForHash(key_hash);
            shard.lock.lockShared();
            defer shard.lock.unlockShared();

            const current = shard.map.getContext(key, self.cache.ctx) orelse return null;
            if (!current.isLive()) return null;
            return current;
        }

        fn reconcileStableLruKey(self: *Self, key: K) void {
            if (comptime stable_policy != .stable_lru) return;

            const current_live = self.currentLiveItemForKey(key);
            const entry = self.lru.index.getContext(key, self.lru.ctx) orelse return;
            if (current_live) |current| {
                if (entry.item == current) return;
            }

            self.lru.removeEntry(self.allocator, entry);
        }

        fn evictItemFromCache(self: *Self, item: *Item) void {
            item.markDead();

            const key_hash = self.cache.ctx.hash(item.key);
            const shard = self.cache.shardForHash(key_hash);
            if (shard.deleteIfSame(self.allocator, item.key, item, &self.cache.total_weight)) |removed| {
                removed.release(self.allocator);
            }
        }

        fn evictEntry(self: *Self, entry: *StableSlru.Entry) void {
            _ = self.lru.index.fetchRemoveContext(entry.key, self.lru.ctx);
            self.evictItemFromCache(entry.item);
            entry.item.release(self.allocator);
            self.allocator.destroy(entry);
        }

        fn ensureWindowLimit(self: *Self) void {
            const window_max = self.lru.windowMaxWeight(self.cache.config.max_weight, self.cache.config.stable_lru_window_percent);
            while (self.lru.window_weight > window_max) {
                const entry = self.lru.popWindowLruEntry() orelse break;
                self.applyTinyLfuAdmissionStableLru(entry);
            }
        }

        fn applyTinyLfuAdmissionStableLru(self: *Self, entry: *StableSlru.Entry) void {
            const candidate = entry.item;
            const candidate_weight = @max(candidate.weight, 1);
            if (candidate_weight > self.cache.config.max_weight) {
                self.evictEntry(entry);
                return;
            }

            const main_max = self.lru.mainMaxWeight(self.cache.config.max_weight, self.cache.config.stable_lru_window_percent);
            const total_weight = self.cache.total_weight.load(.acquire);
            const window_weight = self.lru.window_weight;
            const main_weight = total_weight -| window_weight;
            const main_without_candidate = main_weight -| candidate_weight;
            const available = main_max -| main_without_candidate;

            if (!self.cache.config.enable_tiny_lfu or candidate_weight <= available) {
                entry.promotions = 0;
                entry.is_window = false;
                entry.is_protected = false;
                self.lru.probation.prepend(&entry.node);
                return;
            }

            const candidate_freq = self.cache.sketchFrequency(self.cache.ctx.hash(candidate.key));

            const required_weight = candidate_weight -| available;

            const victim_buf_cap: usize = 64;
            var victim_buf: [victim_buf_cap]*StableSlru.Entry = undefined;
            const max_victims = @min(self.cache.config.eviction_batch_size, victim_buf_cap);
            var victims = std.ArrayListUnmanaged(*StableSlru.Entry).initBuffer(victim_buf[0..max_victims]);

            var victims_weight: usize = 0;
            var victims_freq: u32 = 0;

            while (victims_weight < required_weight and victims_freq <= candidate_freq and victims.items.len < max_victims) {
                const victim: *StableSlru.Entry = self.lru.popProbationLruEntry() orelse break;

                const freq = self.cache.sketchFrequency(self.cache.ctx.hash(victim.key));

                victims.appendBounded(victim) catch {
                    self.lru.probation.append(&victim.node);
                    break;
                };

                victims_weight += @max(victim.item.weight, 1);
                victims_freq += freq;
            }

            const admit = victims_weight >= required_weight and candidate_freq > victims_freq;
            if (admit) {
                for (victims.items) |victim| {
                    _ = self.lru.index.fetchRemoveContext(victim.key, self.lru.ctx);

                    self.evictItemFromCache(victim.item);
                    victim.item.release(self.allocator);
                    self.allocator.destroy(victim);
                }

                entry.promotions = 0;
                entry.is_window = false;
                entry.is_protected = false;
                self.lru.probation.prepend(&entry.node);
                return;
            }

            var i: usize = victims.items.len;
            while (i > 0) {
                i -= 1;
                victims.items[i].is_window = false;
                victims.items[i].is_protected = false;
                self.lru.probation.append(&victims.items[i].node);
            }

            self.evictEntry(entry);
        }

        fn applyTinyLfuAdmissionStableLhd(self: *Self, candidate: *Item) bool {
            if (self.cache.total_weight.load(.acquire) <= self.cache.config.max_weight) return true;

            const candidate_weight = @max(candidate.weight, 1);
            if (candidate_weight > self.cache.config.max_weight) {
                self.evictItemFromCache(candidate);
                return false;
            }

            const candidate_freq = self.cache.sketchFrequency(self.cache.ctx.hash(candidate.key));

            const victim_buf_cap: usize = 64;
            var victim_buf: [victim_buf_cap]*Item = undefined;
            const max_victims = @min(self.cache.config.eviction_batch_size, victim_buf_cap);
            var victims = std.ArrayListUnmanaged(*Item).initBuffer(victim_buf[0..max_victims]);

            var victims_weight: usize = 0;
            var victims_freq: u32 = 0;

            var attempts: usize = 0;
            const max_attempts = @max(self.cache.config.items_to_prune, 1) * self.cache.config.admission_victim_attempt_factor;

            while (victims_weight < candidate_weight and victims_freq <= candidate_freq and victims.items.len < max_victims and attempts < max_attempts) : (attempts += 1) {
                const victim = self.cache.pickEvictionCandidate(self.allocator) orelse break;

                if (victim == candidate or CacheT.containsItem(victims.items, victim)) {
                    victim.release(self.allocator);
                    continue;
                }

                const freq = self.cache.sketchFrequency(self.cache.ctx.hash(victim.key));

                victims.appendBounded(victim) catch {
                    victim.release(self.allocator);
                    break;
                };

                victims_weight += @max(victim.weight, 1);
                victims_freq += freq;
            }

            const admit = victims_weight >= candidate_weight and candidate_freq > victims_freq;
            if (admit) {
                for (victims.items) |victim| {
                    self.evictItemFromCache(victim);
                    victim.release(self.allocator);
                }
                return true;
            }

            for (victims.items) |victim| victim.release(self.allocator);
            self.evictItemFromCache(candidate);
            return false;
        }

        fn evictIfNeeded(self: *Self) void {
            var evicted: usize = 0;
            while (self.cache.total_weight.load(.acquire) > self.cache.config.max_weight and evicted < self.cache.config.items_to_prune) {
                if (comptime stable_policy == .stable_lru) {
                    const pop = self.lru.popEvictionVictim(self.allocator) orelse return;
                    defer pop.item.release(self.allocator);

                    pop.item.markDead();

                    const key_hash = self.cache.ctx.hash(pop.key);
                    const shard = self.cache.shardForHash(key_hash);
                    if (shard.deleteIfSame(self.allocator, pop.key, pop.item, &self.cache.total_weight)) |removed| {
                        removed.release(self.allocator);
                    }
                } else {
                    const candidate = self.cache.pickEvictionCandidate(self.allocator) orelse return;
                    defer candidate.release(self.allocator);

                    candidate.markDead();

                    const key_hash = self.cache.ctx.hash(candidate.key);
                    const shard = self.cache.shardForHash(key_hash);
                    if (shard.deleteIfSame(self.allocator, candidate.key, candidate, &self.cache.total_weight)) |removed| {
                        removed.release(self.allocator);
                    }
                }

                evicted += 1;
            }
        }
    };
}

pub fn CacheUnmanaged(
    comptime K: type,
    comptime V: type,
    comptime policy: Policy,
    comptime Weigher: type,
) type {
    const Context = key_context.AutoContext(K);
    const KeyOps = key_ops.Auto(K);

    const ItemT = ItemMod.Item(K, V, KeyOps);
    const ShardT = ShardMod.Shard(K, *ItemT, Context);
    const max_sketch_stripes: usize = 64;
    const max_victim_batch: usize = 64;
    const min_small_cache_sample_budget: usize = 8;

    return struct {
        const Worker = if (policy.isStable()) StableWorker(@This(), policy) else void;
        const SketchStripe = struct {
            _: void align(std.atomic.cache_line) = {},
            lock: std.Thread.Mutex = .{},
            frequency_sketch: FrequencySketch = .{},
        };

        config: Config,
        weigher: Weigher,
        ctx: Context = .{},
        shard_mask: usize,
        shards: []ShardT,

        access_clock: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
        total_weight: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        rng_state: std.atomic.Value(u64) = std.atomic.Value(u64).init(0x9e3779b97f4a7c15),

        sketch_stripes: []SketchStripe = &[_]SketchStripe{},
        sketch_mask: usize = 0,

        worker: if (policy.isStable()) ?*Worker else void = if (policy.isStable()) null else {},
        worker_lock: if (policy.isStable()) std.Thread.Mutex else void = if (policy.isStable()) .{} else {},

        pub const Key = K;
        pub const KeyContext = Context;

        pub fn init(allocator: std.mem.Allocator, config_in: Config, weigher: Weigher) InitError!@This() {
            comptime validateWeigher(K, V, Weigher);

            const cfg = try config_in.build();

            const shards = try allocator.alloc(ShardT, cfg.shard_count);
            for (shards) |*s| s.* = ShardT.init(.{});

            var out: @This() = .{
                .config = cfg,
                .weigher = weigher,
                .ctx = .{},
                .shard_mask = cfg.shard_count - 1,
                .shards = shards,
            };
            errdefer out.deinit(allocator);

            if (cfg.enable_tiny_lfu) {
                const auto_count = @max(
                    std.math.ceilPowerOfTwoAssert(usize, @min(cfg.shard_count, max_sketch_stripes)),
                    1,
                );
                const requested = if (cfg.sketch_stripe_count == 0)
                    auto_count
                else
                    @max(
                        std.math.ceilPowerOfTwoAssert(usize, @min(cfg.sketch_stripe_count, max_sketch_stripes)),
                        1,
                    );
                const stripe_count = @min(requested, max_sketch_stripes);
                out.sketch_stripes = try allocator.alloc(SketchStripe, stripe_count);
                for (out.sketch_stripes) |*stripe| stripe.* = .{};

                const per_stripe_cap = @max(
                    std.math.divCeil(usize, cfg.max_weight, stripe_count) catch cfg.max_weight,
                    1,
                );
                for (out.sketch_stripes) |*stripe| {
                    try stripe.frequency_sketch.ensureCapacity(allocator, per_stripe_cap, cfg.tiny_lfu_sample_scale);
                }
                out.sketch_mask = stripe_count - 1;
            }

            return out;
        }

        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            if (comptime policy.isStable()) {
                self.worker_lock.lock();
                const worker = self.worker;
                self.worker = null;
                self.worker_lock.unlock();

                if (worker) |w| w.deinit();
            }

            for (self.sketch_stripes) |*stripe| {
                stripe.frequency_sketch.deinit(allocator);
            }
            if (self.sketch_stripes.len > 0) {
                allocator.free(self.sketch_stripes);
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

        pub fn get(self: *@This(), allocator: std.mem.Allocator, key: K) ?ItemRef {
            const key_hash = self.ctx.hash(key);
            if (self.config.enable_tiny_lfu) {
                self.incrementSketch(key_hash);
            }

            const shard = self.shardForHash(key_hash);
            const item = shard.get(key) orelse return null;

            if (self.config.treat_expired_as_miss and item.isExpired()) {
                item.release(allocator);
                return null;
            }

            if (item.isLive() and !item.isExpired()) {
                item.recordHit(self.nextTick());
                if (comptime policy.isStable()) {
                    if (self.getWorker()) |worker| {
                        worker.tryEnqueuePromote(item);
                    }
                }
            }

            return .{ .item = item };
        }

        pub fn peek(self: *@This(), allocator: std.mem.Allocator, key: K) ?ItemRef {
            const key_hash = self.ctx.hash(key);
            const shard = self.shardForHash(key_hash);
            const item = shard.get(key) orelse return null;

            if (self.config.treat_expired_as_miss and item.isExpired()) {
                item.release(allocator);
                return null;
            }

            return .{ .item = item };
        }

        pub fn set(self: *@This(), allocator: std.mem.Allocator, key: K, value: V, ttl_ns: u64) !SetResult {
            const worker = if (comptime policy.isStable()) try self.ensureWorkerStarted(allocator) else {};
            const key_hash = self.ctx.hash(key);

            const weight = self.weigh(key, &value);
            const item = try ItemT.create(allocator, key, value, ttl_ns, weight);
            const tick = self.nextTick();
            item.setCreatedAtTick(tick);
            item.touch(tick);

            // Shard owns one ref.
            item.retain();

            const shard = self.shardForHash(key_hash);
            const old = try shard.set(allocator, item.key, item, &self.total_weight);
            const replaced = old != null;
            if (old) |old_item| {
                old_item.markDead();
                old_item.release(allocator);
            }

            if (comptime policy.isStable()) {
                worker.enqueueInsert(item, replaced);
                worker.enqueueEvict();
            } else {
                if (self.config.enable_tiny_lfu and !replaced) {
                    _ = self.applyTinyLfuAdmission(allocator, item, key_hash);
                }
                try self.evictIfNeeded(allocator);
            }

            const admitted = self.isCurrentItem(item);
            if (!admitted) {
                item.release(allocator);
                return .{ .status = .rejected, .item = null };
            }

            return .{
                .status = if (replaced) .replaced else .inserted,
                .item = .{ .item = item },
            };
        }

        pub fn replace(self: *@This(), allocator: std.mem.Allocator, key: K, value: V) !?SetResult {
            const existing = self.peekRaw(key) orelse return null;
            defer existing.deinit(allocator);

            const ttl = existing.ttlNs();
            return try self.set(allocator, key, value, ttl);
        }

        pub fn delete(self: *@This(), allocator: std.mem.Allocator, key: K) ?ItemRef {
            const key_hash = self.ctx.hash(key);
            const shard = self.shardForHash(key_hash);
            const item = shard.delete(allocator, key, &self.total_weight) orelse return null;

            if (comptime policy.isStable()) {
                if (self.getWorker()) |worker| {
                    worker.enqueueDelete(item);
                }
            }

            item.markDead();

            return .{ .item = item };
        }

        pub fn clear(self: *@This(), allocator: std.mem.Allocator) void {
            if (comptime policy.isStable()) {
                if (self.getWorker()) |worker| {
                    worker.enqueueClear();
                }
            }

            for (self.shards) |*shard| {
                shard.clear(allocator);
            }
            self.total_weight.store(0, .release);

            if (self.config.enable_tiny_lfu) {
                for (self.sketch_stripes) |*stripe| {
                    stripe.lock.lock();
                    stripe.frequency_sketch.clear();
                    stripe.lock.unlock();
                }
            }
        }

        /// Blocks until pending stable-policy maintenance is applied.
        pub fn sync(self: *@This()) void {
            if (comptime policy.isStable()) {
                if (self.getWorker()) |worker| {
                    worker.enqueueSync();
                }
            }
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

        pub fn extend(self: *@This(), allocator: std.mem.Allocator, key: K, ttl_ns: u64) bool {
            const ref = self.peekRaw(key) orelse return false;
            defer ref.deinit(allocator);

            ref.extend(ttl_ns);
            ref.item.touch(self.nextTick());
            return true;
        }

        fn peekRaw(self: *@This(), key: K) ?ItemRef {
            const key_hash = self.ctx.hash(key);
            const shard = self.shardForHash(key_hash);
            const item = shard.get(key) orelse return null;
            return .{ .item = item };
        }

        fn isCurrentItem(self: *@This(), item: *ItemT) bool {
            if (!item.isLive()) return false;

            const item_hash = self.ctx.hash(item.key);
            const shard = self.shardForHash(item_hash);
            shard.lock.lockShared();
            defer shard.lock.unlockShared();

            const current = shard.map.getContext(item.key, self.ctx) orelse return false;
            return current == item and current.isLive();
        }

        pub fn snapshot(self: *@This(), allocator: std.mem.Allocator) !std.ArrayList(ItemRef) {
            var list: std.ArrayList(ItemRef) = .empty;
            errdefer list.deinit(allocator);

            var tmp: std.ArrayList(*ItemT) = .empty;
            defer tmp.deinit(allocator);
            errdefer {
                for (tmp.items) |item| {
                    item.release(allocator);
                }
            }

            for (self.shards) |*shard| {
                try shard.snapshot(allocator, &tmp);
            }

            for (tmp.items) |item| {
                try list.append(allocator, .{ .item = item });
            }

            tmp.clearRetainingCapacity();
            return list;
        }

        pub fn filter(self: *@This(), allocator: std.mem.Allocator, comptime PredContext: type, pred_ctx: PredContext) !std.ArrayList(ItemRef) {
            comptime validateFilterPredicate(PredContext);
            var list: std.ArrayList(ItemRef) = .empty;
            errdefer {
                for (list.items) |it| {
                    it.deinit(allocator);
                }
                list.deinit(allocator);
            }

            var tmp: std.ArrayList(*ItemT) = .empty;
            defer tmp.deinit(allocator);
            errdefer {
                for (tmp.items) |item| {
                    item.release(allocator);
                }
            }

            for (self.shards) |*shard| {
                try shard.snapshot(allocator, &tmp);
            }

            for (tmp.items) |item| {
                if (PredContext.pred(pred_ctx, item.key, &item.value)) {
                    item.retain();
                    {
                        errdefer item.release(allocator);
                        try list.append(allocator, .{ .item = item });
                    }
                }
            }

            for (tmp.items) |item| {
                item.release(allocator);
            }
            tmp.clearRetainingCapacity();
            return list;
        }

        pub const InitError = std.mem.Allocator.Error || Config.BuildError || std.Thread.SpawnError;

        pub const Item = ItemT;
        pub const SetStatus = enum {
            inserted,
            replaced,
            rejected,
        };

        pub const SetResult = struct {
            status: SetStatus,
            item: ?ItemRef,

            pub fn deinit(self: SetResult, allocator: std.mem.Allocator) void {
                if (self.item) |item| item.deinit(allocator);
            }

            pub fn key(self: SetResult) K {
                return (self.item orelse unreachable).key();
            }

            pub fn value(self: SetResult) *const V {
                return (self.item orelse unreachable).value();
            }

            pub fn isExpired(self: SetResult) bool {
                return (self.item orelse unreachable).isExpired();
            }

            pub fn ttlNs(self: SetResult) u64 {
                return (self.item orelse unreachable).ttlNs();
            }

            pub fn extend(self: SetResult, ttl_ns: u64) void {
                (self.item orelse unreachable).extend(ttl_ns);
            }
        };

        /// Ref-counted handle to a cache item.
        ///
        /// Always `defer ref.deinit(allocator);` for values returned by cache operations.
        pub const ItemRef = struct {
            item: *ItemT,

            pub fn deinit(self: ItemRef, allocator: std.mem.Allocator) void {
                self.item.release(allocator);
            }

            /// Returns the cache key.
            ///
            /// Keys are logically immutable once inserted.
            /// - For value keys, this returns a copy.
            /// - For slice keys, this returns a read-only view into cache-owned
            ///   memory that remains valid while this `ItemRef` is alive.
            ///
            /// Do not mutate the returned key (e.g. via `@constCast`).
            pub fn key(self: ItemRef) K {
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

        fn validateWeigher(comptime K_check: type, comptime V_check: type, comptime Weigher_check: type) void {
            if (!@hasDecl(Weigher_check, "weigh")) {
                @compileError("Weigher must declare pub fn weigh(self: Weigher, key: K, value: *const V) usize");
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
            if (p1 != K_check) {
                @compileError("Weigher.weigh key parameter must be K");
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
                @compileError("filter() predicate must declare pub fn pred(self: PredContext, key: K, value: *const V) bool");
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
            if (p1 != K) {
                @compileError("filter() predicate key parameter must be K");
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

        fn weigh(self: *@This(), key: K, value: *const V) usize {
            return self.weigher.weigh(key, value);
        }

        fn ensureWorkerStarted(self: *@This(), allocator: std.mem.Allocator) InitError!*Worker {
            if (comptime policy.isStable()) {
                self.worker_lock.lock();
                defer self.worker_lock.unlock();

                if (self.worker == null) {
                    self.worker = try Worker.init(allocator, self);
                }
                return self.worker.?;
            }
            unreachable;
        }

        fn getWorker(self: *@This()) ?*Worker {
            if (comptime policy.isStable()) {
                self.worker_lock.lock();
                defer self.worker_lock.unlock();
                return self.worker;
            }
            return null;
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

        fn shardForHash(self: *@This(), hash: u64) *ShardT {
            return &self.shards[shardIndex(hash, self.shard_mask)];
        }

        fn sketchStripeForHash(self: *@This(), hash: u64) *SketchStripe {
            std.debug.assert(self.sketch_stripes.len > 0);
            return &self.sketch_stripes[shardIndex(hash, self.sketch_mask)];
        }

        fn incrementSketch(self: *@This(), hash: u64) void {
            if (!self.config.enable_tiny_lfu) return;
            const stripe = self.sketchStripeForHash(hash);
            stripe.lock.lock();
            stripe.frequency_sketch.increment(hash);
            stripe.lock.unlock();
        }

        fn sketchFrequency(self: *@This(), hash: u64) u32 {
            if (!self.config.enable_tiny_lfu) return 0;
            const stripe = self.sketchStripeForHash(hash);
            stripe.lock.lock();
            const freq: u32 = stripe.frequency_sketch.frequency(hash);
            stripe.lock.unlock();
            return freq;
        }

        fn pickEvictionCandidate(self: *@This(), allocator: std.mem.Allocator) ?*ItemT {
            const sample_budget = self.evictionSampleBudget();
            return if (comptime policy.isLhd())
                self.pickSampledLhdWithBudget(allocator, sample_budget)
            else
                self.pickSampledLruWithBudget(allocator, sample_budget);
        }

        fn evictionSampleBudget(self: *@This()) usize {
            const base = @max(self.config.sample_size, 1);
            if (self.itemCount() <= base) {
                return @max(
                    base *| self.config.eviction_sampling_factor_small_cache,
                    min_small_cache_sample_budget,
                );
            }
            return base;
        }

        fn pickSampledLruWithBudget(self: *@This(), allocator: std.mem.Allocator, sample_budget: usize) ?*ItemT {
            var best: ?*Item = null;
            var best_tick: u64 = std.math.maxInt(u64);

            var i: usize = 0;
            while (i < sample_budget) : (i += 1) {
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

        fn pickSampledLhdWithBudget(self: *@This(), allocator: std.mem.Allocator, sample_budget: usize) ?*ItemT {
            const now_tick = @max(self.access_clock.load(.acquire), 1);

            var best: ?*Item = null;

            var i: usize = 0;
            while (i < sample_budget) : (i += 1) {
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

        fn applyTinyLfuAdmission(self: *@This(), allocator: std.mem.Allocator, candidate: *ItemT, candidate_hash: u64) bool {
            if (self.total_weight.load(.acquire) <= self.config.max_weight) return true;

            const candidate_weight = @max(candidate.weight, 1);
            if (candidate_weight > self.config.max_weight) {
                self.evictCandidate(allocator, candidate);
                return false;
            }

            const candidate_freq = self.sketchFrequency(candidate_hash);

            var victim_buf: [max_victim_batch]*ItemT = undefined;
            const max_victims = @min(self.config.eviction_batch_size, max_victim_batch);
            var victims = std.ArrayListUnmanaged(*ItemT).initBuffer(victim_buf[0..max_victims]);

            var victims_weight: usize = 0;
            var victims_freq: u32 = 0;

            var attempts: usize = 0;
            const max_attempts = @max(self.config.items_to_prune, 1) * self.config.admission_victim_attempt_factor;

            while (victims_weight < candidate_weight and victims_freq <= candidate_freq and victims.items.len < max_victims and attempts < max_attempts) : (attempts += 1) {
                const victim = self.pickEvictionCandidate(allocator) orelse break;

                if (victim == candidate or containsItem(victims.items, victim)) {
                    victim.release(allocator);
                    continue;
                }

                const freq = self.sketchFrequency(self.ctx.hash(victim.key));

                victims.appendBounded(victim) catch {
                    victim.release(allocator);
                    break;
                };

                victims_weight += @max(victim.weight, 1);
                victims_freq += freq;
            }

            const admit = victims_weight >= candidate_weight and candidate_freq > victims_freq;
            if (admit) {
                self.evictVictims(allocator, victims.items);
                return true;
            }

            for (victims.items) |victim| victim.release(allocator);
            self.evictCandidate(allocator, candidate);
            return false;
        }

        fn evictCandidate(self: *@This(), allocator: std.mem.Allocator, candidate: *ItemT) void {
            candidate.markDead();

            const key_hash = self.ctx.hash(candidate.key);
            const shard = self.shardForHash(key_hash);
            if (shard.deleteIfSame(allocator, candidate.key, candidate, &self.total_weight)) |removed| {
                removed.release(allocator);
            }
        }

        fn evictVictims(self: *@This(), allocator: std.mem.Allocator, victims: []const *ItemT) void {
            for (victims) |victim| {
                victim.markDead();

                const key_hash = self.ctx.hash(victim.key);
                const shard = self.shardForHash(key_hash);
                if (shard.deleteIfSame(allocator, victim.key, victim, &self.total_weight)) |removed| {
                    removed.release(allocator);
                }

                victim.release(allocator);
            }
        }

        fn containsItem(items: []const *ItemT, needle: *ItemT) bool {
            for (items) |it| {
                if (it == needle) return true;
            }
            return false;
        }

        fn evictIfNeeded(self: *@This(), allocator: std.mem.Allocator) !void {
            var evicted: usize = 0;
            while (self.total_weight.load(.acquire) > self.config.max_weight and evicted < self.config.items_to_prune) {
                const candidate = self.pickEvictionCandidate(allocator) orelse return;
                defer candidate.release(allocator);

                candidate.markDead();

                const key_hash = self.ctx.hash(candidate.key);
                const shard = self.shardForHash(key_hash);
                if (shard.deleteIfSame(allocator, candidate.key, candidate, &self.total_weight)) |removed| {
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
    comptime K: type,
    comptime V: type,
    comptime policy: Policy,
    comptime Weigher: type,
) type {
    const Unmanaged = CacheUnmanaged(K, V, policy, Weigher);

    return struct {
        unmanaged: Unmanaged,
        allocator: std.mem.Allocator,

        pub const SetStatus = Unmanaged.SetStatus;

        pub const ItemRef = struct {
            inner: Unmanaged.ItemRef,
            allocator: std.mem.Allocator,

            pub fn deinit(self: ItemRef) void {
                self.inner.deinit(self.allocator);
            }

            pub fn key(self: ItemRef) K {
                return self.inner.key();
            }

            pub fn value(self: ItemRef) *const V {
                return self.inner.value();
            }

            pub fn isExpired(self: ItemRef) bool {
                return self.inner.isExpired();
            }

            pub fn ttlNs(self: ItemRef) u64 {
                return self.inner.ttlNs();
            }

            pub fn extend(self: ItemRef, ttl_ns: u64) void {
                self.inner.extend(ttl_ns);
            }
        };

        pub const SetResult = struct {
            status: SetStatus,
            item: ?ItemRef,

            pub fn deinit(self: SetResult) void {
                if (self.item) |item| item.deinit();
            }

            pub fn key(self: SetResult) K {
                return (self.item orelse unreachable).key();
            }

            pub fn value(self: SetResult) *const V {
                return (self.item orelse unreachable).value();
            }

            pub fn isExpired(self: SetResult) bool {
                return (self.item orelse unreachable).isExpired();
            }

            pub fn ttlNs(self: SetResult) u64 {
                return (self.item orelse unreachable).ttlNs();
            }

            pub fn extend(self: SetResult, ttl_ns: u64) void {
                (self.item orelse unreachable).extend(ttl_ns);
            }
        };

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
        pub fn get(self: *@This(), key: K) ?ItemRef {
            const item = self.unmanaged.get(self.allocator, key) orelse return null;
            return self.wrapItemRef(item);
        }

        /// Returns an `ItemRef` for `key` without recording a hit.
        pub fn peek(self: *@This(), key: K) ?ItemRef {
            const item = self.unmanaged.peek(self.allocator, key) orelse return null;
            return self.wrapItemRef(item);
        }

        /// Inserts `key` → `value` with TTL and returns insertion status.
        pub fn set(self: *@This(), key: K, value: V, ttl_ns: u64) !SetResult {
            return self.wrapSetResult(try self.unmanaged.set(self.allocator, key, value, ttl_ns));
        }

        /// Replaces the value for an existing key (preserving TTL).
        pub fn replace(self: *@This(), key: K, value: V) !?SetResult {
            const result = (try self.unmanaged.replace(self.allocator, key, value)) orelse return null;
            return self.wrapSetResult(result);
        }

        /// Deletes an item and returns its `ItemRef`.
        pub fn delete(self: *@This(), key: K) ?ItemRef {
            const item = self.unmanaged.delete(self.allocator, key) orelse return null;
            return self.wrapItemRef(item);
        }

        /// Removes all items from the cache.
        pub fn clear(self: *@This()) void {
            self.unmanaged.clear(self.allocator);
        }

        /// Blocks until pending stable-policy maintenance is applied.
        pub fn sync(self: *@This()) void {
            self.unmanaged.sync();
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
        pub fn extend(self: *@This(), key: K, ttl_ns: u64) bool {
            return self.unmanaged.extend(self.allocator, key, ttl_ns);
        }

        /// Returns a snapshot of current items.
        pub fn snapshot(self: *@This(), allocator: std.mem.Allocator) !std.ArrayList(ItemRef) {
            var raw = try self.unmanaged.snapshot(allocator);
            errdefer {
                for (raw.items) |item| item.deinit(self.allocator);
                raw.deinit(allocator);
            }

            var list: std.ArrayList(ItemRef) = .empty;
            errdefer {
                for (list.items) |item| item.deinit();
                list.deinit(allocator);
            }

            try list.ensureTotalCapacity(allocator, raw.items.len);
            for (raw.items) |item| {
                list.appendAssumeCapacity(self.wrapItemRef(item));
            }
            raw.deinit(allocator);
            return list;
        }

        /// Returns items matching `PredContext.pred(pred_ctx, key, value)`.
        pub fn filter(self: *@This(), allocator: std.mem.Allocator, comptime PredContext: type, pred_ctx: PredContext) !std.ArrayList(ItemRef) {
            var raw = try self.unmanaged.filter(allocator, PredContext, pred_ctx);
            errdefer {
                for (raw.items) |item| item.deinit(self.allocator);
                raw.deinit(allocator);
            }

            var list: std.ArrayList(ItemRef) = .empty;
            errdefer {
                for (list.items) |item| item.deinit();
                list.deinit(allocator);
            }

            try list.ensureTotalCapacity(allocator, raw.items.len);
            for (raw.items) |item| {
                list.appendAssumeCapacity(self.wrapItemRef(item));
            }
            raw.deinit(allocator);
            return list;
        }

        fn wrapItemRef(self: *@This(), item: Unmanaged.ItemRef) ItemRef {
            return .{ .inner = item, .allocator = self.allocator };
        }

        fn wrapSetResult(self: *@This(), result: Unmanaged.SetResult) SetResult {
            return .{
                .status = result.status,
                .item = if (result.item) |item| self.wrapItemRef(item) else null,
            };
        }
    };
}

pub fn SampledLruCache(comptime K: type, comptime V: type) type {
    const Weigher = if (comptime isByteSlice(K)) weigher_mod.Bytes(K, V) else weigher_mod.Items(K, V);
    return Cache(K, V, .sampled_lru, Weigher);
}

pub fn SampledLruCacheWithWeigher(comptime K: type, comptime V: type, comptime Weigher: type) type {
    return Cache(K, V, .sampled_lru, Weigher);
}

pub fn SampledLhdCache(comptime K: type, comptime V: type, comptime Weigher: type) type {
    return Cache(K, V, .sampled_lhd, Weigher);
}

pub fn StableLruCache(comptime K: type, comptime V: type) type {
    const Weigher = if (comptime isByteSlice(K)) weigher_mod.Bytes(K, V) else weigher_mod.Items(K, V);
    return Cache(K, V, .stable_lru, Weigher);
}

pub fn StableLruCacheWithWeigher(comptime K: type, comptime V: type, comptime Weigher: type) type {
    return Cache(K, V, .stable_lru, Weigher);
}

pub fn StableLhdCache(comptime K: type, comptime V: type, comptime Weigher: type) type {
    return Cache(K, V, .stable_lhd, Weigher);
}
