const std = @import("std");

pub fn Shard(comptime K: type, comptime ItemPtr: type, comptime Context: type) type {
    return struct {
        lock: std.Thread.RwLock = .{},
        map: std.hash_map.HashMapUnmanaged(K, ItemPtr, Context, 80) = .{},
        items: std.ArrayListUnmanaged(ItemPtr) = .{},
        pos: std.hash_map.HashMapUnmanaged(K, usize, Context, 80) = .{},
        ctx: Context,

        pub fn init(ctx: Context) @This() {
            return .{ .ctx = ctx };
        }

        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            self.lock.lock();
            defer self.lock.unlock();

            self.map.deinit(allocator);
            self.items.deinit(allocator);
            self.pos.deinit(allocator);
        }

        pub fn len(self: *@This()) usize {
            self.lock.lockShared();
            defer self.lock.unlockShared();
            return self.items.items.len;
        }

        pub fn get(self: *@This(), key: K) ?ItemPtr {
            self.lock.lockShared();
            defer self.lock.unlockShared();

            const item = self.map.getContext(key, self.ctx) orelse return null;
            item.retain();
            return item;
        }

        pub fn set(
            self: *@This(),
            allocator: std.mem.Allocator,
            key: K,
            item: ItemPtr,
            total_weight: *std.atomic.Value(usize),
        ) !?ItemPtr {
            self.lock.lock();
            defer self.lock.unlock();

            const map_gop = try self.map.getOrPutContext(allocator, key, self.ctx);
            if (map_gop.found_existing) {
                // Existing key: update in place so map/pos/items stay consistent even on OOM.
                const old = map_gop.value_ptr.*;

                const pos_gop = try self.pos.getOrPutContext(allocator, key, self.ctx);
                std.debug.assert(pos_gop.found_existing);
                const idx = pos_gop.value_ptr.*;

                map_gop.key_ptr.* = item.key;
                map_gop.value_ptr.* = item;

                pos_gop.key_ptr.* = item.key;
                pos_gop.value_ptr.* = idx;

                self.items.items[idx] = item;
                _ = total_weight.fetchSub(old.weight, .acq_rel);
                _ = total_weight.fetchAdd(item.weight, .acq_rel);
                return old;
            }

            map_gop.key_ptr.* = item.key;
            map_gop.value_ptr.* = item;
            errdefer _ = self.map.fetchRemoveContext(item.key, self.ctx);

            try self.pos.ensureUnusedCapacity(allocator, 1);
            try self.items.ensureUnusedCapacity(allocator, 1);

            const idx = self.items.items.len;
            const pos_gop = try self.pos.getOrPutContext(allocator, item.key, self.ctx);
            std.debug.assert(!pos_gop.found_existing);
            pos_gop.key_ptr.* = item.key;
            pos_gop.value_ptr.* = idx;

            self.items.appendAssumeCapacity(item);
            _ = total_weight.fetchAdd(item.weight, .acq_rel);
            return null;
        }

        pub fn delete(
            self: *@This(),
            _: std.mem.Allocator,
            key: K,
            total_weight: *std.atomic.Value(usize),
        ) ?ItemPtr {
            self.lock.lock();
            defer self.lock.unlock();

            const removed = self.map.fetchRemoveContext(key, self.ctx) orelse return null;
            const old = removed.value;

            const idx = (self.pos.fetchRemoveContext(key, self.ctx) orelse unreachable).value;
            const last_idx = self.items.items.len - 1;

            if (idx != last_idx) {
                const swapped = self.items.items[last_idx];
                self.items.items[idx] = swapped;
                const swapped_idx = self.pos.getPtrContext(swapped.key, self.ctx) orelse unreachable;
                swapped_idx.* = idx;
            }

            self.items.items.len = last_idx;
            _ = total_weight.fetchSub(old.weight, .acq_rel);
            return old;
        }

        pub fn deleteIfSame(
            self: *@This(),
            _: std.mem.Allocator,
            key: K,
            expected: ItemPtr,
            total_weight: *std.atomic.Value(usize),
        ) ?ItemPtr {
            self.lock.lock();
            defer self.lock.unlock();

            const current = self.map.getContext(key, self.ctx) orelse return null;
            if (current != expected) return null;

            const removed = self.map.fetchRemoveContext(key, self.ctx) orelse return null;
            const old = removed.value;

            const idx = (self.pos.fetchRemoveContext(key, self.ctx) orelse unreachable).value;
            const last_idx = self.items.items.len - 1;

            if (idx != last_idx) {
                const swapped = self.items.items[last_idx];
                self.items.items[idx] = swapped;
                const swapped_idx = self.pos.getPtrContext(swapped.key, self.ctx) orelse unreachable;
                swapped_idx.* = idx;
            }

            self.items.items.len = last_idx;
            _ = total_weight.fetchSub(old.weight, .acq_rel);
            return old;
        }

        pub fn sampleAtRetained(self: *@This(), idx: usize) ?ItemPtr {
            self.lock.lockShared();
            defer self.lock.unlockShared();

            if (self.items.items.len == 0) return null;
            const item = self.items.items[idx % self.items.items.len];
            item.retain();
            return item;
        }

        pub fn clear(self: *@This(), allocator: std.mem.Allocator) void {
            self.lock.lock();
            defer self.lock.unlock();

            // Release shard-owned refs.
            for (self.items.items) |item| {
                item.release(allocator);
            }

            self.map.clearRetainingCapacity();
            self.items.clearRetainingCapacity();
            self.pos.clearRetainingCapacity();
        }

        pub fn snapshot(self: *@This(), allocator: std.mem.Allocator, out: *std.ArrayList(ItemPtr)) !void {
            self.lock.lockShared();
            defer self.lock.unlockShared();

            for (self.items.items) |item| {
                item.retain();
                errdefer item.release(allocator);
                try out.append(allocator, item);
            }
        }
    };
}
