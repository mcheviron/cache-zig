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
            return self.map.getContext(key, self.ctx);
        }

        pub fn set(self: *@This(), allocator: std.mem.Allocator, key: K, item: ItemPtr) !?ItemPtr {
            self.lock.lock();
            defer self.lock.unlock();

            // IMPORTANT: the map stores keys by value.
            // If `K` contains pointer/slice fields (e.g. `[]const u8`), a replace can
            // change the backing allocation. On overwrite, remove+reinsert so the
            // map's stored key stays tied to the new item's owned key.
            if (self.map.fetchRemoveContext(key, self.ctx)) |removed| {
                const old = removed.value;
                const idx = (self.pos.fetchRemoveContext(key, self.ctx) orelse unreachable).value;

                try self.map.putContext(allocator, item.key, item, self.ctx);
                self.items.items[idx] = item;
                try self.pos.putContext(allocator, item.key, idx, self.ctx);

                return old;
            }

            try self.map.putContext(allocator, item.key, item, self.ctx);
            try self.pos.putContext(allocator, item.key, self.items.items.len, self.ctx);
            try self.items.append(allocator, item);
            return null;
        }

        pub fn delete(self: *@This(), allocator: std.mem.Allocator, key: K) ?ItemPtr {
            self.lock.lock();
            defer self.lock.unlock();

            const removed = self.map.fetchRemoveContext(key, self.ctx) orelse return null;
            const old = removed.value;

            const idx = (self.pos.fetchRemoveContext(key, self.ctx) orelse unreachable).value;
            const last_idx = self.items.items.len - 1;

            if (idx != last_idx) {
                const swapped = self.items.items[last_idx];
                self.items.items[idx] = swapped;
                // Update swapped key index.
                _ = self.pos.fetchRemoveContext(swapped.key, self.ctx);
                self.pos.putContext(allocator, swapped.key, idx, self.ctx) catch unreachable;
            }

            self.items.items.len = last_idx;
            return old;
        }

        pub fn deleteIfSame(
            self: *@This(),
            allocator: std.mem.Allocator,
            key: K,
            expected: ItemPtr,
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
                _ = self.pos.fetchRemoveContext(swapped.key, self.ctx);
                self.pos.putContext(allocator, swapped.key, idx, self.ctx) catch unreachable;
            }

            self.items.items.len = last_idx;
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
                try out.append(allocator, item);
            }
        }
    };
}
