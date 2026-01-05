const std = @import("std");

pub fn Shard(comptime ItemPtr: type) type {
    return struct {
        lock: std.Thread.RwLock = .{},
        map: std.StringHashMapUnmanaged(ItemPtr) = .{},
        items: std.ArrayListUnmanaged(ItemPtr) = .{},
        pos: std.StringHashMapUnmanaged(usize) = .{},

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

        pub fn get(self: *@This(), key: []const u8) ?ItemPtr {
            self.lock.lockShared();
            defer self.lock.unlockShared();
            return self.map.get(key);
        }

        pub fn set(self: *@This(), allocator: std.mem.Allocator, key: []const u8, item: ItemPtr) !?ItemPtr {
            self.lock.lock();
            defer self.lock.unlock();

            // IMPORTANT: StringHashMapUnmanaged stores the key slice as-is.
            // Keys are owned by `Item.key`, so on overwrite we must remove+reinsert
            // to avoid the map holding a pointer to freed key bytes.
            if (self.map.fetchRemove(key)) |removed| {
                const old = removed.value;
                const idx = (self.pos.fetchRemove(key) orelse unreachable).value;

                try self.map.put(allocator, item.key, item);
                self.items.items[idx] = item;
                try self.pos.put(allocator, item.key, idx);

                return old;
            }

            try self.map.put(allocator, item.key, item);
            try self.pos.put(allocator, item.key, self.items.items.len);
            try self.items.append(allocator, item);
            return null;
        }

        pub fn delete(self: *@This(), allocator: std.mem.Allocator, key: []const u8) ?ItemPtr {
            self.lock.lock();
            defer self.lock.unlock();

            const removed = self.map.fetchRemove(key) orelse return null;
            const old = removed.value;

            const idx = (self.pos.fetchRemove(key) orelse unreachable).value;
            const last_idx = self.items.items.len - 1;

            if (idx != last_idx) {
                const swapped = self.items.items[last_idx];
                self.items.items[idx] = swapped;
                // Update swapped key index.
                _ = self.pos.fetchRemove(swapped.key);
                self.pos.put(allocator, swapped.key, idx) catch unreachable;
            }

            self.items.items.len = last_idx;
            return old;
        }

        pub fn deleteIfSame(
            self: *@This(),
            allocator: std.mem.Allocator,
            key: []const u8,
            expected: ItemPtr,
        ) ?ItemPtr {
            self.lock.lock();
            defer self.lock.unlock();

            const current = self.map.get(key) orelse return null;
            if (current != expected) return null;

            const removed = self.map.fetchRemove(key) orelse return null;
            const old = removed.value;

            const idx = (self.pos.fetchRemove(key) orelse unreachable).value;
            const last_idx = self.items.items.len - 1;

            if (idx != last_idx) {
                const swapped = self.items.items[last_idx];
                self.items.items[idx] = swapped;
                _ = self.pos.fetchRemove(swapped.key);
                self.pos.put(allocator, swapped.key, idx) catch unreachable;
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
