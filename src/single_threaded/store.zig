const std = @import("std");

pub fn Store(comptime ItemT: type) type {
    return struct {
        map: std.StringHashMapUnmanaged(*ItemT) = .{},
        items: std.ArrayListUnmanaged(*ItemT) = .{},

        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            var it = self.map.iterator();
            while (it.next()) |e| {
                e.value_ptr.*.markDead();
                e.value_ptr.*.release(allocator);
            }
            self.map.deinit(allocator);
            self.items.deinit(allocator);
        }

        pub fn len(self: *@This()) usize {
            return self.items.items.len;
        }

        pub fn get(self: *@This(), key: []const u8) ?*ItemT {
            return self.map.get(key);
        }

        pub fn put(self: *@This(), allocator: std.mem.Allocator, item: *ItemT) !?*ItemT {
            const old_kv = self.map.fetchRemove(item.key);
            if (old_kv) |kv| {
                self.swapRemoveItem(kv.value);
            }

            const gop = try self.map.getOrPut(allocator, item.key);
            std.debug.assert(!gop.found_existing);
            gop.key_ptr.* = item.key;
            gop.value_ptr.* = item;

            item.items_index = self.items.items.len;
            try self.items.append(allocator, item);

            if (old_kv) |kv| return kv.value;
            return null;
        }

        pub fn delete(self: *@This(), key: []const u8) ?*ItemT {
            const kv = self.map.fetchRemove(key) orelse return null;
            const item = kv.value;
            self.swapRemoveItem(item);
            return item;
        }

        fn swapRemoveItem(self: *@This(), item: *ItemT) void {
            if (self.items.items.len == 0) return;
            const idx = item.items_index;
            if (idx >= self.items.items.len) return;

            const last_idx = self.items.items.len - 1;
            const last = self.items.items[last_idx];
            _ = self.items.swapRemove(idx);
            if (idx != last_idx) {
                last.items_index = idx;
            }
        }
    };
}
