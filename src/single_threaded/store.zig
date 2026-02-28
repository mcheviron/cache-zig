const std = @import("std");

pub fn Store(comptime K: type, comptime ItemT: type, comptime Context: type) type {
    return struct {
        map: std.hash_map.HashMapUnmanaged(K, *ItemT, Context, 80) = .{},
        items: std.ArrayListUnmanaged(*ItemT) = .{},
        ctx: Context,

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

        pub fn init(ctx: Context) @This() {
            return .{ .ctx = ctx };
        }

        pub fn get(self: *@This(), key: K) ?*ItemT {
            return self.map.getContext(key, self.ctx);
        }

        pub fn put(self: *@This(), allocator: std.mem.Allocator, item: *ItemT) !?*ItemT {
            if (self.map.getContext(item.key, self.ctx)) |_| {
                // Existing key: update in-place so the operation stays transactional.
                const gop = try self.map.getOrPutContext(allocator, item.key, self.ctx);
                std.debug.assert(gop.found_existing);

                const old_item = gop.value_ptr.*;
                const idx = old_item.items_index;

                gop.key_ptr.* = item.key;
                gop.value_ptr.* = item;
                self.items.items[idx] = item;
                item.items_index = idx;
                return old_item;
            }

            try self.map.ensureUnusedCapacity(allocator, 1);
            try self.items.ensureUnusedCapacity(allocator, 1);

            const gop = try self.map.getOrPutContext(allocator, item.key, self.ctx);
            std.debug.assert(!gop.found_existing);
            gop.key_ptr.* = item.key;
            gop.value_ptr.* = item;

            item.items_index = self.items.items.len;
            self.items.appendAssumeCapacity(item);
            return null;
        }

        pub fn delete(self: *@This(), key: K) ?*ItemT {
            const kv = self.map.fetchRemoveContext(key, self.ctx) orelse return null;
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
