const std = @import("std");

const Allocator = std.mem.Allocator;

/// A probabilistic multi-set for estimating key popularity within a time window.
///
/// This is a 4-bit Count-Min Sketch with periodic aging, used for TinyLFU-style
/// admission decisions.
pub const FrequencySketch = struct {
    table: []u64 = &[_]u64{},
    table_mask: u64 = 0,
    sample_size: u32 = 0,
    size: u32 = 0,

    const reset_mask: u64 = 0x7777_7777_7777_7777;
    const one_mask: u64 = 0x1111_1111_1111_1111;

    const seed: [4]u64 = .{
        0xc3a5_c85c_97cb_3127,
        0xb492_b66f_be98_f273,
        0x9ae1_6a3b_2f90_404f,
        0xcbf2_9ce4_8422_2325,
    };

    // Hard clamp to avoid huge allocations.
    const max_capacity: usize = 1 << 20;

    pub fn deinit(self: *FrequencySketch, allocator: Allocator) void {
        if (self.table.len != 0) allocator.free(self.table);
        self.* = .{};
    }

    pub fn clear(self: *FrequencySketch) void {
        @memset(self.table, 0);
        self.size = 0;
    }

    /// Ensures the sketch is sized for an expected maximum capacity.
    ///
    /// Notes:
    /// - `cap` is treated as a sizing hint.
    /// - We clamp the backing table to a reasonable maximum to avoid huge allocations
    ///   when `cap` is very large.
    pub fn ensureCapacity(self: *FrequencySketch, allocator: Allocator, cap: usize, sample_scale: usize) Allocator.Error!void {
        const maximum = @min(cap, max_capacity);
        const table_size: usize = if (maximum == 0) 1 else std.math.ceilPowerOfTwoAssert(usize, maximum);

        if (self.table.len >= table_size) {
            if (self.table.len == 0) return;
            self.sample_size = if (cap == 0) 10 else @intCast(
                @min(
                    maximum * @max(sample_scale, 1),
                    std.math.maxInt(u32),
                ),
            );
            return;
        }

        if (self.table.len != 0) allocator.free(self.table);
        self.table = try allocator.alloc(u64, table_size);
        @memset(self.table, 0);

        self.table_mask = @intCast(table_size - 1);
        self.sample_size = if (cap == 0) 10 else @intCast(
            @min(
                maximum * @max(sample_scale, 1),
                std.math.maxInt(u32),
            ),
        );
        self.size = 0;
    }

    /// Returns the estimated frequency of a hash (0..15).
    pub fn frequency(self: *const FrequencySketch, hash: u64) u8 {
        if (self.table.len == 0) return 0;

        const start: u8 = @intCast((hash & 3) << 2);
        var freq: u8 = std.math.maxInt(u8);

        var i: u8 = 0;
        while (i < 4) : (i += 1) {
            const idx = self.indexOf(hash, i);
            const shift: u6 = @intCast((start + i) << 2);
            const count: u8 = @intCast((self.table[idx] >> shift) & 0xF);
            freq = @min(freq, count);
        }

        return freq;
    }

    /// Increments the estimated popularity of a hash (up to 15).
    pub fn increment(self: *FrequencySketch, hash: u64) void {
        if (self.table.len == 0) return;

        const start: u8 = @intCast((hash & 3) << 2);
        var added = false;

        var i: u8 = 0;
        while (i < 4) : (i += 1) {
            const idx = self.indexOf(hash, i);
            added = self.incrementAt(idx, start + i) or added;
        }

        if (added) {
            self.size +|= 1;
            if (self.size >= self.sample_size) {
                self.reset();
            }
        }
    }

    fn incrementAt(self: *FrequencySketch, table_index: usize, counter_index: u8) bool {
        const offset: u6 = @intCast(@as(u32, counter_index) << 2);
        const mask: u64 = @as(u64, 0xF) << offset;
        if (self.table[table_index] & mask != mask) {
            self.table[table_index] +%= @as(u64, 1) << offset;
            return true;
        }
        return false;
    }

    fn reset(self: *FrequencySketch) void {
        var count: u32 = 0;
        for (self.table) |*entry| {
            count +|= @popCount(entry.* & one_mask);
            entry.* = (entry.* >> 1) & reset_mask;
        }

        self.size = (self.size >> 1) -| (count >> 2);
    }

    fn indexOf(self: *const FrequencySketch, hash: u64, depth: u8) usize {
        const i: usize = @intCast(depth);

        var h = hash +% seed[i];
        h = h *% seed[i];
        h = h +% (h >> 32);

        return @intCast(h & self.table_mask);
    }
};
