const std = @import("std");

/// Cache configuration.
///
/// Call `build()` to validate/normalize.
///
/// `build()` performs no allocations.
pub const Config = struct {
    /// Number of shards; must be a power of two.
    ///
    /// Higher shard counts may improve parallelism at the cost of memory.
    shard_count: usize = 16,

    /// Total weight capacity before eviction.
    max_weight: usize = 5000,

    /// Maximum number of evictions per `set`.
    items_to_prune: usize = 500,

    /// Number of candidates sampled per eviction.
    ///
    /// Larger values generally improve eviction quality but increase eviction cost.
    sample_size: usize = 32,

    /// If true (default), expired items are treated as cache misses.
    ///
    /// When false, `get`/`peek` can return expired items.
    treat_expired_as_miss: bool = true,

    /// When true (default), admission uses a frequency sketch.
    enable_tiny_lfu: bool = true,

    /// Scaling factor for the sketch sampling window.
    tiny_lfu_sample_scale: usize = 10,

    /// Stable-policy promotion batching.
    ///
    /// For stable policies, the maintenance worker only applies SLRU promotion
    /// after this many `get()` hits.
    gets_per_promote: usize = 1,

    /// Stable LRU protected segment size (percent of `max_weight`).
    ///
    /// Default is 80%, matching common SLRU guidance.
    ///
    /// Used only by the stable LRU policy.
    stable_lru_protected_percent: u8 = 80,

    /// Buffer size for promotion/touch events.
    promote_buffer: usize = 1024,

    /// Buffer size for delete events.
    delete_buffer: usize = 1024,

    pub const BuildError = error{
        ShardCountZero,
        ShardCountNotPowerOfTwo,
    };

    /// Validate/normalize config. Performs no allocations.
    ///
    /// # Example
    ///
    /// ```zig
    /// const cache_zig = @import("cache_zig");
    /// const cfg = try cache_zig.Config{ .shard_count = 16 }.build();
    /// _ = cfg;
    /// ```
    pub fn build(self: Config) BuildError!Config {
        if (self.shard_count == 0) return error.ShardCountZero;
        if ((self.shard_count & (self.shard_count - 1)) != 0) return error.ShardCountNotPowerOfTwo;

        var out = self;
        out.items_to_prune = @max(out.items_to_prune, 1);
        out.sample_size = @max(out.sample_size, 1);
        out.tiny_lfu_sample_scale = @max(out.tiny_lfu_sample_scale, 1);
        out.gets_per_promote = @max(out.gets_per_promote, 1);
        out.stable_lru_protected_percent = if (out.stable_lru_protected_percent > 100) 100 else out.stable_lru_protected_percent;
        out.promote_buffer = @max(out.promote_buffer, 1);
        out.delete_buffer = @max(out.delete_buffer, 1);
        return out;
    }
};
