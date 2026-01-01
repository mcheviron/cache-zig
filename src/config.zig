const std = @import("std");

/// Cache configuration.
///
/// Config is a plain struct (no builder pattern). Set fields directly.
/// Call `build()` to validate/normalize.
///
/// `build()` performs no allocations.
pub const Config = struct {
    /// Eviction strategy used when `max_weight` is exceeded.
    pub const EvictionPolicy = enum {
        /// Sample candidates and evict the least-recently accessed.
        sampled_lru,

        /// Sample candidates and evict the least hit-dense candidate.
        ///
        /// Hit density is approximated as `hits / (age * weight)`.
        sampled_lhd,
    };

    /// Number of shards; must be a power of two.
    ///
    /// Higher shard counts may improve parallelism at the cost of memory.
    shards: usize = 16,

    /// Total weight capacity before eviction.
    ///
    /// Weight is computed by `Cache(V).Weigher` when provided, otherwise it is
    /// `key.len + @sizeOf(V)`.
    max_weight: usize = 5000,

    /// Maximum number of evictions per `set`.
    items_to_prune: usize = 500,

    /// Number of candidates sampled per eviction.
    ///
    /// Larger values generally improve eviction quality but increase eviction cost.
    sample_size: usize = 32,

    /// Eviction policy used when over capacity.
    eviction_policy: EvictionPolicy = .sampled_lru,

    /// If true (default), expired items are treated as cache misses.
    ///
    /// When false, `get`/`peek` can return expired items.
    treat_expired_as_miss: bool = true,

    pub const BuildError = error{InvalidConfig};

    /// Validate/normalize config. Performs no allocations.
    ///
    /// # Example
    ///
    /// ```zig
    /// const cache_zig = @import("cache_zig");
    /// const cfg = try cache_zig.Config{ .shards = 16 }.build();
    /// _ = cfg;
    /// ```
    pub fn build(self: Config) BuildError!Config {
        if (self.shards == 0 or !std.math.isPowerOfTwo(self.shards)) return error.InvalidConfig;

        var out = self;
        out.items_to_prune = @max(out.items_to_prune, 1);
        out.sample_size = @max(out.sample_size, 1);
        return out;
    }
};
