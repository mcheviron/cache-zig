const std = @import("std");

/// Cache configuration.
///
/// Call `build()` to validate/normalize.
///
/// `build()` performs no allocations.
pub const Config = struct {
    pub const DefaultCostMode = enum {
        /// Default cost is `key.len + @sizeOf(V)`.
        bytes,

        /// Default cost is `1` (treat `max_weight` as max items).
        items,
    };

    /// Optional cost function used for capacity accounting and LHD ranking.
    ///
    /// `value` is `*const V` cast to `*const anyopaque`.
    pub const CostFn = *const fn (ctx: ?*anyopaque, key: []const u8, value: *const anyopaque) usize;

    /// Eviction strategy used when `max_weight` is exceeded.
    pub const EvictionPolicy = enum {
        /// Sample candidates and evict the least-recently accessed.
        sampled_lru,

        /// Sample candidates and evict the least hit-dense candidate.
        ///
        /// Hit density is approximated as `hits / (age * weight)`.
        sampled_lhd,

        /// True LRU eviction backed by a stable linked list.
        stable_lru,

        /// Full (non-sampled) LHD eviction driven by a maintenance worker.
        stable_lhd,
    };

    /// Number of shards; must be a power of two.
    ///
    /// Higher shard counts may improve parallelism at the cost of memory.
    shards: usize = 16,

    /// Total cost capacity before eviction.
    max_weight: usize = 5000,

    /// Default cost computation when `cost_fn` is not provided.
    default_cost_mode: DefaultCostMode = .bytes,

    /// Optional context pointer passed to `cost_fn`.
    cost_ctx: ?*anyopaque = null,

    /// Optional cost function.
    cost_fn: ?CostFn = null,

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

    /// Stable-policy promotion batching.
    ///
    /// For stable policies, the maintenance worker only moves an entry to the front
    /// after this many `get()` hits.
    gets_per_promote: usize = 1,

    /// Buffer size for promotion/touch events.
    promote_buffer: usize = 1024,

    /// Buffer size for delete events.
    delete_buffer: usize = 1024,

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
        if (self.shards == 0) return error.InvalidConfig;
        if ((self.shards & (self.shards - 1)) != 0) return error.InvalidConfig;

        var out = self;
        out.items_to_prune = @max(out.items_to_prune, 1);
        out.sample_size = @max(out.sample_size, 1);
        out.gets_per_promote = @max(out.gets_per_promote, 1);
        out.promote_buffer = @max(out.promote_buffer, 1);
        out.delete_buffer = @max(out.delete_buffer, 1);
        return out;
    }
};
