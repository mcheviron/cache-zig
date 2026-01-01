const std = @import("std");

/// Cache configuration.
///
/// Config is a plain struct (no builder pattern). Set fields directly.
/// Call `build()` to validate/normalize.
///
/// `build()` performs no allocations.
pub const Config = struct {
    pub const EvictionPolicy = enum {
        sampled_lru,
        sampled_lhd,
    };

    /// Number of shards; must be a power of two.
    shards: usize = 16,

    /// Total weight capacity before eviction.
    max_weight: usize = 5000,

    /// Maximum number of evictions per `set`.
    items_to_prune: usize = 500,

    /// Number of candidates sampled per eviction.
    sample_size: usize = 32,

    eviction_policy: EvictionPolicy = .sampled_lru,

    /// If true (default), expired items are treated as cache misses.
    treat_expired_as_miss: bool = true,

    pub const BuildError = error{InvalidConfig};

    /// Validate/normalize config. Performs no allocations.
    pub fn build(self: Config) BuildError!Config {
        if (self.shards == 0 or !std.math.isPowerOfTwo(self.shards)) return error.InvalidConfig;

        var out = self;
        out.items_to_prune = @max(out.items_to_prune, 1);
        out.sample_size = @max(out.sample_size, 1);
        return out;
    }
};
