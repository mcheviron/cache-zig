const std = @import("std");

pub const Config = struct {
    shards: usize = 16,
    max_weight: usize = 5000,
    items_to_prune: usize = 500,
    sample_size: usize = 32,

    pub const BuildError = error{InvalidConfig};

    pub fn build(self: Config) BuildError!Config {
        if (self.shards == 0 or !std.math.isPowerOfTwo(self.shards)) return error.InvalidConfig;

        var out = self;
        out.items_to_prune = @max(out.items_to_prune, 1);
        out.sample_size = @max(out.sample_size, 1);
        return out;
    }
};
