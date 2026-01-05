/// Cache eviction policy.
///
/// Shared between single-threaded and multithreaded implementations.
pub const EvictionPolicy = enum {
    sampled_lru,
    sampled_lhd,
    stable_lru,
    stable_lhd,

    pub fn isStable(self: EvictionPolicy) bool {
        return self == .stable_lru or self == .stable_lhd;
    }

    pub fn isLhd(self: EvictionPolicy) bool {
        return self == .sampled_lhd or self == .stable_lhd;
    }
};
