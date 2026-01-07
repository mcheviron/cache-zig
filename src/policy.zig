/// Cache policy.
///
/// Shared between single-threaded and multi-threaded implementations.
pub const Policy = enum {
    sampled_lru,
    sampled_lhd,
    stable_lru,
    stable_lhd,

    pub fn isStable(self: Policy) bool {
        return self == .stable_lru or self == .stable_lhd;
    }

    pub fn isLhd(self: Policy) bool {
        return self == .sampled_lhd or self == .stable_lhd;
    }
};
