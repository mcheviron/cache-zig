const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("cache_zig", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const mod_tests = b.addTest(.{ .root_module = mod });
    const run_mod_tests = b.addRunArtifact(mod_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);

    const mod_single_threaded = b.addModule("cache_zig_single_threaded", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .single_threaded = true,
    });

    const mod_single_threaded_tests = b.addTest(.{ .root_module = mod_single_threaded });
    const run_mod_single_threaded_tests = b.addRunArtifact(mod_single_threaded_tests);

    const test_single_threaded_step = b.step("test-single-threaded", "Run single-threaded tests (-fsingle-threaded)");
    test_single_threaded_step.dependOn(&run_mod_single_threaded_tests.step);
}
