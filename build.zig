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

    const bench_mod = b.createModule(.{
        .root_source_file = b.path("src/bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    const bench_exe = b.addExecutable(.{
        .name = "cache-bench",
        .root_module = bench_mod,
    });
    const run_bench = b.addRunArtifact(bench_exe);
    if (b.args) |args| {
        run_bench.addArgs(args);
    }

    const bench_step = b.step("bench", "Run benchmarks (default synthetic mode)");
    bench_step.dependOn(&run_bench.step);

    const run_bench_synth = b.addRunArtifact(bench_exe);
    run_bench_synth.addArgs(&.{ "--mode", "synthetic" });
    if (b.args) |args| {
        run_bench_synth.addArgs(args);
    }
    const bench_synth_step = b.step("bench-synth", "Run synthetic benchmark matrix");
    bench_synth_step.dependOn(&run_bench_synth.step);

    const run_bench_trace = b.addRunArtifact(bench_exe);
    run_bench_trace.addArgs(&.{ "--mode", "trace" });
    if (b.args) |args| {
        run_bench_trace.addArgs(args);
    }
    const bench_trace_step = b.step("bench-trace", "Run trace benchmark matrix");
    bench_trace_step.dependOn(&run_bench_trace.step);

    const run_bench_compare = b.addRunArtifact(bench_exe);
    run_bench_compare.addArgs(&.{ "--mode", "compare-moka" });
    if (b.args) |args| {
        run_bench_compare.addArgs(args);
    }
    const bench_compare_step = b.step("bench-compare", "Compare cache-zig results against Moka CSV baseline");
    bench_compare_step.dependOn(&run_bench_compare.step);
}
