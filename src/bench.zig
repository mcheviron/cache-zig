const std = @import("std");
const cache_zig = @import("root.zig");

const Allocator = std.mem.Allocator;
const Config = cache_zig.Config;
const Policy = cache_zig.Policy;
const Weigher = cache_zig.multi_threaded.weigher.Items(u64, u64);

const DEFAULT_THREADS = [_]usize{ 1, 4, 16 };
const DEFAULT_TRACE_NAMES = [_][]const u8{ "loop", "s3", "ds1", "oltp" };

const SYNTHETIC_WORKLOADS = [_]SyntheticWorkload{
    .{ .name = "get-heavy", .get_percent = 90 },
    .{ .name = "mixed", .get_percent = 50 },
    .{ .name = "set-heavy", .get_percent = 10 },
};

const SYNTHETIC_PAIRS = [_]KeyCapacityPair{
    .{ .key_space = 1_024, .capacity = 1_024 },
    .{ .key_space = 8_192, .capacity = 4_096 },
    .{ .key_space = 65_536, .capacity = 8_192 },
};

const LOOP_CAPS = [_]usize{ 256, 512, 768, 1_024 };
const S3_CAPS = [_]usize{ 100_000, 400_000, 800_000 };
const DS1_CAPS = [_]usize{ 1_000_000, 4_000_000, 8_000_000 };
const OLTP_CAPS = [_]usize{ 256, 512, 1_000, 2_000 };

const TRACE_SPECS = [_]TraceSpec{
    .{ .name = "loop", .relative_path = "lirs/loop.trc", .capacities = LOOP_CAPS[0..] },
    .{ .name = "s3", .relative_path = "arc/S3.lis", .capacities = S3_CAPS[0..] },
    .{ .name = "ds1", .relative_path = "arc/DS1.lis", .capacities = DS1_CAPS[0..] },
    .{ .name = "oltp", .relative_path = "arc/OLTP.lis", .capacities = OLTP_CAPS[0..] },
};
const BENCH_TTL_NS: u64 = 60 * std.time.ns_per_s;

const PolicyChoice = enum {
    sampled_lru,
    sampled_lhd,
    stable_lru,
    stable_lhd,
};

const Mode = enum {
    synthetic,
    trace,
    compare_moka,
};

const KeyCapacityPair = struct {
    key_space: usize,
    capacity: usize,
};

const SyntheticWorkload = struct {
    name: []const u8,
    get_percent: u8,
};

const TraceSpec = struct {
    name: []const u8,
    relative_path: []const u8,
    capacities: []const usize,
};

const TraceCommand = struct {
    start: u64,
    len: u32,
    req_id: u64,
};

const SingleRunResult = struct {
    throughput_ops_s: f64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    hit_ratio_pct: f64,
    total_ops: u64,
};

const BenchRow = struct {
    mode: []const u8,
    policy: []const u8,
    workload: []const u8,
    trace: []const u8,
    capacity: usize,
    key_space: usize,
    threads: usize,
    repetitions: u32,
    median_ops_s: f64,
    median_p50_ns: u64,
    median_p95_ns: u64,
    median_p99_ns: u64,
    median_hit_ratio_pct: f64,
    run_variance_pct: f64,
    total_ops: u64,
};

const CompareRow = struct {
    trace: []const u8,
    capacity: usize,
    clients: usize,
    cache_zig_ops_s: f64,
    moka_best_ops_s: f64,
    cache_zig_p95_ns: u64,
    moka_avg_ns: u64,
    cache_zig_hit_ratio_pct: f64,
    moka_hit_ratio_pct: f64,
    throughput_pass: bool,
    p95_guard_pass: bool,
    hit_parity_pass: bool,
    variance_pass: bool,
    overall_pass: bool,
};

const CliConfig = struct {
    mode: Mode = .synthetic,
    repetitions: u32 = 10,
    ops_per_thread: usize = 100_000,
    warmup_ops_per_thread: usize = 10_000,
    trace_warmup_entries: usize = 50_000,
    latency_sample_stride_trace: usize = 8,
    max_latency_samples_trace: usize = 1_000_000,
    output_prefix: []const u8 = ".agent/benchmarks/latest",
    trace_root: []const u8 = "",
    threads: std.ArrayList(usize),
    trace_names: std.ArrayList([]const u8),
    trace_capacity_override: ?usize = null,
    cache_zig_csv: []const u8 = "",
    moka_csv: []const u8 = "",
    variance_threshold_pct: f64 = 15.0,
};

const TraceMokaRow = struct {
    trace: []const u8,
    cache_name: []const u8,
    capacity: usize,
    clients: usize,
    reads: u64,
    hit_ratio_pct: f64,
    duration_secs: f64,

    fn throughput(self: TraceMokaRow) f64 {
        if (self.duration_secs <= 0) return 0;
        return @as(f64, @floatFromInt(self.reads)) / self.duration_secs;
    }

    fn avgLatencyNs(self: TraceMokaRow) u64 {
        if (self.reads == 0 or self.duration_secs <= 0) return 0;
        return @intFromFloat((self.duration_secs * @as(f64, std.time.ns_per_s)) / @as(f64, @floatFromInt(self.reads)));
    }
};

pub fn main() !void {
    var gpa_state: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer {
        _ = gpa_state.deinit();
    }
    const allocator = gpa_state.allocator();

    var cli = try parseCli(allocator);
    defer {
        for (cli.trace_names.items) |name| allocator.free(name);
        cli.threads.deinit(allocator);
        cli.trace_names.deinit(allocator);
    }

    switch (cli.mode) {
        .synthetic => try runSyntheticMode(allocator, &cli),
        .trace => try runTraceMode(allocator, &cli),
        .compare_moka => try runCompareMode(allocator, &cli),
    }
}

fn runSyntheticMode(allocator: Allocator, cli: *const CliConfig) !void {
    var rows: std.ArrayList(BenchRow) = .empty;
    defer rows.deinit(allocator);

    for (SYNTHETIC_WORKLOADS) |workload| {
        for (SYNTHETIC_PAIRS) |pair| {
            for (cli.threads.items) |threads| {
                inline for ([_]PolicyChoice{ .sampled_lru, .sampled_lhd, .stable_lru, .stable_lhd }) |policy_choice| {
                    const row = switch (policy_choice) {
                        .sampled_lru => try runSyntheticCell(.sampled_lru, allocator, cli, workload, pair, threads),
                        .sampled_lhd => try runSyntheticCell(.sampled_lhd, allocator, cli, workload, pair, threads),
                        .stable_lru => try runSyntheticCell(.stable_lru, allocator, cli, workload, pair, threads),
                        .stable_lhd => try runSyntheticCell(.stable_lhd, allocator, cli, workload, pair, threads),
                    };
                    try rows.append(allocator, row);
                }
            }
        }
    }

    try writeBenchRows(allocator, cli.output_prefix, rows.items);
    printBenchSummary("synthetic", rows.items);
}

fn runTraceMode(allocator: Allocator, cli: *const CliConfig) !void {
    var rows: std.ArrayList(BenchRow) = .empty;
    defer rows.deinit(allocator);

    const trace_root = try resolveTraceRoot(allocator, cli.trace_root);
    defer allocator.free(trace_root);

    for (cli.trace_names.items) |trace_name| {
        const spec = traceSpecByName(trace_name) orelse return error.UnknownTraceName;
        const trace_path = try std.fs.path.join(allocator, &.{ trace_root, spec.relative_path });
        defer allocator.free(trace_path);

        const commands = try loadTraceCommands(allocator, trace_path);
        defer allocator.free(commands);

        const capacities = if (cli.trace_capacity_override) |override| &[_]usize{override} else spec.capacities;
        for (capacities) |capacity| {
            for (cli.threads.items) |threads| {
                inline for ([_]PolicyChoice{ .sampled_lru, .sampled_lhd, .stable_lru, .stable_lhd }) |policy_choice| {
                    const row = switch (policy_choice) {
                        .sampled_lru => try runTraceCell(.sampled_lru, allocator, cli, spec.name, commands, capacity, threads),
                        .sampled_lhd => try runTraceCell(.sampled_lhd, allocator, cli, spec.name, commands, capacity, threads),
                        .stable_lru => try runTraceCell(.stable_lru, allocator, cli, spec.name, commands, capacity, threads),
                        .stable_lhd => try runTraceCell(.stable_lhd, allocator, cli, spec.name, commands, capacity, threads),
                    };
                    try rows.append(allocator, row);
                }
            }
        }
    }

    try writeBenchRows(allocator, cli.output_prefix, rows.items);
    printBenchSummary("trace", rows.items);
}

fn runCompareMode(allocator: Allocator, cli: *const CliConfig) !void {
    if (cli.cache_zig_csv.len == 0) return error.MissingCacheZigCsv;
    if (cli.moka_csv.len == 0) return error.MissingMokaCsv;

    const cache_rows = try parseCacheBenchRowsFromCsv(allocator, cli.cache_zig_csv);
    defer {
        for (cache_rows) |row| {
            allocator.free(row.mode);
            allocator.free(row.policy);
            allocator.free(row.workload);
            allocator.free(row.trace);
        }
        allocator.free(cache_rows);
    }

    const moka_rows = try parseMokaRowsFromCsv(allocator, cli.moka_csv);
    defer {
        for (moka_rows) |row| {
            allocator.free(row.trace);
            allocator.free(row.cache_name);
        }
        allocator.free(moka_rows);
    }

    var compare_rows: std.ArrayList(CompareRow) = .empty;
    defer compare_rows.deinit(allocator);

    var has_failure = false;
    for (cache_rows) |row| {
        if (!std.mem.eql(u8, row.mode, "trace")) continue;
        if (!std.mem.eql(u8, row.policy, "sampled_lru")) continue;

        const moka_best = bestMokaForCell(moka_rows, row.trace, row.capacity, row.threads) orelse continue;

        const throughput_pass = row.median_ops_s >= moka_best.throughput();
        const moka_avg_ns = moka_best.avgLatencyNs();
        const p95_guard_pass = if (moka_avg_ns == 0)
            false
        else
            row.median_p95_ns <= @as(u64, @intFromFloat(@as(f64, @floatFromInt(moka_avg_ns)) * 1.20));
        const hit_delta = @abs(row.median_hit_ratio_pct - moka_best.hit_ratio_pct);
        const hit_parity_pass = hit_delta <= 0.5;
        const variance_pass = row.run_variance_pct <= cli.variance_threshold_pct;
        const overall_pass = throughput_pass and p95_guard_pass and hit_parity_pass and variance_pass;
        has_failure = has_failure or !overall_pass;

        try compare_rows.append(allocator, .{
            .trace = row.trace,
            .capacity = row.capacity,
            .clients = row.threads,
            .cache_zig_ops_s = row.median_ops_s,
            .moka_best_ops_s = moka_best.throughput(),
            .cache_zig_p95_ns = row.median_p95_ns,
            .moka_avg_ns = moka_avg_ns,
            .cache_zig_hit_ratio_pct = row.median_hit_ratio_pct,
            .moka_hit_ratio_pct = moka_best.hit_ratio_pct,
            .throughput_pass = throughput_pass,
            .p95_guard_pass = p95_guard_pass,
            .hit_parity_pass = hit_parity_pass,
            .variance_pass = variance_pass,
            .overall_pass = overall_pass,
        });
    }

    try writeCompareRows(allocator, cli.output_prefix, compare_rows.items);
    printCompareSummary(compare_rows.items);

    if (has_failure) return error.PerfGateFailed;
}

fn runSyntheticCell(
    comptime policy: Policy,
    allocator: Allocator,
    cli: *const CliConfig,
    workload: SyntheticWorkload,
    pair: KeyCapacityPair,
    threads: usize,
) !BenchRow {
    var runs = try allocator.alloc(SingleRunResult, cli.repetitions);
    defer allocator.free(runs);

    var rep: u32 = 0;
    while (rep < cli.repetitions) : (rep += 1) {
        runs[rep] = try runSyntheticOnce(
            policy,
            allocator,
            workload,
            pair.key_space,
            pair.capacity,
            threads,
            cli.ops_per_thread,
            cli.warmup_ops_per_thread,
            @as(u64, rep) * 0x9e37_79b9_7f4a_7c15,
        );
    }

    const summary = summarizeRuns(allocator, runs);
    return .{
        .mode = "synthetic",
        .policy = policyLabel(policy),
        .workload = workload.name,
        .trace = "-",
        .capacity = pair.capacity,
        .key_space = pair.key_space,
        .threads = threads,
        .repetitions = cli.repetitions,
        .median_ops_s = summary.median_ops_s,
        .median_p50_ns = summary.median_p50_ns,
        .median_p95_ns = summary.median_p95_ns,
        .median_p99_ns = summary.median_p99_ns,
        .median_hit_ratio_pct = summary.median_hit_ratio_pct,
        .run_variance_pct = summary.variance_pct,
        .total_ops = summary.total_ops,
    };
}

fn runTraceCell(
    comptime policy: Policy,
    allocator: Allocator,
    cli: *const CliConfig,
    trace_name: []const u8,
    commands: []const TraceCommand,
    capacity: usize,
    threads: usize,
) !BenchRow {
    var runs = try allocator.alloc(SingleRunResult, cli.repetitions);
    defer allocator.free(runs);

    var rep: u32 = 0;
    while (rep < cli.repetitions) : (rep += 1) {
        runs[rep] = try runTraceOnce(
            policy,
            allocator,
            commands,
            capacity,
            threads,
            cli.trace_warmup_entries,
            cli.latency_sample_stride_trace,
            cli.max_latency_samples_trace,
        );
    }

    const summary = summarizeRuns(allocator, runs);
    return .{
        .mode = "trace",
        .policy = policyLabel(policy),
        .workload = trace_name,
        .trace = trace_name,
        .capacity = capacity,
        .key_space = 0,
        .threads = threads,
        .repetitions = cli.repetitions,
        .median_ops_s = summary.median_ops_s,
        .median_p50_ns = summary.median_p50_ns,
        .median_p95_ns = summary.median_p95_ns,
        .median_p99_ns = summary.median_p99_ns,
        .median_hit_ratio_pct = summary.median_hit_ratio_pct,
        .run_variance_pct = summary.variance_pct,
        .total_ops = summary.total_ops,
    };
}

fn runSyntheticOnce(
    comptime policy: Policy,
    allocator: Allocator,
    workload: SyntheticWorkload,
    key_space: usize,
    capacity: usize,
    thread_count: usize,
    measure_ops_per_thread: usize,
    warmup_ops_per_thread: usize,
    seed_base: u64,
) !SingleRunResult {
    const CacheT = cache_zig.multi_threaded.Cache(u64, u64, policy, Weigher);

    var cache = try CacheT.init(allocator, Config{
        .shard_count = 64,
        .max_weight = @max(capacity, 1),
        .items_to_prune = 1_024,
        .sample_size = 32,
        .enable_tiny_lfu = true,
        .sketch_stripe_count = 64,
        .eviction_batch_size = 16,
        .eviction_sampling_factor_small_cache = 4,
        .admission_victim_attempt_factor = 4,
    });
    defer cache.deinit();

    var i: usize = 0;
    while (i < capacity) : (i += 1) {
        var set_result = try cache.set(@intCast(i), @intCast(i), BENCH_TTL_NS);
        set_result.deinit();
    }

    const total_measure_ops = thread_count * measure_ops_per_thread;
    var all_latencies = try allocator.alloc(u64, total_measure_ops);
    defer allocator.free(all_latencies);

    const WorkerCtx = struct {
        cache: *CacheT,
        start_gate: *std.Thread.ResetEvent,
        get_percent: u8,
        key_space: u64,
        warmup_ops: usize,
        measure_ops: usize,
        seed: u64,
        latencies: []u64,
        reads: u64 = 0,
        hits: u64 = 0,
    };

    const Worker = struct {
        fn run(ctx: *WorkerCtx) void {
            var state = ctx.seed;

            var warmup: usize = 0;
            while (warmup < ctx.warmup_ops) : (warmup += 1) {
                const key = nextRand(&state) % ctx.key_space;
                const op_selector: u8 = @intCast(nextRand(&state) % 100);
                if (op_selector < ctx.get_percent) {
                    if (ctx.cache.get(key)) |item| {
                        item.deinit();
                    }
                } else {
                    var set_result = ctx.cache.set(key, nextRand(&state), BENCH_TTL_NS) catch continue;
                    set_result.deinit();
                }
            }

            ctx.start_gate.wait();
            var m: usize = 0;
            while (m < ctx.measure_ops) : (m += 1) {
                const op_start = std.time.nanoTimestamp();
                const key = nextRand(&state) % ctx.key_space;
                const op_selector: u8 = @intCast(nextRand(&state) % 100);

                if (op_selector < ctx.get_percent) {
                    ctx.reads += 1;
                    if (ctx.cache.get(key)) |item| {
                        ctx.hits += 1;
                        item.deinit();
                    }
                } else {
                    ctx.reads += 1;
                    var set_result = ctx.cache.set(key, nextRand(&state), BENCH_TTL_NS) catch {
                        ctx.latencies[m] = 1;
                        continue;
                    };
                    set_result.deinit();
                }

                const op_end = std.time.nanoTimestamp();
                ctx.latencies[m] = @intCast(@max(op_end - op_start, 1));
            }
        }
    };

    var contexts = try allocator.alloc(WorkerCtx, thread_count);
    defer allocator.free(contexts);
    var threads = try allocator.alloc(std.Thread, thread_count);
    defer allocator.free(threads);

    var start_gate = std.Thread.ResetEvent{};
    start_gate.reset();

    var t: usize = 0;
    while (t < thread_count) : (t += 1) {
        const start = t * measure_ops_per_thread;
        const end = start + measure_ops_per_thread;
        contexts[t] = .{
            .cache = &cache,
            .start_gate = &start_gate,
            .get_percent = workload.get_percent,
            .key_space = @intCast(@max(key_space, 1)),
            .warmup_ops = warmup_ops_per_thread,
            .measure_ops = measure_ops_per_thread,
            .seed = seed_base +% @as(u64, @intCast((t + 1) * 97)),
            .latencies = all_latencies[start..end],
        };
        threads[t] = try std.Thread.spawn(.{}, Worker.run, .{&contexts[t]});
    }

    const start_ns = std.time.nanoTimestamp();
    start_gate.set();
    for (threads) |th| th.join();
    cache.sync();
    const end_ns = std.time.nanoTimestamp();
    const elapsed_ns: u64 = @intCast(@max(end_ns - start_ns, 1));

    var reads: u64 = 0;
    var hits: u64 = 0;
    for (contexts) |ctx| {
        reads += ctx.reads;
        hits += ctx.hits;
    }

    return .{
        .throughput_ops_s = (@as(f64, @floatFromInt(total_measure_ops)) * @as(f64, std.time.ns_per_s)) /
            @as(f64, @floatFromInt(elapsed_ns)),
        .p50_ns = quantileU64(all_latencies, 50),
        .p95_ns = quantileU64(all_latencies, 95),
        .p99_ns = quantileU64(all_latencies, 99),
        .hit_ratio_pct = if (reads == 0) 0 else (@as(f64, @floatFromInt(hits)) * 100.0) / @as(f64, @floatFromInt(reads)),
        .total_ops = @intCast(total_measure_ops),
    };
}

fn runTraceOnce(
    comptime policy: Policy,
    allocator: Allocator,
    commands: []const TraceCommand,
    capacity: usize,
    thread_count: usize,
    warmup_entries: usize,
    latency_sample_stride: usize,
    max_latency_samples: usize,
) !SingleRunResult {
    const CacheT = cache_zig.multi_threaded.Cache(u64, u64, policy, Weigher);

    var cache = try CacheT.init(allocator, Config{
        .shard_count = 64,
        .max_weight = @max(capacity, 1),
        .items_to_prune = 1_024,
        .sample_size = 64,
        .enable_tiny_lfu = true,
        .sketch_stripe_count = 64,
        .eviction_batch_size = 16,
        .eviction_sampling_factor_small_cache = 4,
        .admission_victim_attempt_factor = 4,
    });
    defer cache.deinit();

    const warmup_count = @min(commands.len, warmup_entries);
    var i: usize = 0;
    while (i < warmup_count) : (i += 1) {
        const cmd = commands[i];
        var off: u32 = 0;
        while (off < cmd.len) : (off += 1) {
            const key = cmd.start + off;
            if (cache.get(key)) |item| {
                item.deinit();
            } else {
                var set_result = cache.set(key, cmd.req_id + off, BENCH_TTL_NS) catch continue;
                set_result.deinit();
            }
        }
    }

    const measured = commands[warmup_count..];

    const WorkerCtx = struct {
        cache: *CacheT,
        start_gate: *std.Thread.ResetEvent,
        next_index: *std.atomic.Value(usize),
        commands: []const TraceCommand,
        sample_stride: usize,
        sample_cap: usize,
        allocator: Allocator,
        latencies: std.ArrayListUnmanaged(u64) = .{},
        reads: u64 = 0,
        hits: u64 = 0,
    };

    const Worker = struct {
        fn run(ctx: *WorkerCtx) void {
            ctx.latencies.ensureTotalCapacity(ctx.allocator, ctx.sample_cap) catch {};
            ctx.start_gate.wait();

            var op_counter: u64 = 0;
            while (true) {
                const idx = ctx.next_index.fetchAdd(1, .acq_rel);
                if (idx >= ctx.commands.len) break;

                const cmd = ctx.commands[idx];
                var off: u32 = 0;
                while (off < cmd.len) : (off += 1) {
                    const op_start = std.time.nanoTimestamp();
                    const key = cmd.start + off;
                    ctx.reads += 1;
                    if (ctx.cache.get(key)) |item| {
                        ctx.hits += 1;
                        item.deinit();
                    } else {
                        var set_result = ctx.cache.set(key, cmd.req_id + off, BENCH_TTL_NS) catch {
                            op_counter += 1;
                            continue;
                        };
                        set_result.deinit();
                    }

                    if (ctx.sample_stride > 0 and ctx.latencies.items.len < ctx.sample_cap and (op_counter % ctx.sample_stride == 0)) {
                        const op_end = std.time.nanoTimestamp();
                        ctx.latencies.appendAssumeCapacity(@intCast(@max(op_end - op_start, 1)));
                    }
                    op_counter += 1;
                }
            }
        }
    };

    var next_index: std.atomic.Value(usize) = std.atomic.Value(usize).init(0);
    var start_gate = std.Thread.ResetEvent{};
    start_gate.reset();

    var contexts = try allocator.alloc(WorkerCtx, thread_count);
    defer {
        for (contexts) |*ctx| {
            ctx.latencies.deinit(allocator);
        }
        allocator.free(contexts);
    }
    var workers = try allocator.alloc(std.Thread, thread_count);
    defer allocator.free(workers);

    const per_worker_samples = @max(max_latency_samples / @max(thread_count, 1), 1);

    var t: usize = 0;
    while (t < thread_count) : (t += 1) {
        contexts[t] = .{
            .cache = &cache,
            .start_gate = &start_gate,
            .next_index = &next_index,
            .commands = measured,
            .sample_stride = @max(latency_sample_stride, 1),
            .sample_cap = per_worker_samples,
            .allocator = allocator,
        };
        workers[t] = try std.Thread.spawn(.{}, Worker.run, .{&contexts[t]});
    }

    const start_ns = std.time.nanoTimestamp();
    start_gate.set();
    for (workers) |w| w.join();
    cache.sync();
    const end_ns = std.time.nanoTimestamp();
    const elapsed_ns: u64 = @intCast(@max(end_ns - start_ns, 1));

    var reads: u64 = 0;
    var hits: u64 = 0;
    var sample_count: usize = 0;
    for (contexts) |ctx| {
        reads += ctx.reads;
        hits += ctx.hits;
        sample_count += ctx.latencies.items.len;
    }

    var all_samples = try allocator.alloc(u64, @max(sample_count, 1));
    defer allocator.free(all_samples);
    var cursor: usize = 0;
    for (contexts) |ctx| {
        @memcpy(all_samples[cursor .. cursor + ctx.latencies.items.len], ctx.latencies.items);
        cursor += ctx.latencies.items.len;
    }
    if (cursor == 0) {
        all_samples[0] = 1;
        cursor = 1;
    }

    return .{
        .throughput_ops_s = (@as(f64, @floatFromInt(reads)) * @as(f64, std.time.ns_per_s)) /
            @as(f64, @floatFromInt(elapsed_ns)),
        .p50_ns = quantileU64(all_samples[0..cursor], 50),
        .p95_ns = quantileU64(all_samples[0..cursor], 95),
        .p99_ns = quantileU64(all_samples[0..cursor], 99),
        .hit_ratio_pct = if (reads == 0) 0 else (@as(f64, @floatFromInt(hits)) * 100.0) / @as(f64, @floatFromInt(reads)),
        .total_ops = reads,
    };
}

fn summarizeRuns(allocator: Allocator, runs: []const SingleRunResult) struct {
    median_ops_s: f64,
    median_p50_ns: u64,
    median_p95_ns: u64,
    median_p99_ns: u64,
    median_hit_ratio_pct: f64,
    variance_pct: f64,
    total_ops: u64,
} {
    var ops = allocator.alloc(f64, runs.len) catch unreachable;
    defer allocator.free(ops);
    var p50 = allocator.alloc(u64, runs.len) catch unreachable;
    defer allocator.free(p50);
    var p95 = allocator.alloc(u64, runs.len) catch unreachable;
    defer allocator.free(p95);
    var p99 = allocator.alloc(u64, runs.len) catch unreachable;
    defer allocator.free(p99);
    var hit = allocator.alloc(f64, runs.len) catch unreachable;
    defer allocator.free(hit);

    var min_ops: f64 = std.math.inf(f64);
    var max_ops: f64 = 0;

    var total_ops: u64 = 0;
    for (runs, 0..) |r, idx| {
        ops[idx] = r.throughput_ops_s;
        p50[idx] = r.p50_ns;
        p95[idx] = r.p95_ns;
        p99[idx] = r.p99_ns;
        hit[idx] = r.hit_ratio_pct;
        min_ops = @min(min_ops, r.throughput_ops_s);
        max_ops = @max(max_ops, r.throughput_ops_s);
        total_ops = r.total_ops;
    }

    const median_ops = medianF64(ops);
    const variance_pct = if (median_ops <= 0) 0 else ((max_ops - min_ops) / median_ops) * 100.0;

    return .{
        .median_ops_s = median_ops,
        .median_p50_ns = medianU64(p50),
        .median_p95_ns = medianU64(p95),
        .median_p99_ns = medianU64(p99),
        .median_hit_ratio_pct = medianF64(hit),
        .variance_pct = variance_pct,
        .total_ops = total_ops,
    };
}

fn quantileU64(values_in: []u64, percentile: u8) u64 {
    std.sort.heap(u64, values_in, {}, comptime std.sort.asc(u64));
    const idx = if (values_in.len == 0) 0 else @min((values_in.len * percentile) / 100, values_in.len - 1);
    return values_in[idx];
}

fn medianU64(values_in: []u64) u64 {
    std.sort.heap(u64, values_in, {}, comptime std.sort.asc(u64));
    return values_in[values_in.len / 2];
}

fn medianF64(values_in: []f64) f64 {
    std.sort.heap(f64, values_in, {}, comptime std.sort.asc(f64));
    const mid = values_in.len / 2;
    if (values_in.len % 2 == 0 and values_in.len > 1) {
        return (values_in[mid - 1] + values_in[mid]) / 2.0;
    }
    return values_in[mid];
}

fn writeBenchRows(allocator: Allocator, prefix: []const u8, rows: []const BenchRow) !void {
    const csv_path = try std.fmt.allocPrint(allocator, "{s}.csv", .{prefix});
    defer allocator.free(csv_path);
    const json_path = try std.fmt.allocPrint(allocator, "{s}.json", .{prefix});
    defer allocator.free(json_path);

    try ensureParentDir(csv_path);
    try ensureParentDir(json_path);

    {
        var file = try std.fs.cwd().createFile(csv_path, .{ .truncate = true });
        defer file.close();
        var buffer: [4096]u8 = undefined;
        var fw = file.writer(&buffer);
        const w = &fw.interface;

        try w.print(
            "mode,policy,workload,trace,capacity,key_space,threads,repetitions,median_ops_s,median_p50_ns,median_p95_ns,median_p99_ns,median_hit_ratio_pct,run_variance_pct,total_ops\n",
            .{},
        );
        for (rows) |row| {
            try w.print(
                "{s},{s},{s},{s},{d},{d},{d},{d},{d:.3},{d},{d},{d},{d:.4},{d:.4},{d}\n",
                .{
                    row.mode,
                    row.policy,
                    row.workload,
                    row.trace,
                    row.capacity,
                    row.key_space,
                    row.threads,
                    row.repetitions,
                    row.median_ops_s,
                    row.median_p50_ns,
                    row.median_p95_ns,
                    row.median_p99_ns,
                    row.median_hit_ratio_pct,
                    row.run_variance_pct,
                    row.total_ops,
                },
            );
        }
        try fw.end();
    }

    {
        var file = try std.fs.cwd().createFile(json_path, .{ .truncate = true });
        defer file.close();
        var buffer: [4096]u8 = undefined;
        var fw = file.writer(&buffer);
        try std.json.Stringify.value(rows, .{ .whitespace = .indent_2 }, &fw.interface);
        try fw.end();
    }
}

fn writeCompareRows(allocator: Allocator, prefix: []const u8, rows: []const CompareRow) !void {
    const csv_path = try std.fmt.allocPrint(allocator, "{s}.csv", .{prefix});
    defer allocator.free(csv_path);
    const json_path = try std.fmt.allocPrint(allocator, "{s}.json", .{prefix});
    defer allocator.free(json_path);

    try ensureParentDir(csv_path);
    try ensureParentDir(json_path);

    {
        var file = try std.fs.cwd().createFile(csv_path, .{ .truncate = true });
        defer file.close();
        var buffer: [4096]u8 = undefined;
        var fw = file.writer(&buffer);
        const w = &fw.interface;

        try w.print(
            "trace,capacity,clients,cache_zig_ops_s,moka_best_ops_s,cache_zig_p95_ns,moka_avg_ns,cache_zig_hit_ratio_pct,moka_hit_ratio_pct,throughput_pass,p95_guard_pass,hit_parity_pass,variance_pass,overall_pass\n",
            .{},
        );
        for (rows) |row| {
            try w.print(
                "{s},{d},{d},{d:.3},{d:.3},{d},{d},{d:.4},{d:.4},{any},{any},{any},{any},{any}\n",
                .{
                    row.trace,
                    row.capacity,
                    row.clients,
                    row.cache_zig_ops_s,
                    row.moka_best_ops_s,
                    row.cache_zig_p95_ns,
                    row.moka_avg_ns,
                    row.cache_zig_hit_ratio_pct,
                    row.moka_hit_ratio_pct,
                    row.throughput_pass,
                    row.p95_guard_pass,
                    row.hit_parity_pass,
                    row.variance_pass,
                    row.overall_pass,
                },
            );
        }
        try fw.end();
    }

    {
        var file = try std.fs.cwd().createFile(json_path, .{ .truncate = true });
        defer file.close();
        var buffer: [4096]u8 = undefined;
        var fw = file.writer(&buffer);
        try std.json.Stringify.value(rows, .{ .whitespace = .indent_2 }, &fw.interface);
        try fw.end();
    }
}

fn parseCacheBenchRowsFromCsv(allocator: Allocator, path: []const u8) ![]BenchRow {
    const contents = try std.fs.cwd().readFileAlloc(allocator, path, 256 * 1024 * 1024);
    defer allocator.free(contents);

    var out: std.ArrayList(BenchRow) = .empty;
    errdefer out.deinit(allocator);

    var lines = std.mem.splitScalar(u8, contents, '\n');
    var is_header = true;
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \r\t");
        if (line.len == 0) continue;
        if (is_header) {
            is_header = false;
            continue;
        }

        var cols: [15][]const u8 = undefined;
        var count: usize = 0;
        var parts = std.mem.splitScalar(u8, line, ',');
        while (parts.next()) |part| {
            if (count >= cols.len) break;
            cols[count] = std.mem.trim(u8, part, " \r\t");
            count += 1;
        }
        if (count < cols.len) continue;

        try out.append(allocator, .{
            .mode = try allocator.dupe(u8, cols[0]),
            .policy = try allocator.dupe(u8, cols[1]),
            .workload = try allocator.dupe(u8, cols[2]),
            .trace = try allocator.dupe(u8, cols[3]),
            .capacity = try std.fmt.parseInt(usize, cols[4], 10),
            .key_space = try std.fmt.parseInt(usize, cols[5], 10),
            .threads = try std.fmt.parseInt(usize, cols[6], 10),
            .repetitions = try std.fmt.parseInt(u32, cols[7], 10),
            .median_ops_s = try std.fmt.parseFloat(f64, cols[8]),
            .median_p50_ns = try std.fmt.parseInt(u64, cols[9], 10),
            .median_p95_ns = try std.fmt.parseInt(u64, cols[10], 10),
            .median_p99_ns = try std.fmt.parseInt(u64, cols[11], 10),
            .median_hit_ratio_pct = try std.fmt.parseFloat(f64, cols[12]),
            .run_variance_pct = try std.fmt.parseFloat(f64, cols[13]),
            .total_ops = try std.fmt.parseInt(u64, cols[14], 10),
        });
    }

    return out.toOwnedSlice(allocator);
}

fn parseMokaRowsFromCsv(allocator: Allocator, path: []const u8) ![]TraceMokaRow {
    const contents = try std.fs.cwd().readFileAlloc(allocator, path, 128 * 1024 * 1024);
    defer allocator.free(contents);

    var out: std.ArrayList(TraceMokaRow) = .empty;
    errdefer out.deinit(allocator);

    var lines = std.mem.splitScalar(u8, contents, '\n');
    var is_header = true;
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \r\t");
        if (line.len == 0) continue;
        if (is_header) {
            is_header = false;
            continue;
        }

        var cols: [7][]const u8 = undefined;
        var count: usize = 0;
        var parts = std.mem.splitScalar(u8, line, ',');
        while (parts.next()) |part| {
            if (count >= cols.len) break;
            cols[count] = std.mem.trim(u8, part, " \r\t");
            count += 1;
        }
        if (count < cols.len) continue;

        try out.append(allocator, .{
            .trace = try allocator.dupe(u8, cols[0]),
            .cache_name = try allocator.dupe(u8, cols[1]),
            .capacity = try std.fmt.parseInt(usize, cols[2], 10),
            .clients = try std.fmt.parseInt(usize, cols[3], 10),
            .reads = try std.fmt.parseInt(u64, cols[4], 10),
            .hit_ratio_pct = try std.fmt.parseFloat(f64, cols[5]),
            .duration_secs = try std.fmt.parseFloat(f64, cols[6]),
        });
    }

    return out.toOwnedSlice(allocator);
}

fn bestMokaForCell(rows: []const TraceMokaRow, trace: []const u8, capacity: usize, clients: usize) ?TraceMokaRow {
    var found: ?TraceMokaRow = null;
    for (rows) |row| {
        if (!std.mem.eql(u8, row.trace, trace)) continue;
        if (row.capacity != capacity) continue;
        if (row.clients != clients) continue;
        if (!std.mem.startsWith(u8, row.cache_name, "Moka Sync Cache") and !std.mem.startsWith(u8, row.cache_name, "Moka SegmentedCache")) {
            continue;
        }

        if (found == null or row.throughput() > found.?.throughput()) {
            found = row;
        }
    }
    return found;
}

fn resolveTraceRoot(allocator: Allocator, configured: []const u8) ![]u8 {
    if (configured.len != 0) {
        return allocator.dupe(u8, configured);
    }

    const candidates = [_][]const u8{
        "cache-trace",
        "tmp/mokabench/cache-trace",
        "/tmp/mokabench/cache-trace",
    };
    for (candidates) |path| {
        if (std.fs.cwd().access(path, .{})) |_| {
            return allocator.dupe(u8, path);
        } else |_| {}
    }
    return error.TraceRootNotFound;
}

fn loadTraceCommands(allocator: Allocator, path: []const u8) ![]TraceCommand {
    const raw = try std.fs.cwd().readFileAlloc(allocator, path, 2 * 1024 * 1024 * 1024);
    defer allocator.free(raw);

    var list: std.ArrayList(TraceCommand) = .empty;
    errdefer list.deinit(allocator);

    var line_it = std.mem.splitScalar(u8, raw, '\n');
    var line_no: u64 = 0;
    while (line_it.next()) |line_raw| : (line_no += 1) {
        const line = std.mem.trim(u8, line_raw, " \r\t");
        if (line.len == 0) continue;
        if (std.mem.eql(u8, line, "*")) continue;

        var tok = std.mem.tokenizeAny(u8, line, " \t");
        const first = tok.next() orelse continue;
        const start = try std.fmt.parseInt(u64, first, 10);
        const len = if (tok.next()) |second|
            try std.fmt.parseInt(u32, second, 10)
        else
            1;
        try list.append(allocator, .{
            .start = start,
            .len = @max(len, 1),
            .req_id = line_no + 1,
        });
    }

    return list.toOwnedSlice(allocator);
}

fn traceSpecByName(name: []const u8) ?TraceSpec {
    for (TRACE_SPECS) |spec| {
        if (std.mem.eql(u8, spec.name, name)) return spec;
    }
    return null;
}

fn parseCli(allocator: Allocator) !CliConfig {
    const args = try std.process.argsAlloc(allocator);

    var cli = CliConfig{
        .threads = std.ArrayList(usize).empty,
        .trace_names = std.ArrayList([]const u8).empty,
    };
    errdefer {
        cli.threads.deinit(allocator);
        cli.trace_names.deinit(allocator);
    }

    try cli.threads.appendSlice(allocator, DEFAULT_THREADS[0..]);
    for (DEFAULT_TRACE_NAMES) |name| {
        try cli.trace_names.append(allocator, try allocator.dupe(u8, name));
    }

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--mode")) {
            i += 1;
            if (i >= args.len) break;
            cli.mode = parseMode(args[i]);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ops-per-thread")) {
            i += 1;
            if (i >= args.len) break;
            cli.ops_per_thread = try std.fmt.parseInt(usize, args[i], 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--warmup-ops-per-thread")) {
            i += 1;
            if (i >= args.len) break;
            cli.warmup_ops_per_thread = try std.fmt.parseInt(usize, args[i], 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--trace-warmup-entries")) {
            i += 1;
            if (i >= args.len) break;
            cli.trace_warmup_entries = try std.fmt.parseInt(usize, args[i], 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--latency-sample-stride-trace")) {
            i += 1;
            if (i >= args.len) break;
            cli.latency_sample_stride_trace = try std.fmt.parseInt(usize, args[i], 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--max-latency-samples-trace")) {
            i += 1;
            if (i >= args.len) break;
            cli.max_latency_samples_trace = try std.fmt.parseInt(usize, args[i], 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--repetitions")) {
            i += 1;
            if (i >= args.len) break;
            cli.repetitions = try std.fmt.parseInt(u32, args[i], 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--output-prefix")) {
            i += 1;
            if (i >= args.len) break;
            cli.output_prefix = args[i];
            continue;
        }
        if (std.mem.eql(u8, arg, "--cache-trace-root")) {
            i += 1;
            if (i >= args.len) break;
            cli.trace_root = args[i];
            continue;
        }
        if (std.mem.eql(u8, arg, "--threads")) {
            i += 1;
            if (i >= args.len) break;
            cli.threads.clearRetainingCapacity();
            try parseCsvUsize(allocator, args[i], &cli.threads);
            continue;
        }
        if (std.mem.eql(u8, arg, "--trace-file")) {
            i += 1;
            if (i >= args.len) break;
            for (cli.trace_names.items) |name| allocator.free(name);
            cli.trace_names.clearRetainingCapacity();
            try parseCsvStrings(allocator, args[i], &cli.trace_names);
            continue;
        }
        if (std.mem.eql(u8, arg, "--capacity")) {
            i += 1;
            if (i >= args.len) break;
            cli.trace_capacity_override = try std.fmt.parseInt(usize, args[i], 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--cache-zig-csv")) {
            i += 1;
            if (i >= args.len) break;
            cli.cache_zig_csv = args[i];
            continue;
        }
        if (std.mem.eql(u8, arg, "--moka-csv")) {
            i += 1;
            if (i >= args.len) break;
            cli.moka_csv = args[i];
            continue;
        }
        if (std.mem.eql(u8, arg, "--variance-threshold")) {
            i += 1;
            if (i >= args.len) break;
            cli.variance_threshold_pct = try std.fmt.parseFloat(f64, args[i]);
            continue;
        }
    }

    cli.repetitions = @max(cli.repetitions, 1);
    cli.ops_per_thread = @max(cli.ops_per_thread, 1);
    cli.warmup_ops_per_thread = @max(cli.warmup_ops_per_thread, 1);
    cli.trace_warmup_entries = @max(cli.trace_warmup_entries, 1);
    cli.latency_sample_stride_trace = @max(cli.latency_sample_stride_trace, 1);
    cli.max_latency_samples_trace = @max(cli.max_latency_samples_trace, 1);
    if (cli.threads.items.len == 0) try cli.threads.append(allocator, 1);
    if (cli.trace_names.items.len == 0) try cli.trace_names.append(allocator, try allocator.dupe(u8, "loop"));
    return cli;
}

fn parseCsvUsize(allocator: Allocator, raw: []const u8, out: *std.ArrayList(usize)) !void {
    var parts = std.mem.splitScalar(u8, raw, ',');
    while (parts.next()) |part_raw| {
        const part = std.mem.trim(u8, part_raw, " \r\t");
        if (part.len == 0) continue;
        try out.append(allocator, try std.fmt.parseInt(usize, part, 10));
    }
}

fn parseCsvStrings(allocator: Allocator, raw: []const u8, out: *std.ArrayList([]const u8)) !void {
    var parts = std.mem.splitScalar(u8, raw, ',');
    while (parts.next()) |part_raw| {
        const part = std.mem.trim(u8, part_raw, " \r\t");
        if (part.len == 0) continue;
        try out.append(allocator, try allocator.dupe(u8, part));
    }
}

fn parseMode(raw: []const u8) Mode {
    if (std.mem.eql(u8, raw, "trace")) return .trace;
    if (std.mem.eql(u8, raw, "compare-moka")) return .compare_moka;
    return .synthetic;
}

fn policyLabel(policy: Policy) []const u8 {
    return switch (policy) {
        .sampled_lru => "sampled_lru",
        .sampled_lhd => "sampled_lhd",
        .stable_lru => "stable_lru",
        .stable_lhd => "stable_lhd",
    };
}

fn nextRand(state: *u64) u64 {
    var x = state.*;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    x *%= 0x2545_F491_4F6C_DD1D;
    state.* = x;
    return x;
}

fn ensureParentDir(path: []const u8) !void {
    if (std.fs.path.dirname(path)) |parent| {
        if (parent.len != 0) {
            try std.fs.cwd().makePath(parent);
        }
    }
}

fn printBenchSummary(mode_name: []const u8, rows: []const BenchRow) void {
    std.debug.print("cache-zig {s} benchmark rows: {d}\n", .{ mode_name, rows.len });
    std.debug.print(
        "{s: <10} {s: <11} {s: <10} {s: >7} {s: >12} {s: >10} {s: >9}\n",
        .{ "mode", "policy", "workload", "threads", "median ops/s", "p95(ns)", "hit(%)" },
    );
    for (rows) |row| {
        std.debug.print(
            "{s: <10} {s: <11} {s: <10} {d: >7} {d: >12.0} {d: >10} {d: >9.3}\n",
            .{
                row.mode,
                row.policy,
                if (std.mem.eql(u8, row.trace, "-")) row.workload else row.trace,
                row.threads,
                row.median_ops_s,
                row.median_p95_ns,
                row.median_hit_ratio_pct,
            },
        );
    }
}

fn printCompareSummary(rows: []const CompareRow) void {
    var passed: usize = 0;
    var failed: usize = 0;
    for (rows) |row| {
        if (row.overall_pass) passed += 1 else failed += 1;
    }
    std.debug.print("compare-moka cells: {d}, passed: {d}, failed: {d}\n", .{ rows.len, passed, failed });
}
