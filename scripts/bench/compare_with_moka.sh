#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/.agent/benchmarks/compare-$(date +%Y%m%d-%H%M%S)}"
MOKABENCH_DIR="${MOKABENCH_DIR:-$ROOT_DIR/tmp/mokabench}"
CACHE_TRACE_ROOT="${CACHE_TRACE_ROOT:-$MOKABENCH_DIR/cache-trace}"
THREADS="${THREADS:-1,4,16}"
TRACES="${TRACES:-loop,s3,ds1,oltp}"
REPETITIONS="${REPETITIONS:-10}"
VARIANCE_THRESHOLD="${VARIANCE_THRESHOLD:-15}"

mkdir -p "$OUT_DIR"
mkdir -p "$ROOT_DIR/tmp"

if [[ ! -d "$MOKABENCH_DIR/.git" ]]; then
  git clone --depth 1 https://github.com/moka-rs/mokabench.git "$MOKABENCH_DIR"
fi

pushd "$MOKABENCH_DIR" >/dev/null
if [[ ! -d "$CACHE_TRACE_ROOT/arc" ]]; then
  git submodule init
  git submodule update --depth 1
fi

cargo build --release

MOKA_CSV="$OUT_DIR/moka_trace.csv"
echo "trace,cache,capacity,clients,reads,hit_ratio_pct,duration_secs" > "$MOKA_CSV"

IFS=',' read -r -a TRACE_ARRAY <<< "$TRACES"
for trace in "${TRACE_ARRAY[@]}"; do
  ./target/release/mokabench --trace-file "$trace" --num-clients "$THREADS" \
    | awk -F', ' -v trace="$trace" '
      /^Moka Sync Cache, / || /^Moka SegmentedCache\(8\), / {
        # Columns from mokabench CSV:
        # 1 cache, 2 capacity, 3 clients, 5 reads, 6 hit ratio, 7 duration
        print trace "," $1 "," $2 "," $3 "," $5 "," $6 "," $7
      }' >> "$MOKA_CSV"
done
popd >/dev/null

pushd "$ROOT_DIR" >/dev/null

zig build -Doptimize=ReleaseFast bench-trace -- \
  --trace-file "$TRACES" \
  --threads "$THREADS" \
  --repetitions "$REPETITIONS" \
  --cache-trace-root "$CACHE_TRACE_ROOT" \
  --output-prefix "$OUT_DIR/cache_zig_trace"

zig build -Doptimize=ReleaseFast bench-compare -- \
  --cache-zig-csv "$OUT_DIR/cache_zig_trace.csv" \
  --moka-csv "$MOKA_CSV" \
  --variance-threshold "$VARIANCE_THRESHOLD" \
  --output-prefix "$OUT_DIR/compare"

echo "comparison complete"
echo "cache-zig trace rows: $OUT_DIR/cache_zig_trace.csv"
echo "moka rows:            $MOKA_CSV"
echo "gate report:          $OUT_DIR/compare.csv"

popd >/dev/null
