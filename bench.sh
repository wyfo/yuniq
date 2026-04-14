#!/usr/bin/env bash
# Benchmark script for yuniq vs dedup / count-sort contenders.
# Usage: ./bench.sh [file ...]
#   Files default to all bench_data/*.txt
#   Results are written as markdown to bench_results/
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
YUNIQ="$SCRIPT_DIR/target/release/yuniq"
BENCH_DATA="$SCRIPT_DIR/bench_data"
BENCH_RESULTS="$SCRIPT_DIR/bench_results"

# ── build ────────────────────────────────────────────────────────────────────
echo "Building yuniq (release)..."
cargo build --release --manifest-path "$SCRIPT_DIR/Cargo.toml" 2>&1
echo ""

mkdir -p "$BENCH_RESULTS"

# ── mode / file list ─────────────────────────────────────────────────────────
RUN_DEDUP=1
RUN_COUNT=1

if [ $# -gt 0 ] && { [ "$1" = "dedup" ] || [ "$1" = "count" ]; }; then
    [ "$1" = "dedup" ] && RUN_COUNT=0
    [ "$1" = "count" ] && RUN_DEDUP=0
    shift
fi

# ── generate missing data files ───────────────────────────────────────────────
if [ -z "$(ls -A "$BENCH_DATA" 2>/dev/null)" ]; then
    echo "Benchmark data missing — generating..."
    python3 "$SCRIPT_DIR/bench_data.py"
    echo ""
fi

# ── file list ─────────────────────────────────────────────────────────────────
if [ $# -gt 0 ]; then
    FILES=("$@")
else
    FILES=("$BENCH_DATA"/*.txt)
fi

# ── benchmarks ───────────────────────────────────────────────────────────────
for file in "${FILES[@]}"; do
    name="$(basename "$file" .txt)"
    echo "━━━ $name ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # ── dedup ────────────────────────────────────────────────────────────────
    echo "$YUNIQ                < $file > /dev/null"
    if [ "$RUN_DEDUP" = "1" ]; then
    echo "[dedup] $name"
    hyperfine \
        --warmup 3 \
        --shell=bash \
        --export-markdown "$BENCH_RESULTS/${name}_dedup.md" \
        -n "yuniq"               "$YUNIQ                < $file > /dev/null" \
        -n "yuniq -U"            "$YUNIQ -U              < $file > /dev/null" \
        -n "yuniq --fast"        "$YUNIQ --fast          < $file > /dev/null" \
        -n "yuniq --lean"        "$YUNIQ --lean          < $file > /dev/null" \
        -n "yuniq (pipe)"        "cat $file | $YUNIQ              > /dev/null" \
        -n "yuniq --fast (pipe)" "cat $file | $YUNIQ --fast       > /dev/null" \
        -n "xuniq"               "xuniq                  < $file > /dev/null" \
        -n "xuniq --safe"        "xuniq --safe            < $file > /dev/null" \
        -n "hist -u"             "hist -u                < $file > /dev/null" \
        -n "huniq"               "huniq                  < $file > /dev/null" \
        -n "runiq"               "runiq -fquick          < $file > /dev/null" \
        -n "ripuniq"             "ripuniq                < $file > /dev/null"
    fi # RUN_DEDUP

    # ── count + sort ─────────────────────────────────────────────────────────
    if [ "$RUN_COUNT" = "1" ]; then
    echo "[count+sort] $name"
    hyperfine \
        --warmup 3 \
        --shell=bash \
        --export-markdown "$BENCH_RESULTS/${name}_count.md" \
        -n "yuniq -c"               "$YUNIQ -c               < $file > /dev/null" \
        -n "yuniq -cU"              "$YUNIQ -cU              < $file > /dev/null" \
        -n "yuniq -c --lean"        "$YUNIQ -c --lean         < $file > /dev/null" \
        -n "yuniq -c (pipe)"        "cat $file | $YUNIQ -c              > /dev/null" \
        -n "hist"                   "hist                    < $file > /dev/null" \
        -n "huniq -cs"              "huniq -cs               < $file > /dev/null" \
        -n "cuniq -cs"              "cuniq -cs               < $file > /dev/null"
    fi # RUN_COUNT

    echo ""
done

echo "Results written to $BENCH_RESULTS/"
