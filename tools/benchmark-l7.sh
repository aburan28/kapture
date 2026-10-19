#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
bench_dir=$(mktemp -d)
trap 'rm -rf "$bench_dir"' EXIT
go build -o "$bench_dir/kapture-bench" ./tools/l7-bench
cargo +1.97.0 build --release --locked --manifest-path tools/flowscope-bench/Cargo.toml --target-dir "$bench_dir/rust"
printf 'implementation,protocol,body_bytes,iterations,exchanges,input_bytes,ns_per_exchange\n'
for trial in 1 2 3 4 5; do
  for size in 128 98304; do
    iterations=20000
    if [ "$size" = 128 ]; then iterations=200000; fi
    # Alternate order to avoid consistently giving one tool a warmer CPU.
    if [ $((trial % 2)) = 1 ]; then
      GOMAXPROCS=1 "$bench_dir/kapture-bench" -body "$size" -iterations "$iterations"
      "$bench_dir/rust/release/kapture-flowscope-bench" "$size" "$iterations"
    else
      "$bench_dir/rust/release/kapture-flowscope-bench" "$size" "$iterations"
      GOMAXPROCS=1 "$bench_dir/kapture-bench" -body "$size" -iterations "$iterations"
    fi
  done
done
