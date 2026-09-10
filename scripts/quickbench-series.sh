#!/bin/sh
# Called by make quickbench-series; all arguments after the directory/count go
# to quickbench. Build once, then measure fresh databases in separate processes.
set -eu
out=$1
runs=$2
shift 2
case "$runs" in ''|*[!0-9]*) echo 'QB_RUNS must be an integer >= 5' >&2; exit 2;; esac
if [ "$runs" -lt 5 ]; then echo 'QB_RUNS must be >= 5' >&2; exit 2; fi
if [ -z "$out" ]; then echo 'Set QB_SERIES_DIR to a new baseline or candidate directory' >&2; exit 2; fi
mkdir -p "$(dirname "$out")"
# Refuse to overwrite or mix a previous series, including an incomplete run.
mkdir "$out"
bin="$out/quickbench"
trap 'rm -f "$bin"' EXIT HUP INT TERM
go build -o "$bin" ./cmd/quickbench
run=1
while [ "$run" -le "$runs" ]; do
    echo "Quickbench sample $run/$runs -> $out/run-$run.json"
    "$bin" "$@" -out "$out/run-$run.json" > "$out/run-$run.log" 2>&1 || {
        cat "$out/run-$run.log" >&2
        exit 1
    }
    run=$((run + 1))
done
echo "Saved $runs samples in $out"
