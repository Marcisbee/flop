#!/usr/bin/env bash
set -euo pipefail
# Build the same measurement adapter against any checkout without changing its module.
root=$(cd "$(dirname "$0")/../.." && pwd)
checkout=${1:-$root}
output=${2:-$root/.quickbench/results/engines-build}
mkdir -p "$output"
output=$(cd "$output" && pwd)
cp "$checkout/go.mod" "$output/compare.mod"
cp "$checkout/go.sum" "$output/compare.sum"
adapter="$checkout/zz_engine_compare_test.go"
if [[ -e "$adapter" ]]; then
  echo "Refusing to overwrite $adapter" >&2
  exit 1
fi
cp "$root/benchmarks/engines/adapter/engine_compare_test.go" "$adapter"
trap 'mv "$adapter" "$output/adapter-source.go"' EXIT
cd "$checkout"
go mod edit -modfile="$output/compare.mod" -require=github.com/marcisbee/flop/benchmarks/engines@v0.0.0 -replace="github.com/marcisbee/flop/benchmarks/engines=$root/benchmarks/engines"
go test -mod=mod -modfile="$output/compare.mod" -tags enginecompare -c -o "$output/engine.test" .
git rev-parse HEAD > "$output/revision"
sha256sum "$output/engine.test" > "$output/binary.sha256"
