GOCACHE ?= /tmp/go-build-cache

QB_ROWS ?= 120000
QB_LOOKUPS ?= 40000
QB_SEARCHES ?= 8000
QB_BATCH ?= 2000
QB_SEARCH_LIMIT ?= 8
QB_SYNC_MODE ?= normal
QB_WARM_TIMEOUT ?= 45s
QB_RESULTS_DIR ?= .quickbench/results

PB_ROWS ?= 20000
PB_OPS ?= 40000
PB_WORKERS ?= 4
PB_SYNC_MODE ?= full
PB_MODE ?= all
PB_RESULTS_DIR ?= .pillarbench/results
PB_GATE_ROWS ?= 2000
PB_GATE_OPS ?= 4000
PB_GATE_WORKERS ?= 4
PB_GATE_SYNC_MODE ?= full
PB_GATE_CRASH_RUNS ?= 1
PB_GATE_RESULTS_DIR ?= .pillarbench/results
PB_GATE_MIN_TPS ?= 250
PB_GATE_MAX_INSERT_P99_US ?= 50000
PB_GATE_MAX_UPDATE_P99_US ?= 120000
PB_GATE_MAX_DELETE_P99_US ?= 50000
PB_GATE_MAX_ALLOC_PER_OP ?= 20000
PB_GATE_MAX_RECOVERY_MS ?= 5000

.PHONY: test quickbench quickbench-save quickbench-compare-last quickbench-list pillarbench pillarbench-save pillar-gate

test:
	GOCACHE=$(GOCACHE) go test ./...

quickbench:
	GOCACHE=$(GOCACHE) go run ./cmd/quickbench \
		-rows $(QB_ROWS) \
		-lookups $(QB_LOOKUPS) \
		-searches $(QB_SEARCHES) \
		-batch $(QB_BATCH) \
		-search-limit $(QB_SEARCH_LIMIT) \
		-sync-mode $(QB_SYNC_MODE) \
		-warm-timeout $(QB_WARM_TIMEOUT) \
		-json

quickbench-save:
	@mkdir -p $(QB_RESULTS_DIR)
	@stamp=$$(date -u +%Y%m%dT%H%M%SZ); \
	commit=$$(git rev-parse --short HEAD 2>/dev/null || echo unknown); \
	out="$(QB_RESULTS_DIR)/$${stamp}-$${commit}-$(QB_SYNC_MODE)-r$(QB_ROWS).json"; \
	echo "Saving quickbench to $$out"; \
	GOCACHE=$(GOCACHE) go run ./cmd/quickbench \
		-rows $(QB_ROWS) \
		-lookups $(QB_LOOKUPS) \
		-searches $(QB_SEARCHES) \
		-batch $(QB_BATCH) \
		-search-limit $(QB_SEARCH_LIMIT) \
		-sync-mode $(QB_SYNC_MODE) \
		-warm-timeout $(QB_WARM_TIMEOUT) \
		-json \
		-out "$$out"

quickbench-compare-last:
	@mkdir -p $(QB_RESULTS_DIR)
	@old=$$(ls -1t $(QB_RESULTS_DIR)/*.json 2>/dev/null | head -n 1); \
	if [ -z "$$old" ]; then \
		echo "No baseline found in $(QB_RESULTS_DIR). Run 'make quickbench-save' first."; \
		exit 1; \
	fi; \
	stamp=$$(date -u +%Y%m%dT%H%M%SZ); \
	commit=$$(git rev-parse --short HEAD 2>/dev/null || echo unknown); \
	new="$(QB_RESULTS_DIR)/$${stamp}-$${commit}-$(QB_SYNC_MODE)-r$(QB_ROWS).json"; \
	echo "Old baseline: $$old"; \
	echo "New run: $$new"; \
	GOCACHE=$(GOCACHE) go run ./cmd/quickbench \
		-rows $(QB_ROWS) \
		-lookups $(QB_LOOKUPS) \
		-searches $(QB_SEARCHES) \
		-batch $(QB_BATCH) \
		-search-limit $(QB_SEARCH_LIMIT) \
		-sync-mode $(QB_SYNC_MODE) \
		-warm-timeout $(QB_WARM_TIMEOUT) \
		-json \
		-out "$$new"; \
	GOCACHE=$(GOCACHE) go run ./cmd/quickbenchcmp -old "$$old" -new "$$new"

quickbench-list:
	@mkdir -p $(QB_RESULTS_DIR)
	@ls -1t $(QB_RESULTS_DIR)/*.json 2>/dev/null || true

pillarbench:
	GOCACHE=$(GOCACHE) go run ./cmd/pillarbench \
		-mode $(PB_MODE) \
		-sync-mode $(PB_SYNC_MODE) \
		-rows $(PB_ROWS) \
		-ops $(PB_OPS) \
		-workers $(PB_WORKERS) \
		-json

pillarbench-save:
	@mkdir -p $(PB_RESULTS_DIR)
	@stamp=$$(date -u +%Y%m%dT%H%M%SZ); \
	commit=$$(git rev-parse --short HEAD 2>/dev/null || echo unknown); \
	out="$(PB_RESULTS_DIR)/$${stamp}-$${commit}-$(PB_MODE)-$(PB_SYNC_MODE)-r$(PB_ROWS)-o$(PB_OPS).json"; \
	echo "Saving pillarbench to $$out"; \
	GOCACHE=$(GOCACHE) go run ./cmd/pillarbench \
		-mode $(PB_MODE) \
		-sync-mode $(PB_SYNC_MODE) \
		-rows $(PB_ROWS) \
		-ops $(PB_OPS) \
		-workers $(PB_WORKERS) \
		-json \
		-out "$$out"

pillar-gate:
	@mkdir -p $(PB_GATE_RESULTS_DIR)
	@stamp=$$(date -u +%Y%m%dT%H%M%SZ); \
	out="$(PB_GATE_RESULTS_DIR)/gate-$${stamp}-$(PB_GATE_SYNC_MODE)-r$(PB_GATE_ROWS)-o$(PB_GATE_OPS).json"; \
	echo "Running pillarbench gate report -> $$out"; \
	GOCACHE=$(GOCACHE) go run ./cmd/pillarbench \
		-mode all \
		-sync-mode $(PB_GATE_SYNC_MODE) \
		-rows $(PB_GATE_ROWS) \
		-ops $(PB_GATE_OPS) \
		-workers $(PB_GATE_WORKERS) \
		-crash-runs $(PB_GATE_CRASH_RUNS) \
		-json=false \
		-out "$$out"; \
	echo "Validating gate thresholds"; \
	GOCACHE=$(GOCACHE) go run ./cmd/pillargate \
		-report "$$out" \
		-min-workload-tps $(PB_GATE_MIN_TPS) \
		-max-insert-p99-us $(PB_GATE_MAX_INSERT_P99_US) \
		-max-update-p99-us $(PB_GATE_MAX_UPDATE_P99_US) \
		-max-delete-p99-us $(PB_GATE_MAX_DELETE_P99_US) \
		-max-alloc-per-op $(PB_GATE_MAX_ALLOC_PER_OP) \
		-max-recovery-ms $(PB_GATE_MAX_RECOVERY_MS)
