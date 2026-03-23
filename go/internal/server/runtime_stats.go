package server

import "runtime"

// RuntimeStatsSnapshot returns a point-in-time view of Go runtime memory stats.
// Note: these are runtime allocator stats, not exact OS RSS.
func RuntimeStatsSnapshot() map[string]interface{} {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	lastPauseNs := uint64(0)
	if ms.NumGC > 0 {
		lastPauseNs = ms.PauseNs[(ms.NumGC-1)%uint32(len(ms.PauseNs))]
	}

	return map[string]interface{}{
		"heapAlloc":          ms.HeapAlloc,
		"heapInuse":          ms.HeapInuse,
		"heapIdle":           ms.HeapIdle,
		"heapReleased":       ms.HeapReleased,
		"heapSys":            ms.HeapSys,
		"stackInuse":         ms.StackInuse,
		"stackSys":           ms.StackSys,
		"mspanInuse":         ms.MSpanInuse,
		"mcacheInuse":        ms.MCacheInuse,
		"sys":                ms.Sys,
		"nextGC":             ms.NextGC,
		"numGC":              ms.NumGC,
		"gcPauseTotalNs":     ms.PauseTotalNs,
		"lastGCPauseNs":      lastPauseNs,
		"goroutines":         runtime.NumGoroutine(),
		"gcCPUFraction":      ms.GCCPUFraction,
		"timestampUnixMilli": ms.LastGC / 1e6,
	}
}
