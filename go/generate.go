// Sync shared assets (admin HTML, JS shim) into the Go source tree
// so that go:embed can find them. Source of truth is ../shared/.
//
//go:generate sh sync_shared.sh

package flop
