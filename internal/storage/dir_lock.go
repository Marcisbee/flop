package storage

// DirLock is an exclusive process-level lock for a database data directory.
type DirLock interface {
	Close() error
}
