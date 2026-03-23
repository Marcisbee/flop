package storage

import (
	"os"
	"sync"

	"github.com/marcisbee/flop/internal/schema"
	"github.com/marcisbee/flop/internal/util"
)

// PageCache is an LRU cache for pages with dirty tracking.
type PageCache struct {
	mu         sync.Mutex
	cache      *util.LRUCache[uint32, *Page]
	dirtyPages map[uint32]bool
	file       *os.File
}

func NewPageCache(file *os.File, maxPages int) *PageCache {
	pc := &PageCache{
		dirtyPages: make(map[uint32]bool),
		file:       file,
	}
	lru := util.NewLRUCache[uint32, *Page](maxPages)
	lru.OnEvict = func(pageNum uint32, page *Page) {
		if pc.dirtyPages[pageNum] {
			pc.flushPageSync(pageNum, page)
		}
	}
	pc.cache = lru
	return pc
}

func (pc *PageCache) GetPage(pageNumber uint32) (*Page, error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if p, ok := pc.cache.Get(pageNumber); ok {
		return p, nil
	}

	p, err := pc.readPageFromDisk(pageNumber)
	if err != nil {
		return nil, err
	}
	pc.cache.Set(pageNumber, p)
	return p, nil
}

func (pc *PageCache) PutPage(pageNumber uint32, page *Page) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.cache.Set(pageNumber, page)
}

func (pc *PageCache) MarkDirty(pageNumber uint32) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.dirtyPages[pageNumber] = true
}

func (pc *PageCache) FlushAll() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	for pageNum := range pc.dirtyPages {
		if p, ok := pc.cache.Get(pageNum); ok {
			if err := pc.writePageToDisk(pageNum, p); err != nil {
				return err
			}
		}
	}
	pc.dirtyPages = make(map[uint32]bool)
	return pc.file.Sync()
}

func (pc *PageCache) readPageFromDisk(pageNumber uint32) (*Page, error) {
	offset := int64(schema.FileHeaderSize) + int64(pageNumber)*int64(schema.PageSize)
	buf := make([]byte, schema.PageSize)
	n, err := pc.file.ReadAt(buf, offset)
	if err != nil && n < schema.PageSize {
		return nil, err
	}
	return NewPage(buf), nil
}

func (pc *PageCache) writePageToDisk(pageNumber uint32, page *Page) error {
	offset := int64(schema.FileHeaderSize) + int64(pageNumber)*int64(schema.PageSize)
	_, err := pc.file.WriteAt(page.Data[:], offset)
	return err
}

func (pc *PageCache) flushPageSync(pageNumber uint32, page *Page) {
	offset := int64(schema.FileHeaderSize) + int64(pageNumber)*int64(schema.PageSize)
	pc.file.WriteAt(page.Data[:], offset)
	delete(pc.dirtyPages, pageNumber)
}

func (pc *PageCache) IsDirty(pageNumber uint32) bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.dirtyPages[pageNumber]
}

func (pc *PageCache) DirtyCount() int {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return len(pc.dirtyPages)
}

func (pc *PageCache) Clear() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.dirtyPages = make(map[uint32]bool)
	pc.cache.Clear()
}
