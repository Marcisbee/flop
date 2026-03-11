package flop

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

const defaultPageCacheCap = 10000 // ~40MB at 4KB pages

// pageCache is a bounded LRU cache for pages with dirty tracking.
type pageCache struct {
	mu    sync.Mutex
	cap   int
	items map[uint64]*cacheNode
	head  *cacheNode // most recently used
	tail  *cacheNode // least recently used
}

type cacheNode struct {
	id    uint64
	page  *Page
	dirty bool
	prev  *cacheNode
	next  *cacheNode
}

func newPageCache(cap int) *pageCache {
	return &pageCache{
		cap:   cap,
		items: make(map[uint64]*cacheNode),
	}
}

func (c *pageCache) get(id uint64) *Page {
	c.mu.Lock()
	node, ok := c.items[id]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	c.moveToFront(node)
	c.mu.Unlock()
	return node.page
}

func (c *pageCache) put(id uint64, pg *Page, dirty bool) {
	c.mu.Lock()
	if node, ok := c.items[id]; ok {
		node.page = pg
		if dirty {
			node.dirty = true
		}
		c.moveToFront(node)
		c.mu.Unlock()
		return
	}
	node := &cacheNode{id: id, page: pg, dirty: dirty}
	c.items[id] = node
	c.pushFront(node)
	c.evict()
	c.mu.Unlock()
}

func (c *pageCache) remove(id uint64) {
	c.mu.Lock()
	node, ok := c.items[id]
	if ok {
		c.unlink(node)
		delete(c.items, id)
	}
	c.mu.Unlock()
}

// collectDirty returns all dirty pages and marks them clean.
func (c *pageCache) collectDirty() []*Page {
	c.mu.Lock()
	defer c.mu.Unlock()
	var pages []*Page
	for _, node := range c.items {
		if node.dirty {
			pages = append(pages, node.page)
			node.dirty = false
		}
	}
	return pages
}

func (c *pageCache) moveToFront(node *cacheNode) {
	if c.head == node {
		return
	}
	c.unlink(node)
	c.pushFront(node)
}

func (c *pageCache) pushFront(node *cacheNode) {
	node.prev = nil
	node.next = c.head
	if c.head != nil {
		c.head.prev = node
	}
	c.head = node
	if c.tail == nil {
		c.tail = node
	}
}

func (c *pageCache) unlink(node *cacheNode) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		c.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		c.tail = node.prev
	}
	node.prev = nil
	node.next = nil
}

func (c *pageCache) evict() {
	for len(c.items) > c.cap {
		if c.tail == nil || c.tail.dirty {
			break // don't evict dirty pages
		}
		victim := c.tail
		c.unlink(victim)
		delete(c.items, victim.id)
	}
}

// Pager manages page-level I/O backed by a file.
// It uses read/write directly (no mmap for portability), with a bounded LRU page cache.
type Pager struct {
	file      *os.File
	mu        sync.RWMutex
	pageCount atomic.Uint64
	freeList  []uint64     // recycled page IDs
	cache     *pageCache
}

// MetaPage layout (page 0):
//
//	[24:32]  root page ID
//	[32:40]  page count
//	[40:48]  next row ID counter
//	[48:56]  free list head page ID
const metaRootOffset = pageHeaderSize
const metaPageCountOffset = pageHeaderSize + 8
const metaNextIDOffset = pageHeaderSize + 16
const metaFreeHeadOffset = pageHeaderSize + 24

func OpenPager(path string) (*Pager, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("open pager: %w", err)
	}

	p := &Pager{
		file:  f,
		cache: newPageCache(defaultPageCacheCap),
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	if info.Size() == 0 {
		// Initialize with meta page
		var meta Page
		meta.SetType(PageMeta)
		meta.SetPageID(0)
		binary.BigEndian.PutUint64(meta[metaRootOffset:], 0)     // no root yet
		binary.BigEndian.PutUint64(meta[metaPageCountOffset:], 1) // just meta page
		binary.BigEndian.PutUint64(meta[metaNextIDOffset:], 1)   // next row ID
		meta.UpdateChecksum()
		if _, err := f.WriteAt(meta[:], 0); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
		p.pageCount.Store(1)
	} else {
		pc := uint64(info.Size()) / PageSize
		p.pageCount.Store(pc)
	}

	return p, nil
}

func (p *Pager) Close() error {
	return p.file.Close()
}

// ReadPage reads a page from disk (or cache).
func (p *Pager) ReadPage(id uint64) (*Page, error) {
	if pg := p.cache.get(id); pg != nil {
		return pg, nil
	}

	p.mu.RLock()
	pg := &Page{}
	_, err := p.file.ReadAt(pg[:], int64(id)*PageSize)
	p.mu.RUnlock()
	if err != nil {
		return nil, fmt.Errorf("read page %d: %w", id, err)
	}

	if pg.Type() != PageFree && !pg.ValidateChecksum() {
		return nil, fmt.Errorf("page %d: checksum mismatch (corrupted)", id)
	}

	p.cache.put(id, pg, false)
	return pg, nil
}

// WritePage writes a page to the cache as dirty (not yet flushed).
func (p *Pager) WritePage(pg *Page) {
	pg.UpdateChecksum()
	p.cache.put(pg.PageID(), pg, true)
}

// CachePage stores a page in the cache as dirty (for copy-on-write).
func (p *Pager) CachePage(pg *Page) {
	p.cache.put(pg.PageID(), pg, true)
}

// AllocPage returns a new page with a fresh ID.
func (p *Pager) AllocPage(pageType uint8) *Page {
	var id uint64
	if len(p.freeList) > 0 {
		id = p.freeList[len(p.freeList)-1]
		p.freeList = p.freeList[:len(p.freeList)-1]
	} else {
		id = p.pageCount.Add(1) - 1
	}
	pg := &Page{}
	pg.SetType(pageType)
	pg.SetPageID(id)
	p.cache.put(id, pg, true)
	return pg
}

// FreePage marks a page as free for recycling.
func (p *Pager) FreePage(id uint64) {
	p.freeList = append(p.freeList, id)
	p.cache.remove(id)
}

// FlushAll writes all dirty cached pages to disk and syncs.
func (p *Pager) FlushAll() error {
	dirtyPages := p.cache.collectDirty()
	if len(dirtyPages) == 0 {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Ensure file is large enough
	needed := int64(p.pageCount.Load()) * PageSize
	info, err := p.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < needed {
		if err := p.file.Truncate(needed); err != nil {
			return err
		}
	}

	for _, pg := range dirtyPages {
		if _, err := p.file.WriteAt(pg[:], int64(pg.PageID())*PageSize); err != nil {
			return err
		}
	}

	return p.file.Sync()
}

// ReadMeta reads the meta page.
func (p *Pager) ReadMeta() (rootPageID uint64, pageCount uint64, nextID uint64, err error) {
	pg, err := p.ReadPage(0)
	if err != nil {
		return 0, 0, 0, err
	}
	if pg.Type() != PageMeta {
		return 0, 0, 0, errors.New("page 0 is not meta page")
	}
	rootPageID = binary.BigEndian.Uint64(pg[metaRootOffset:])
	pageCount = binary.BigEndian.Uint64(pg[metaPageCountOffset:])
	nextID = binary.BigEndian.Uint64(pg[metaNextIDOffset:])
	return
}

// WriteMeta updates the meta page.
func (p *Pager) WriteMeta(rootPageID, pageCount, nextID uint64) {
	pg := &Page{}
	pg.SetType(PageMeta)
	pg.SetPageID(0)
	binary.BigEndian.PutUint64(pg[metaRootOffset:], rootPageID)
	binary.BigEndian.PutUint64(pg[metaPageCountOffset:], pageCount)
	binary.BigEndian.PutUint64(pg[metaNextIDOffset:], nextID)
	pg.UpdateChecksum()
	p.cache.put(uint64(0), pg, true)
}
