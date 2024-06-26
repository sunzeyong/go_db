package server

import (
	"fmt"
	"os"
	"syscall"
)

//  持久化和空闲页管理
type KV struct {
	Path string
	fp   *os.File

	tree BTree
	free FreeList

	mmap struct {
		file   int      // file size, can be larger than the database size
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // multiple mmaps, can be non-continuous
	}

	page struct {
		flushed uint64 // database size in number of pages, 已经分配了mmap对应位置
		nfree   int    // number of pages taken from the free list
		nappend int    // number of pages to be appended
		// newly allocated or deallocated pages keyed by the pointer
		// nil value denotes a deallocated page
		updates map[uint64][]byte
	}
}

func InitKV(path string) *KV {
	return &KV{
		Path: path,

		mmap: struct {
			file   int
			total  int
			chunks [][]byte
		}{
			chunks: make([][]byte, 0),
		},

		page: struct {
			flushed uint64
			nfree   int
			nappend int
			updates map[uint64][]byte
		}{
			updates: make(map[uint64][]byte),
		},
	}
}

// 操作
func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

func (db *KV) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return flushPages(db)
}

func (db *KV) Update(key []byte, val []byte, mode int) (bool, error) {

	


	return false, nil
}

func (db *KV) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, flushPages(db)
}

func (db *KV) Open() error {
	fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("OpenFile: %w", err)
	}
	db.fp = fp

	// mmap映射 初始化mmap
	sz, chunk, err := mmapInit(db.fp)
	if err != nil {
		defer db.fp.Close()
		return err
	}

	db.mmap.file = sz
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}

	// tree如何操作page
	db.tree.get = db.pageGet
	db.tree.new = db.pageNew
	db.tree.del = db.pageDel

	db.free.get = db.pageGet
	db.free.new = db.pageAppend
	db.free.use = db.pageUse

	// 初始化 tree 和 flush
	err = masterLoad(db)
	if err != nil {
		defer db.fp.Close()
		return err
	}

	return nil
}

func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		assert(err == nil, "kv close err")
	}
	db.fp.Close()
}

// callback for free and tree
func (db *KV) pageGet(ptr uint64) BNode {

	if page, ok := db.page.updates[ptr]; ok {
		assert(page != nil, "pageGet, page is nil")
		return BNode{page}
	}

	return pageGetMapped(db, ptr)
}

func pageGetMapped(db *KV, ptr uint64) BNode {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return BNode{chunk[offset : offset+BTREE_PAGE_SIZE]}
		}
		start = end
	}
	panic("bad ptr")
}

// callback for tree

// 若有空闲 则先分配空闲页 如果没有 则append个新页
func (db *KV) pageNew(node BNode) uint64 {
	assert(len(node.data) <= BTREE_PAGE_SIZE, "function:pageNew, node data size exceed PAGE_SIZE")

	ptr := uint64(0)
	if db.page.nfree < db.free.Total() {
		ptr = db.free.Get(db.page.nfree)
		db.page.nfree++
	} else {
		ptr = db.page.flushed + uint64(db.page.nappend)
		db.page.nappend++
	}
	db.page.updates[ptr] = node.data
	return ptr
}

func (db *KV) pageDel(ptr uint64) {
	db.page.updates[ptr] = nil
}

// callback for freelist
func (db *KV) pageAppend(node BNode) uint64 {
	assert(len(node.data) <= BTREE_PAGE_SIZE, "")
	ptr := db.page.flushed + uint64(db.page.nappend)
	db.page.nappend++
	db.page.updates[ptr] = node.data
	return ptr
}

func (db *KV) pageUse(ptr uint64, node BNode) {
	db.page.updates[ptr] = node.data
}
