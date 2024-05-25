package server

import (
	"fmt"
	"testing"
	"unsafe"
)

type C struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func (c *C) strings() {
	for addr, page := range c.pages {
		fmt.Printf("addr: %v \n%s", addr, page.String())
	}
}

func newC() *C {
	pages := map[uint64]BNode{}

	return &C{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				assert(ok, fmt.Sprintf("function:get, cant find node"))
				return node
			},
			new: func(node BNode) uint64 {
				assert(node.nbytes() <= BTREE_PAGE_SIZE, fmt.Sprintf("function:new, node bytes exceed max, size: %v", node.nbytes()))

				addr := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				assert(pages[addr].data == nil, fmt.Sprintf("function:new, new page data is not nil, content: %s", string(pages[addr].data)))
				pages[addr] = node
				return addr
			},
			del: func(ptr uint64) {
				_, ok := pages[ptr]
				assert(ok, fmt.Sprintf("function:del, fail to find page"))
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (c *C) del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}

func TestInsert(t *testing.T) {
	client := newC()

	client.add("1", "2")
	client.strings()
	
	client.add("3", "4")
	client.strings()

	client.del("3")
	client.strings()
}
