package server

import (
	"fmt"
)

type BTree struct {
	root uint64 // pointer

	get func(uint64) BNode // dereference a pointer
	new func(BNode) uint64 // allocate a new page
	del func(uint64)       // deallocate a page
}

type InsertReq struct {
	tree *BTree

	Added bool
	Key   []byte
	Val   []byte
	Mode  int
}

func (tree *BTree) Get(key []byte) ([]byte, bool) {
	assert(len(key) != 0, "function:Get, key is empty")
	assert(len(key) <= BTREE_MAX_KEY_SIZE, fmt.Sprintf("function:Get, key is exceed size, key: %v", key))

	node := treeGet(tree, tree.get(tree.root), key)
	if node.data == nil {
		return nil, false
	}

	return node.getVal(nodeLookupLE(node, key)), true
}

func (tree *BTree) Insert(key, val []byte) {
	assert(len(key) != 0, "function:Insert, key is empty")
	assert(len(key) <= BTREE_MAX_KEY_SIZE, fmt.Sprintf("function:Insert, key is exceed size, key: %v", key))
	assert(len(val) <= BTREE_MAX_VAL_SIZE, fmt.Sprintf("function:Insert, val is exceed size, val: %v", val))

	if tree.root == 0 {
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_LEAF, 2)

		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}

	node := tree.get(tree.root)
	tree.del(tree.root)

	node = treeInsert(tree, node, key, val)
	nsplit, splitted := nodeSplit3(node)
	if nsplit > 1 {
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_NODE, nsplit)

		for i, knode := range splitted[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(splitted[0])
	}
}

func (tree *BTree) Delete(key []byte) bool {
	assert(len(key) != 0, "function:Delete, key len is zero")
	assert(len(key) <= BTREE_MAX_KEY_SIZE, fmt.Sprintf("function:Delete, key is exceed max key size, key: %v, len: %v", key, len(key)))
	if tree.root == 0 {
		return false
	}

	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated.data) == 0 {
		return false
	}

	tree.del(tree.root)
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}
	return true
}

func (tree *BTree) InsertEx(req *InsertReq) {

}
