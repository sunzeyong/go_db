package server

import (
	"encoding/binary"
	"fmt"
)

const (
	BNODE_NODE = 1 // internal nodes without values
	BNODE_LEAF = 2 // leaf nodes with values

	HEADLEN = 4

	BTREE_PAGE_SIZE    = 4096
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

func init() {
	node1Max := HEADLEN + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	assert(node1Max <= BTREE_PAGE_SIZE, "init check fail, node1Max exceed page max")
}

type BTree struct {
	root uint64 // pointer

	get func(uint64) BNode // dereference a pointer
	new func(BNode) uint64 // allocate a new page
	del func(uint64)       // deallocate a page
}

func (tree *BTree) Delete(key []byte) bool {
	assert(len(key) != 0, fmt.Sprintf("function:Delete, key len is zero"))
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

func (tree *BTree) Insert(key, val []byte) {
	assert(len(key) != 0, fmt.Sprintf("function:Insert, key is empty"))
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

/*
*
node format
pointers 指向下级节点 若叶子结点 则为空 但是有占位空间
offset kvPair[1:n]的偏移量 0就是第一个kv不用存储 注意这里偏移量总数还是nkeys，在修改文件时，idx的kv需要修改idx+1位置的offset
| type | nkeys | pointers   | offsets    | key-values
| 2B   | 2B    | nkeys * 8B | nkeys * 2B | ...

format of the KV pair
| klen | vlen | key | val |
| 2B   | 2B   | ... | ... |
*/
type BNode struct {
	data []byte // dumped to the disk
}

// decoding data

// type and number of keys
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// ptr
func (node BNode) getPtr(idx uint16) uint64 {
	assert(idx < node.nkeys(), fmt.Sprintf("function:getPtr, idx exceed node max key number, idx: %v, total key: %v", idx, node.nkeys()))
	posStart := HEADLEN + 8*idx
	return binary.LittleEndian.Uint64(node.data[posStart:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	assert(idx <= node.nkeys(), fmt.Sprintf("function:getPtr, idx exceed node max key number"))

	posStart := HEADLEN + 8*idx
	binary.LittleEndian.PutUint64(node.data[posStart:], val)
}

// offset
func offsetPos(node BNode, idx uint16) uint16 {
	assert(1 <= idx && idx <= node.nkeys(), fmt.Sprintf("function:offsetPos, idx out of range [1:n], idx: %v, n: %v", idx, node.nkeys()))
	// idx==0不用存储，所以当idx==1时，在offsets中相对偏移是1-1=0
	return HEADLEN + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// kv pairs
func (node BNode) kvPos(idx uint16) uint16 {
	// 允许获取n位置的position 即当前数据占的空间末尾位置
	assert(idx <= node.nkeys(), fmt.Sprintf("function:kvPos, idx out of max number of keys, idx: %v, total: %v", idx, node.nkeys()))
	return HEADLEN + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	assert(idx < node.nkeys(), fmt.Sprintf("function:getKey, idx out of max number of keys, idx: %v, total: %v", idx, node.nkeys()))
	posStart := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[posStart:])
	return node.data[posStart+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	assert(idx < node.nkeys(), fmt.Sprintf("function:getVal, idx out of max number of keys, idx: %v, total: %v", idx, node.nkeys()))
	posStart := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[posStart:])
	vlen := binary.LittleEndian.Uint16(node.data[posStart+2:])
	return node.data[posStart+4+klen:][:vlen]
}

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}
