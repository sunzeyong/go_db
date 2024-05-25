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
	return binary.LittleEndian.Uint16(node.data[2:])
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
	nkeys := node.nkeys()

	assert(1 <= idx && idx <= nkeys, fmt.Sprintf("function:offsetPos, idx out of range [1:n], idx: %v, nkey: %v", idx, node.nkeys()))
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

// for debug
func (node BNode) String() string {
	header := fmt.Sprintf("raw data: %v, \nbtype: %v, nkeys: %v\n", node.data[:node.nbytes()], node.btype(), node.nkeys())

	pointer, offset, kvpari := "", "", ""
	for i := uint16(0); i < node.nkeys(); i++ {
		pointer += fmt.Sprintf("idx: %d, pointer: %v\n", i, node.getPtr(i))
		offset += fmt.Sprintf("idx: %d, offset: %v\n", i, node.getOffset(i+1))
		kvpari += fmt.Sprintf("idx: %d, key: %v, val: %v\n", i, string(node.getKey(i)), string(node.getVal(i)))
	}

	return fmt.Sprintf("%s\n%s\n%s\n%s", header, pointer, offset, kvpari)
}
