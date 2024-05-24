package server

import (
	"bytes"
	"fmt"
)

// main function for insert a key
// first call use root node
func treeInsert(tree *BTree, node BNode, key, val []byte) BNode {
	// the result
	new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}

	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(new, node, idx, key, val)
		} else {
			leafInsert(new, node, idx+1, key, val)
		}
	case BNODE_NODE:
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("bad node!")
	}

	return new
}

/*
*

	idx 0   1   2   3
	val 1   3   5   7
	key=4 return = 1
	所以在叶子节点情况下，4存的idx位置是2
	非叶子节点情况下，返回的就是下一层page，目标值要存进这个page或者这个page下面
*/
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)

	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		// 找到第一个不大于key的idx
		if cmp <= 0 {
			found = i
		}

		if cmp >= 0 {
			break
		}
	}
	return found
}

// 新增一个key 需要拷贝一份新的page, new是将要包含新key的page, old是原来的page
// idx是
func leafInsert(new, old BNode, idx uint16, key, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)

	// 最后一个参数是要拷贝几个数据, 若idx=1，则前面有idx=0,1的数据需要拷贝
	nodeAppendRange(new, old, 0, 0, idx+1)
	// idx+1 是目标存入的索引位置
	nodeAppendKV(new, idx+1, 0, key, val)
	nodeAppendRange(new, old, idx+2, idx+1, old.nkeys()-(idx+1))
}

func leafUpdate(new, old BNode, idx uint16, key, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx+1)
	nodeUpdateKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1))
}

// new需要将node的数据拷贝过来
func nodeInsert(tree *BTree, new BNode, node BNode, idx uint16, key, val []byte) {
	// get and deallocate the kid node
	kptr := node.getPtr(idx)
	knode := tree.get(kptr)
	tree.del(kptr)

	// recrusive
	knode = treeInsert(tree, knode, key, val)

	// split the result
	nsplit, splited := nodeSplit3(knode)

	// update the kid links
	nodeReplaceKidN(tree, new, node, idx, splited[:nsplit]...)
}

func nodeSplit3(node BNode) (uint16, [3]BNode) {
	if node.nbytes() <= BTREE_PAGE_SIZE {
		// 之前初始化时，时2*BTREE_PAGE_SIZE
		node.data = node.data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{node}
	}

	left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)}
	right := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(left, right, node)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.data = left.data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}

	// the left node is still too large
	leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(leftleft, middle, left)
	assert(leftleft.nbytes() <= BTREE_PAGE_SIZE, fmt.Sprintf("function:nodeSplit3, page size is exceed max, size: %v", leftleft.nbytes()))
	return 3, [3]BNode{leftleft, middle, right}
}

func nodeSplit2(left, right, old BNode) {
	halfSize := old.nbytes() / 2

	left.setHeader(old.btype(), 1)
	nodeAppendRange(left, old, 0, 0, 1)

	i := uint16(1)
	for {
		// 8 ptr; 2 offset; 4 keylen vallen
		nextleftSize := left.nbytes() + 8 + 2 + 4 + uint16(len(old.getKey(i))+len(old.getVal(i)))

		if len(left.data) == 2*BTREE_PAGE_SIZE {
			if left.nbytes() > halfSize {
				break
			}
		} else {
			if nextleftSize > halfSize {
				break
			}
		}

		nodeAppendRange(left, old, i, i, 1)
		i = i + 1
	}

	nodeAppendRange(right, old, 0, i, old.nkeys()-i)
}

func nodeReplaceKidN(tree *BTree, new, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}
