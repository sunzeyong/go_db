package server

import (
	"bytes"
	"fmt"
)

func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{}
		}

		new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		leafDelete(new, node, idx)
		return new

	case BNODE_NODE:
		return nodeDelete(tree, node, idx, key)

	default:
		panic("bad node!")
	}
}

func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-(idx+1))
}

func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated.data) == 0 {
		return BNode{}
	}
	tree.del(kptr)

	new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)

	switch {
	case mergeDir < 0:
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))

	case mergeDir > 0:
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))

	case mergeDir == 0:
		assert(updated.nkeys() > 0, fmt.Sprintf("function:nodeDelete, update.nkeys not bigger than 0, nkeys: %v", updated.nkeys()))
		nodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}

func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}

func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	if idx > 0 {
		sibling := tree.get(node.getPtr(idx - 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADLEN
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}

	if idx+1 < node.nkeys() {
		sibling := tree.get(node.getPtr(idx + 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADLEN
		if merged <= BTREE_PAGE_SIZE {
			return 1, sibling
		}
	}
	return 0, BNode{}
}

func nodeReplace2Kid(new, node BNode, idx uint16, merged uint64, key []byte) {
	new.setHeader(BNODE_NODE, node.nkeys())
	nodeAppendRange(new, node, 0, 0, idx-1)
	nodeAppendKV(new, idx, merged, key, nil)
	nodeAppendRange(new, node, idx+1, idx+1, node.nkeys()-(idx+1))
}
