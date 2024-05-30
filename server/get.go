package server

import "bytes"

func treeGet(tree *BTree, node BNode, key []byte) BNode {
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{}
		}
		return node

	case BNODE_NODE:
		childPtr := node.getPtr(idx)
		childPage := tree.get(childPtr)
		return treeGet(tree, childPage, key)

	default:
		panic("bad node!")
	}
}
