package server

import "bytes"

const (
	CMP_GE = +3 // >=
	CMP_GT = +2 // >
	CMP_LT = -2 // <
	CMP_LE = -3 // <=
)

type BIter struct {
	tree *BTree
	path []BNode
	pos  []uint16
}

// find the closest position to a key with respect to the `cmp` relation
func (tree *BTree) Seek(key []byte, cmp int) *BIter {
	iter := tree.SeekLE(key)
	if cmp != CMP_LE && iter.Valid() {
		cur, _ := iter.Deref()
		if !cmpOK(cur, cmp, key) {
			// off by one
			if cmp > 0 {
				iter.Next()
			} else {
				iter.Prev()
			}
		}
	}
	return iter
}

// get the current KV pair
func (iter *BIter) Deref() ([]byte, []byte) {
	return nil, nil
}

// precondition of the Deref()
func (iter *BIter) Valid() bool {
	return false
}

// moving backward and forward
func (iter *BIter) Prev() {
	iterPrev(iter, len(iter.path)-1)
}
func (iter *BIter) Next() {

}

func iterPrev(iter *BIter, level int) {
	if iter.pos[level] > 0 {
		iter.pos[level]-- // move within this node
	} else if level > 0 {
		iterPrev(iter, level-1) // move to a slibing node
	} else {
		return // dummy key
	}
	if level+1 < len(iter.pos) {
		// update the kid node
		node := iter.path[level]
		kid := iter.tree.get(node.getPtr(iter.pos[level]))
		iter.path[level+1] = kid
		iter.pos[level+1] = kid.nkeys() - 1
	}
}

// find the closest position that is less or equal to the input key
func (tree *BTree) SeekLE(key []byte) *BIter {
	iter := &BIter{tree: tree}
	for ptr := tree.root; ptr != 0; {
		node := tree.get(ptr)
		idx := nodeLookupLE(node, key)
		iter.path = append(iter.path, node)
		iter.pos = append(iter.pos, idx)
		if node.btype() == BNODE_NODE {
			ptr = node.getPtr(idx)
		} else {
			ptr = 0
		}
	}
	return iter
}

// key cmp ref
func cmpOK(key []byte, cmp int, ref []byte) bool {
	r := bytes.Compare(key, ref)
	switch cmp {
	case CMP_GE:
		return r >= 0
	case CMP_GT:
		return r > 0
	case CMP_LT:
		return r < 0
	case CMP_LE:
		return r <= 0
	default:
		panic("what?")
	}
}
