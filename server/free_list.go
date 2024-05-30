package server

import (
	"encoding/binary"
	"fmt"
)

/**

|   node1   |     |   node2   |     |   node3   |
+-----------+     +-----------+     +-----------+
| total=xxx |     |           |     |           |
| next=yyy  | ==> | next=qqq  | ==> | next=eee  | ==> ...
| size=zzz  |     | size=ppp  |     | size=rrr  |
| pointers  |     | pointers  |     | pointers  |

The node format:
| type | size | total | next | pointers  |
|  2B  |  2B  |  8B   |  8B  | size * 8B |

*/

const (
	BNODE_FREE_LIST  = 3
	FREE_LIST_HEADER = 4 + 8 + 8
	FREE_LIST_CAP    = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8
)

// 内存结构中的数据链表，具体的page信息需要到通过get获取到
type FreeList struct {
	head uint64

	get func(uint64) BNode
	new func(BNode) uint64
	use func(uint64, BNode)
}

func (fl *FreeList) Total() int {
	if fl.head == 0 {
		return 0
	}
	headPage := fl.get(fl.head)
	return int(binary.LittleEndian.Uint64(headPage.data[32:]))
}

/*
	topn对应的 pointer位置

| next | ==> | next | ==> | next |
|   2  |     |   5  |     |   8  |
|   1  |     |   4  |     |   7  |
|   0  |     |   3  |     |   6  |
*/
func (fl *FreeList) Get(topn int) uint64 {
	assert(0 <= topn && topn < fl.Total(), fmt.Sprintf("function:freelist.get, topn out of range, topn: %d, total: %d", topn, fl.Total()))

	node := fl.get(fl.head)
	for flnSize(node) <= topn {
		topn -= flnSize(node)
		next := flnNext(node)
		assert(next != 0, "function:freelist.get, next is nil")
		node = fl.get(next)
	}

	return flnPtr(node, flnSize(node)-topn-1)

}

func (fl *FreeList) Update(popn int, freed []uint64) {
	assert(popn <= fl.Total(), fmt.Sprintf("function:FreeList.update, popn is out of range, popn: %d, total: %d", popn, fl.Total()))
	if popn == 0 && len(freed) == 0 {
		return
	}

	total := fl.Total()
	reuse := []uint64{}
	for fl.head != 0 && len(reuse)*FREE_LIST_CAP < len(freed) {
		node := fl.get(fl.head)
		freed = append(freed, fl.head)
		if popn >= flnSize(node) {
			popn -= flnSize(node)
		} else {
			remain := flnSize(node) - popn
			popn = 0

			for remain > 0 && len(reuse)*FREE_LIST_CAP < len(freed)+remain {
				remain--
				reuse = append(reuse, flnPtr(node, remain))
			}

			for i := 0; i < remain; i++ {
				freed = append(freed, flnPtr(node, i))
			}
		}

		total -= flnSize(node)
		fl.head = flnNext(node)
	}
	assert(len(reuse)*FREE_LIST_CAP >= len(freed) || fl.head == 0, fmt.Sprintf("freelist.update error, len(reuse): %d, fl.head: %s", len(reuse), fl.head))

	flPush(fl, freed, reuse)
	flnSetTotal(fl.get(fl.head), uint64(total+len(freed)))
}

func flPush(fl *FreeList, freed []uint64, reuse []uint64) {
	for len(freed) > 0 {
		new := BNode{make([]byte, BTREE_PAGE_SIZE)}

		size := len(freed)
		if size > FREE_LIST_CAP {
			size = FREE_LIST_CAP
		}

		flnSetHeader(new, uint16(size), fl.head)
		for i, ptr := range freed[:size] {
			flnSetPtr(new, i, ptr)
		}

		if len(reuse) > 0 {
			fl.head, reuse = reuse[0], reuse[1:]
			fl.use(fl.head, new)
		} else {
			fl.head = fl.new(new)
		}

	}
	assert(len(reuse) == 0, fmt.Sprintf("flPush err, len(reuse) is not 0, len(reuse):%d", len(reuse)))
}

/*
*
The node format:
| type | size | total | next | pointers  |
|  2B  |  2B  |  8B   |  8B  | size * 8B |
*/
func flnSize(node BNode) int {
	if node.data == nil {
		return 0
	}
	return int(binary.LittleEndian.Uint16(node.data[16:]))
}

func flnSetSize(node BNode, size uint16) {
	binary.LittleEndian.PutUint16(node.data[16:], size)
}

func flnNext(node BNode) uint64 {
	if node.data == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(node.data[96:])
}

func flnSetNext(node BNode, next uint64) {
	binary.LittleEndian.PutUint64(node.data[96:], next)
}

func flnPtr(node BNode, idx int) uint64 {
	if node.data == nil {
		return 0
	}
	headOffSet := FREE_LIST_HEADER * 8
	ptrOffSet := headOffSet + 8*idx
	return binary.LittleEndian.Uint64(node.data[ptrOffSet:])
}

func flnSetPtr(node BNode, idx int, ptr uint64) {
	headOffSet := FREE_LIST_HEADER * 8
	ptrOffSet := headOffSet + 8*idx

	binary.LittleEndian.PutUint64(node.data[ptrOffSet:], ptr)
}

func flnSetHeader(node BNode, size uint16, next uint64) {
	flnSetSize(node, size)
	flnSetNext(node, next)
}

func flnSetTotal(node BNode, total uint64) {
	binary.LittleEndian.PutUint64(node.data[32:], total)
}
