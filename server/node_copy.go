package server

import (
	"encoding/binary"
	"fmt"
)

// src从srcStartIdx开始拷贝 到 dst的dstStartIdx位置，包含srcStartIdx这个位置
func nodeAppendRange(dst, src BNode, dstStartIdx, srcStartIdx uint16, n uint16) {
	if n == 0 {
		return
	}
	assert(srcStartIdx+n <= src.nkeys(), fmt.Sprintf("function:nodeAppendRange, n exceed max key number, n: %v, srcStartIdx: %v, src.nkey: %v", n, srcStartIdx, src.nkeys()))
	assert(dstStartIdx+n <= dst.nkeys(), fmt.Sprintf("function:nodeAppendRange, n exceed max key number, n: %v, dstStartIdx: %v, dst.nkey: %v", n, dstStartIdx, dst.nkeys()))

	// copy pointer
	for i := uint16(0); i < n; i++ {
		dst.setPtr(dstStartIdx+i, src.getPtr(srcStartIdx+i))
	}

	// copy offsets
	/**
	将src的偏移转成dst的偏移，先计算出src的相对偏移量，再加上dst已有的偏移

	src |----srcLen----[ 1 ][  2  ]
	dst |---dstLen---[ 1 ][  2  ]
	*/
	srcLen := src.getOffset(srcStartIdx)
	dstLen := dst.getOffset(dstStartIdx)
	for i := uint16(1); i <= n; i++ {
		offset := (src.getOffset(srcStartIdx+i) - srcLen) + dstLen
		dst.setOffset(dstStartIdx+i, offset)
	}

	// copy kvs
	// end计算的入参是加了n 是为了将所有数据包含，而不是取最后一个数据的开始pos
	begin := src.kvPos(srcStartIdx)
	end := src.kvPos(srcStartIdx + n)
	copy(dst.data[dst.kvPos(dstStartIdx):], src.data[begin:end])
}

func nodeAppendKV(dst BNode, idx uint16, ptr uint64, key, val []byte) {
	// ptrs
	dst.setPtr(idx, ptr)

	// kvs
	// 虽然idx位置的数据还没有，但是可以通过kvPos查询到，因为是插入idx-1时候维护的
	pos := dst.kvPos(idx)
	binary.LittleEndian.PutUint16(dst.data[pos:], uint16(len(key)))
	binary.LittleEndian.PutUint16(dst.data[pos+2:], uint16(len(val)))
	copy(dst.data[pos+4:], key)
	copy(dst.data[pos+4+uint16(len(key)):], val)

	// the offset of the next key
	// 这里的idx的offset其实是idx-1插入时维护的
	lastOffset := dst.getOffset(idx)
	offset := lastOffset + 4 + uint16(len(key)+len(val))
	dst.setOffset(idx+1, offset)
}

func nodeUpdateKV(new BNode, idx uint16, ptr uint64, key, val []byte) {
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))
	copy(new.data[pos+4+uint16(len(key)):], val)

	// the offset of the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16(len(key)+len(val)))
}
