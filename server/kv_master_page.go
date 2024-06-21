package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const DB_SIG = "BuildYourOwnDB05"

// the master page format.
// it contains the pointer to the root and other important bits.
// | sig | btree_root | page_used | free_list |
// | 16B | 8B         | 8B        | 8B        |
func masterLoad(db *KV) error {
	if db.mmap.file == 0 {
		db.page.flushed = 1
		return nil
	}

	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[16:])
	used := binary.LittleEndian.Uint64(data[24:])
	free := binary.LittleEndian.Uint64(data[32:])

	if !bytes.Equal([]byte(DB_SIG), data[:16]) {
		return errors.New("bad singature")
	}

	bad := !(1 <= used && used <= uint64(db.mmap.file/BTREE_PAGE_SIZE))
	bad = bad || !(root < used)
	if bad {
		return errors.New("bad master page")
	}

	db.tree.root = root
	db.page.flushed = used
	db.free.head = free
	return nil
}

func masterStore(db *KV) error {
	var data [40]byte

	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	binary.LittleEndian.PutUint64(data[32:], db.free.head)

	_, err := db.fp.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("write master page: %w", err)
	}
	return nil
}
