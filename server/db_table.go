package server

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

type TableDef struct {
	Name   string
	Types  []uint32 // col types
	Cols   []string // col names
	PKeys  int
	Prefix uint32
}

func (db *DB) TableNew(tdef *TableDef) error {
	if err := tableDefCheck(tdef); err != nil {
		return err
	}

	table := (&Record{}).AddStr("name", []byte(tdef.Name))
	ok, err := dbGet(db, TDEF_TABLE, table)
	if err != nil {
		return err
	}
	if ok {
		return fmt.Errorf("table exists: %s", tdef.Name)
	}

	// allocate a new prefix
	assert(tdef.Prefix == 0, fmt.Sprintf("tableNew, tdef.prefix is not zero, prefix:%v", tdef.Prefix))
	tdef.Prefix = TABLE_PREFIX_MIN
	meta := (&Record{}).AddStr("key", []byte("next_prefix"))
	ok, err = dbGet(db, TDEF_META, meta)
	if err != nil {
		return err
	}
	if ok {
		tdef.Prefix = binary.LittleEndian.Uint32(meta.Get("val").Str)
		assert(tdef.Prefix > TABLE_PREFIX_MIN, fmt.Sprintf("tdef.prefix: %d is over table_prefix_min: %d", tdef.Prefix, TABLE_PREFIX_MIN))
	} else {
		meta.AddStr("val", make([]byte, 4))
	}

	// update the next prefix
	binary.LittleEndian.PutUint32(meta.Get("val").Str, tdef.Prefix+1)
	_, err = dbUpdate(db, TDEF_META, *meta, MODE_UPSERT)
	if err != nil {
		return err
	}

	// store the definition
	val, err := json.Marshal(tdef)
	if err != nil {
		return err
	}
	table.AddStr("def", val)
	_, err = dbUpdate(db, TDEF_TABLE, *table, MODE_UPSERT)
	return err
}

func tableDefCheck(tdef *TableDef) error {
	return nil
}
