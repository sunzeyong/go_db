package server

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const (
	TYPE_ERROR = iota
	TYPE_BYTES
	TYPE_INT64

	TABLE_PREFIX_MIN uint32 = 3
)

type Value struct {
	Type uint32
	I64  int64
	Str  []byte
}

type Record struct {
	Cols []string
	Vals []Value
}

func (r *Record) AddStr(key string, val []byte) *Record {
	if r.Cols == nil {
		r.Cols = make([]string, 0)
	}
	r.Cols = append(r.Cols, key)

	v := Value{
		Type: TYPE_BYTES,
		Str:  val,
	}
	if r.Vals == nil {
		r.Vals = make([]Value, 0)
	}
	r.Vals = append(r.Vals, v)

	return r
}

func (r *Record) AddInt64(key string, val int64) *Record {
	if r.Cols == nil {
		r.Cols = make([]string, 0)
	}
	r.Cols = append(r.Cols, key)

	v := Value{
		Type: TYPE_BYTES,
		I64:  val,
	}
	if r.Vals == nil {
		r.Vals = make([]Value, 0)
	}
	r.Vals = append(r.Vals, v)

	return r
}

func (r *Record) Get(key string) *Value {
	for idx, item := range r.Cols {
		if item == key {
			return &r.Vals[idx]
		}
	}
	return nil
}

type DB struct {
	Path string

	kv     KV
	tables map[string]*TableDef
}

type TableDef struct {
	Name   string
	Types  []uint32 // col types
	Cols   []string // col names
	Pkeys  int
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

var TDEF_META = &TableDef{
	Prefix: 1,
	Name:   "@meta",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"key", "val"},
	Pkeys:  1,
}

var TDEF_TABLE = &TableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"name", "def"},
	Pkeys:  1,
}

func checkRecord(tdef *TableDef, rec Record, n int) ([]Value, error) {
	return nil, nil
}

func encodeValues(out []byte, vals []Value) []byte {
	return nil
}

func decodeValues(in []byte, out []Value) {

}

func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	out = encodeValues(out, vals)
	return out
}

func getTableDef(db *DB, name string) *TableDef {
	tdef, ok := db.tables[name]
	if !ok {
		if db.tables == nil {
			db.tables = map[string]*TableDef{}
		}

		tdef = getTableDefDB(db, name)
		if tdef != nil {
			db.tables[name] = tdef
		}
	}
	return tdef
}

func getTableDefDB(db *DB, name string) *TableDef {
	rec := (&Record{}).AddStr("name", []byte(name))
	ok, err := dbGet(db, TDEF_TABLE, rec)
	assert(err == nil, fmt.Sprintf("getTableDefDB, fail to get db def, err: %s", err))
	if !ok {
		return nil
	}

	tdef := &TableDef{}
	err = json.Unmarshal(rec.Get("def").Str, tdef)
	assert(err == nil, fmt.Sprintf("getTableDefDB, fail to unmarshal db def, tdef:%v", rec.Get("def")))
	return tdef
}
