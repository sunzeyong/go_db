package server

import "fmt"

func (db *DB) Get(table string, rec *Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("tbale not found: %s", table)
	}
	return dbGet(db, tdef, rec)
}

func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
	values, err := checkRecord(tdef, *rec, tdef.Pkeys)
	if err != nil {
		return false, err
	}

	key := encodeKey(nil, tdef.Prefix, values[:tdef.Pkeys])
	val, ok := db.kv.Get(key)
	if !ok {
		return false, nil
	}

	for i := tdef.Pkeys; i < len(tdef.Cols); i++ {
		values[i].Type = tdef.Types[i]
	}
	decodeValues(val, values[tdef.Pkeys:])

	rec.Cols = append(rec.Cols, tdef.Cols[tdef.Pkeys:]...)
	rec.Vals = append(rec.Vals, values[tdef.Pkeys:]...)
	return true, nil
}
