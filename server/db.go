package server

type DB struct {
	Path string

	kv     KV
	tables map[string]*TableDef
}
