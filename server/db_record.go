package server

const (
	TYPE_ERROR = iota
	TYPE_BYTES
	TYPE_INT64
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
