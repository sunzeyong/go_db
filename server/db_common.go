package server

import "encoding/binary"

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
