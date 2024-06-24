package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func checkRecord(tdef *TableDef, rec Record) ([]Value, error) {
	if len(rec.Cols) != tdef.PKeys {
		return nil, fmt.Errorf("checkRecord fail, tdef.PKeys: %v, len(record.Val): %v", tdef.PKeys, len(rec.Cols))
	}

	if tdef.PKeys == len(tdef.Cols) {
		return nil, fmt.Errorf("checkRecord fail, record has all columns, tdef.PKeys: %v", tdef.PKeys)
	}

	output := make([]Value, len(tdef.Cols))
	copy(output, rec.Vals)

	return output, nil
}

func encodeValues(out []byte, vals []Value) []byte {
	for _, v := range vals {
		switch v.Type {
		case TYPE_INT64:
			var buf [8]byte
			u := uint64(v.I64) + (1 << 63)
			binary.BigEndian.PutUint64(buf[:], u)

		case TYPE_BYTES:
			out = append(out, escapeString(v.Str)...)
			out = append(out, 0)
		default:
			panic("wrong type")
		}
	}

	return out
}

// encode过程使用\x00作为不同字符串的分界标志，所以字符串中的\x00需要转译成\x01；\x01需要转译成\x01\x02。可以保证顺序
func escapeString(in []byte) []byte {
	zeros := bytes.Count(in, []byte{0})
	ones := bytes.Count(in, []byte{1})
	if zeros+ones == 0 {
		return in
	}
	out := make([]byte, len(in)+zeros+ones)
	pos := 0
	for _, ch := range in {
		if ch <= 1 {
			out[pos+0] = 0x01
			out[pos+1] = ch + 1
			pos += 2
		} else {
			out[pos] = ch
			pos += 1
		}
	}
	return out
}

func decodeValues(in []byte, out []Value) {
	offset := 0
	for i, v := range out {
		switch v.Type {
		case TYPE_INT64:
			uint64Val := binary.BigEndian.Uint64(in[offset:])
			int64Val := int64(uint64Val - (1 << 63))

			out[i].I64 = int64Val
			offset += 8

		case TYPE_BYTES:
			zeroIdx := bytes.IndexByte(in[offset:], 0)
			if zeroIdx == -1 {
				assert(zeroIdx != -1, "decodeValues fail, cannot find zero")
			}

			str := in[offset:zeroIdx]
			escapeVal := make([]byte, 0)
			pos := 0
			for idx, ch := range str {
				if ch == 0 {
					break
				}
				if pos != idx {
					continue
				}

				if ch == 1 {
					if idx+1 < len(str) && str[idx+1] == 2 {
						escapeVal = append(escapeVal, 1)
						pos += 2
					} else {
						escapeVal = append(escapeVal, 0)
						pos++
					}

				} else {
					escapeVal = append(escapeVal, ch)
					pos++
				}
			}

			out[i].Str = escapeVal
			offset += (zeroIdx + 1)

		default:
			panic("wrong type")
		}
	}
}

func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	out = encodeValues(out, vals)
	return out
}
