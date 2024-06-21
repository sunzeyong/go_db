package server

import "testing"

func TestKv(t *testing.T) {
	kv := InitKV("./kv_file.hex")
	defer kv.Close()

	if err := kv.Open(); err != nil {
		t.Fatalf("fail to open kv, err: %s", err)
	}

	if err := kv.Set([]byte("b_key"), []byte("b_value")); err != nil {
		t.Fatalf("fail to set key, err: %s", err)
	}
	if err := kv.Set([]byte("c_key"), []byte("c_value")); err != nil {
		t.Fatalf("fail to set key, err: %s", err)
	}
	if err := kv.Set([]byte("g_key"), []byte("g_value")); err != nil {
		t.Fatalf("fail to set key, err: %s", err)
	}
	if err := kv.Set([]byte("a_key"), []byte("a_value")); err != nil {
		t.Fatalf("fail to set key, err: %s", err)
	}

	if v, ok := kv.Get([]byte("a_key")); !ok {
		t.Fatalf("fail to get key")
	} else {
		if string(v) != "a_value" {
			t.Fatalf("wrong value, got: %s, expected: %s", v, "a_value")
		}
	}
}
