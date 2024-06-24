package server

import "testing"

func TestSetHeader(t *testing.T) {
	node := BNode{data: make([]byte, 1024)}

	node.setHeader(2, 400)
	t.Logf("node: %v", node.data)
}
