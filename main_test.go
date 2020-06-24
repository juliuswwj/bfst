package main

import (
	"os"
	"runtime/debug"
	"testing"
)

func assert(t *testing.T, cond bool, msg ...interface{}) {
	if !cond {
		debug.PrintStack()
		t.Fatal(msg...)
	}
}

func TestMain(t *testing.T) {
	uri := parseURI("test")
	assert(t, uri == nil, "parseURI nil")

	uri = parseURI("file:test")
	assert(t, uri != nil && uri.path == "test", "parseURI file")

	const DIR = "/bfst_tmp"
	path := os.Getenv("HOME") + DIR
	os.RemoveAll(path)

	uri = parseURI("file:" + path)
	assert(t, uri != nil && uri.proto == "file", "parseURI file 2")
	assert(t, uri.init() == nil, "init file Ok")

	err := uri.putBlock(make([]byte, 4096))
	assert(t, err == nil, "putBlock Ok")

	uri = parseURI(os.Getenv("USER") + "@localhost" + DIR)
	assert(t, uri != nil && uri.host == "localhost", "parseURI !nil")

	err = uri.init()
	assert(t, err == nil, "init path:", err)

	index := uri.allIndex()
	assert(t, len(index) == 1, "allIndex == 1")

	size := 65536 * 4
	b := make([]byte, size)
	for i := 0; i < 10; i++ {
		b[0] = byte(i)
		err = uri.putBlock(b)
		assert(t, err == nil, "putBlock 2 Ok")
	}
	index = uri.allIndex()
	assert(t, len(index) == 11, "allIndex == 11")

	for h, sz := range index {
		b, err = uri.getBlock(h)
		assert(t, err == nil && len(b) == sz, "getBlock")
	}

}
