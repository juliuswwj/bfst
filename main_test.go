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

	uri = parseURI(os.Getenv("USER") + "@localhost/bfst_tmp")
	assert(t, uri != nil && uri.path == "bfst_tmp", "parseURI !nil")

	path := os.Getenv("HOME") + "/" + uri.path
	os.RemoveAll(path)
	os.MkdirAll(path, 0755)

	assert(t, cmdInit(uri) == 5, "init path exists")
	os.RemoveAll(path)

	assert(t, cmdInit(uri) == 0, "init path Ok")

}
