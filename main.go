package main

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"strings"
)

const usage = `Usage:
    bfst user@host[:port][/path] [subcommands]
subcommands = 
    init
    ls [filter1 filter2 ...]
    get file1 [file2 ...]
    put file1 [file2 ...]
`

type URI struct {
	user, host, port, path string
}

func parseURI(str string) *URI {
	uri := new(URI)
	n := strings.Index(str, "/")
	if n > 0 {
		uri.path = str[n+1:]
		str = str[:n]
	}
	n = strings.LastIndex(str, ":")
	if n > 0 {
		uri.port = str[n+1:]
		str = str[:n]
	}
	n = strings.Index(str, "@")
	if n < 0 {
		return nil
	}
	uri.user = str[:n]
	uri.host = str[n+1:]
	return uri
}

func ssh(uri *URI, cmd string, stdin io.Reader) ([]byte, error) {
	cmds := []string{"-T"}
	if uri.port != "" {
		cmds = append(cmds, "-p", uri.port)
	}
	if uri.path != "" {
		cmd = "cd " + uri.path + "; " + cmd
	}
	cmds = append(cmds, cmd)
	p := exec.Command("ssh", cmds...)
	p.Stdin = stdin
	stdout := &bytes.Buffer{}
	p.Stdout = stdout
	err := p.Run()
	return stdout.Bytes(), err
}

func verify(uri *URI) bool {
	ret, err := ssh(uri, "ls", nil)
	if err != nil {
		return false
	}
	print(string(ret))
	return false
}

func cmdInit(uri *URI) int {
	if verify(uri) {
		println("E: already init")
		return 4
	}

	return 0
}

func cmdLs(uri *URI, flags []string) int {
	if !verify(uri) {
		println("E: not init")
		return 4
	}

	return 0
}

func cmdPut(uri *URI, files []string) int {
	if !verify(uri) {
		println("E: not init")
		return 4
	}

	return 0
}

func cmdGet(uri *URI, files []string) int {
	if !verify(uri) {
		println("E: not init")
		return 4
	}

	return 0
}

func main() {
	if len(os.Args) < 3 {
		print(usage)
		os.Exit(1)
	}

	uri := parseURI(os.Args[1])
	if uri == nil {
		println("E: invalid URI")
		os.Exit(2)
	}

	ret := 0
	switch os.Args[2] {
	case "init":
		ret = cmdInit(uri)
	case "ls":
		ret = cmdLs(uri, os.Args[3:])
	case "put":
		ret = cmdPut(uri, os.Args[3:])
	case "get":
		ret = cmdGet(uri, os.Args[3:])
	default:
		ret = 3
	}
	os.Exit(ret)
}
