package main

import (
	"os"
)

const usage = `Usage:
  bfst user@host[:port][/path] [subcommands]
subcommands = 
  init
  ls [filter1 filter2 ...]
  rm file1 [file2 ...]
  get file1 [file2 ...]
  put file1 [file2 ...]
`

func main() {
	if len(os.Args) == 2 && os.Args[1][0] == '.' {
		os.Exit(remote(os.Args[1][1:]))
	}

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
	case "rm":
		ret = cmdRm(uri, os.Args[3:])
	default:
		{
			print(usage)
			ret = 3
		}
	}
	os.Exit(ret)
}
