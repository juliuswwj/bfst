package main

import (
	"os"
	"strings"
)

const usage = `Usage:
bfst indexfile
bfst user@host[:port][/path] [subcommands]
subcommands = 
  init
  ls [filter1 filter2 ...]
  rm file1 [file2 ...]
  get file1 [file2 ...]
  put file1 [file2 ...]
  index file1 [file2 ...]
`

func main() {
	if len(os.Args) == 2 && os.Args[1][0] == '.' {
		uri := parseURI("file:.")
		if os.Args[1] == ".init" {
			uri.init()
		} else {
			uri.remote()
		}
		os.Exit(0)
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

	var err error
	switch os.Args[2] {
	case "init":
		err = uri.init()
	case "ls", "dir":
		{
			var bs []byte
			bs, err = uri.ls(os.Args[3:])
			if len(bs) > 0 {
				print(string(bs))
			}
		}
	case "put":
		err = uri.cmdPut(os.Args[3:], false)
	case "get":
		err = uri.cmdGet(os.Args[3:])
	case "rm":
		{
			var bs []byte
			bs, err = uri.rm(os.Args[3:])
			if len(bs) > 0 {
				println(string(bs))
			}
		}
	default:
		if strings.LastIndex(os.Args[2], ".idx") == len(os.Args[2])-4 {
			err = uri.cmdUpdate(os.Args[2])
		} else {
			print(usage)
			os.Exit(1)
		}
	}
	if err != nil {
		println("E:", err.Error())
		os.Exit(3)
	}
}
