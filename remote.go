package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const LOCKFILE = "index.l"

func readIndex(uri *URI) map[string]int {
	var ret []byte
	var err error
	if uri != nil {
		ret, err = ssh(uri, "cat index", nil)
	} else {
		ret, err = ioutil.ReadFile("index")
	}
	if err != nil {
		return nil
	}

	index := make(map[string]int)
	for _, txt := range strings.Split(string(ret), "\n") {
		n := strings.Index(txt, " ")
		if n < 0 {
			continue
		}
		sz, err := strconv.Atoi(txt[n+1:])
		if err != nil {
			continue
		}
		index[txt[:n]] = sz
	}
	return index
}

func writeIndex(index map[string]int) bool {
	f, err := os.Create("index")
	if err != nil {
		return false
	}
	defer f.Close()
	for k, v := range index {
		fmt.Fprintf(f, "%s %d\n", k, v)
	}
	return true
}

func lockIndex() bool {
	for i := 0; i < 300; i++ {
		_, err := os.Stat(LOCKFILE)
		if !os.IsExist(err) {
			f, err := os.Create(LOCKFILE)
			if err != nil {
				f.Close()
			}
			break
		}
		if i == 299 {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
	return true
}

func showFile(index map[string]int, file string, mtime string) {
	if strings.Index(file, "index.") != 0 {
		return
	}
	dat, err := ioutil.ReadFile(file)
	if err != nil || len(dat) == 0 {
		return
	}

	size := 0
	for _, hash := range strings.Split(string(dat), "\n") {
		sz, ok := index[hash]
		if ok {
			size += sz
		}
	}
	file = file[6:]
}

type fileInfo struct {
	name   string
	mtime  time.Time
	size   int
	blocks []string
}

func (fi *fileInfo) read(index map[string]int, indexFile string) {
	dat, err := ioutil.ReadFile(indexFile)
	if err != nil || len(dat) == 0 {
		return
	}

	fi.blocks = strings.Split(string(dat), "\n")

	size := 0
	for _, hash := range fi.blocks {
		sz, ok := index[hash]
		if ok {
			size += sz
		}
	}
	fi.size = size
}

func listFiles(filter []string) []*fileInfo {
	index := readIndex(nil)
	files, err := ioutil.ReadDir(".")
	if index == nil || err != nil {
		return nil
	}

	var regs []*regexp.Regexp
	for _, f := range filter {
		if f == "" {
			continue
		}

		if f[0] != '/' {
			f = strings.ReplaceAll(f, "$", "\\$")
			f = strings.ReplaceAll(f, "^", "\\^")
			f = strings.ReplaceAll(f, ".", "\\.")
			f = strings.ReplaceAll(f, "*", ".*")
			f = strings.ReplaceAll(f, "?", ".")
			f = "^" + f + "$"
		} else {
			f = f[1 : len(f)-1]
		}

		r, err := regexp.Compile(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "f=%s e=%s\n", f, err.Error())
			continue
		}
		regs = append(regs, r)
	}

	var result []*fileInfo
	for _, file := range files {
		name := file.Name()
		if strings.Index(name, "index.") != 0 {
			continue
		}
		name = name[6:]
		//fmt.Fprintf(os.Stderr, "n=%s\n", name)
		if len(regs) > 0 {
			b := false
			for _, r := range regs {
				if r.Match([]byte(name)) {
					b = true
					break
				}
			}
			if !b {
				continue
			}
		}
		fi := &fileInfo{
			name:  name,
			mtime: file.ModTime(),
		}
		fi.read(index, file.Name())
		if fi.size > 0 {
			result = append(result, fi)
		}
	}
	return result
}

func remoteLs(filter []string) int {
	for _, file := range listFiles(filter) {
		fmt.Printf("%-20s %-12d %s\n", strings.ReplaceAll(file.mtime.Format(time.RFC3339)[:19], "T", " "), file.size, file.name)
	}
	return 0
}

func remoteGet(filter []string) int {
	for _, file := range listFiles(filter) {
		fmt.Printf("%s %d %d\n", file.name, file.size, file.mtime.Unix())
		for _, block := range file.blocks {
			fmt.Printf("%s\n", block)
		}
	}
	return 0
}

func remoteRm(filter []string) int {
	for _, file := range listFiles(filter) {
		os.Remove("index." + file.name)
		fmt.Printf("rm %s ...\n", file.name)
	}
	return 0
}

func remoteFile(lines []string) int {
	if len(lines) < 3 {
		fmt.Fprintln(os.Stderr, "E: not enough input lines")
		return 1
	}

	// read index
	index := readIndex(nil)
	if index == nil {
		fmt.Fprintln(os.Stderr, "E: no index")
		return 2
	}

	// cal size
	size := 0
	for _, hash := range lines[2:] {
		bsz, ok := index[hash]
		if !ok {
			fmt.Fprintln(os.Stderr, "E: unknown block")
			return 3
		}
		size += bsz
	}

	// verify size
	osize, _ := strconv.Atoi(lines[1])
	if size != osize {
		fmt.Fprintln(os.Stderr, "E: file has wrong size")
		return 5
	}

	ioutil.WriteFile("index."+lines[0], []byte(strings.Join(lines[2:], "\n")), 0644)
	return 0
}

func remoteBlock(hash string) int {
	bs, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintln(os.Stderr, "E: invalid input")
		return 1
	}
	rhash := sha256.Sum256(bs)
	if hash != hex.EncodeToString(rhash[:]) {
		fmt.Fprintln(os.Stderr, "E: invalid hash")
		return 2
	}
	os.MkdirAll(hash[:2]+"/"+hash[2:4], 0755)
	err = ioutil.WriteFile(hash[:2]+"/"+hash[2:4]+"/"+hash[4:], bs, 0644)
	if err != nil {
		fmt.Fprintln(os.Stderr, "E: can not write "+err.Error())
		return 3
	}
	if !lockIndex() {
		fmt.Fprintln(os.Stderr, "E: can not lock")
		return 4
	}
	defer os.Remove(LOCKFILE)
	index := readIndex(nil)
	if index == nil {
		fmt.Fprintln(os.Stderr, "E: no index")
		return 5
	}
	index[hash] = len(bs)
	if !writeIndex(index) {
		fmt.Fprintln(os.Stderr, "E: write index")
		return 6
	}
	return 0
}

func remote(cmd string) int {
	if len(cmd) == 64 {
		return remoteBlock(cmd)
	}

	lines := []string{}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	// SSH_CLIENT

	switch cmd {
	case "ls":
		return remoteLs(lines)

	case "file":
		return remoteFile(lines)

	case "get":
		return remoteGet(lines)

	case "rm":
		return remoteRm(lines)

	default:
		{
			fmt.Fprintln(os.Stderr, "E: invalid cmd "+cmd)
			return 1
		}
	}
}
