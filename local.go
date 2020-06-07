package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

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
	cmds := []string{"-T", "-C"}
	if uri.port != "" {
		cmds = append(cmds, "-p", uri.port)
	}
	if uri.path != "" {
		cmd = "cd " + uri.path + "; " + cmd
	}
	cmds = append(cmds, uri.user+"@"+uri.host, cmd)
	p := exec.Command("ssh", cmds...)
	p.Stdin = stdin
	stdout := &bytes.Buffer{}
	p.Stdout = stdout
	stderr := &bytes.Buffer{}
	p.Stderr = stderr
	err := p.Run()
	if err != nil {
		return nil, errors.New(string(stderr.Bytes()))
	}
	return stdout.Bytes(), err
}

func cmdInit(uri *URI) int {
	ret, err := ssh(uri, "pwd", nil)
	if err != nil {
		println("E: ssh failed")
		return 4
	}
	str := strings.Trim(string(ret), " \t\r\n")
	subdir := uri.user
	if len(uri.path) > 0 {
		subdir += "/" + uri.path
	}
	if strings.HasSuffix(str, subdir) {
		println("E: path exists")
		return 5
	}
	// run mkdir in home directory
	ssh(uri, "mkdir -p "+uri.path, nil)

	// touch files in target directory
	ssh(uri, "touch index index.o index.l", nil)

	// index files are created?
	ret, err = ssh(uri, "ls", nil)
	if err != nil || strings.Index(string(ret), "index.l") < 0 {
		println("E: init failed")
		return 6
	}

	// push bfst to directory
	elf, err := os.Open("bfst")
	defer elf.Close()
	if err != nil {
		println("E: no bfst")
		return 7
	}
	ret, err = ssh(uri, "cat >bfst; chmod 755 bfst; sha256sum bfst", elf)
	if err != nil {
		println("E: push bfst")
		return 7
	}

	// check sha256
	sha := sha256.New()
	elf.Seek(0, os.SEEK_SET)
	buf := make([]byte, 65536)
	for {
		sz, err := elf.Read(buf)
		if err != nil || sz <= 0 {
			break
		}
		sha.Write(buf[:sz])
	}
	hash := hex.EncodeToString(sha.Sum(nil))
	if strings.Index(string(ret), hash) < 0 {
		println("E: bfst data error")
		return 8
	} else {
		// remove lock
		ssh(uri, "rm index.l", nil)
	}
	return 0
}

func cmdLs(uri *URI, flags []string) int {
	// call remoteLs
	ret, err := ssh(uri, "./bfst .ls ", strings.NewReader(strings.Join(flags, "\n")))
	if err != nil {
		println("E: ssh failed or not init")
		return 4
	}
	print(string(ret))
	return 0
}

func cmdPut(uri *URI, files []string) int {
	index := readIndex(uri)
	if index == nil {
		fmt.Fprintln(os.Stderr, "E: no index")
		return 4
	}

	// put files
	buf := make([]byte, 1024*1024)
	for _, fn := range files {
		f, err := os.Open(fn)
		if err != nil {
			println("E: failed to open " + fn)
			continue
		}
		size, err := f.Seek(0, os.SEEK_END)
		f.Seek(0, os.SEEK_SET)

		// remove path part from filename
		i := strings.LastIndex(fn, "/")
		if i >= 0 {
			fn = fn[i+1:]
		}
		i = strings.LastIndex(fn, "\\")
		if i >= 0 {
			fn = fn[i+1:]
		}
		result := []string{fn, fmt.Sprint(size)}

		// put file blocks
		mcnt := (int(size) + len(buf) - 1) / len(buf)
		cnt := 1
		bsz := int(size)
		for cnt <= mcnt {
			fmt.Printf("\r%s %d/%d    ", fn, cnt, mcnt)
			cnt++

			bsz, err = f.Read(buf)
			if err != nil || bsz <= 0 {
				if err == io.EOF {
					err = nil
				}
				break
			}
			rhash := sha256.Sum256(buf[:bsz])
			hash := hex.EncodeToString(rhash[:])

			osz, has := index[hash]
			if !has {
				_, err = ssh(uri, "./bfst ."+hash, bytes.NewBuffer(buf[:bsz]))
				if err != nil {
					break
				}
				index[hash] = bsz
			} else if osz != bsz {
				panic(fmt.Sprintf("block[%s] size %d!=%d", hash, bsz, osz))
			}
			result = append(result, hash)
		}
		f.Close()
		println("")
		if err != nil {
			println(err.Error())
			continue
		}
		ssh(uri, "./bfst .file", strings.NewReader(strings.Join(result, "\n")))
	}
	return 0
}

func (fi *fileInfo) download(uri *URI) {
	cachedir := os.Getenv("CACHEDIR")
	if cachedir == "" {
		cachedir = os.Getenv("HOME") + "/.bfst_cache"
	}
	readBlock := func(hash string) []byte {
		path := hash[:2] + "/" + hash[2:4]
		fpath := path + "/" + hash[4:]
		bs, err := ioutil.ReadFile(cachedir + "/" + fpath)
		if err == nil {
			return bs
		}
		err = os.MkdirAll(cachedir+"/"+path, 0755)
		if err != nil {
			println("E: create cachedir " + path)
			return nil
		}
		bs, err = ssh(uri, "cat "+fpath, nil)
		if err != nil {
			println("E: download block " + hash)
			return nil
		}

		rhash := sha256.Sum256(bs)
		if hash != hex.EncodeToString(rhash[:]) {
			println("E: checksum block " + hash)
			return nil
		}
		ioutil.WriteFile(cachedir+"/"+fpath, bs, 0644)
		return bs
	}

	f, err := os.Create(fi.name)
	if err != nil {
		println("E: create " + fi.name)
		return
	}
	defer f.Close()

	for i, hash := range fi.blocks {
		fmt.Printf("\r%s %d/%d  ", fi.name, i+1, len(fi.blocks))
		data := readBlock(hash)
		if data == nil {
			println("E: read block " + hash)
			return
		}
		f.Write(data)
	}
	println("")

	off, _ := f.Seek(0, os.SEEK_CUR)
	if int(off) != fi.size {
		println("E: size not equal")
		return
	}
}

func cmdGet(uri *URI, filter []string) int {
	ret, err := ssh(uri, "./bfst .get", strings.NewReader(strings.Join(filter, "\n")))
	if err != nil {
		println(string(ret))
		return 1
	}

	var file *fileInfo
	for _, line := range strings.Split(string(ret), "\n") {
		ts := strings.Split(line, " ")
		if len(ts) == 3 {
			// name size mtime
			if file != nil {
				file.download(uri)
			}
			file = &fileInfo{name: ts[0]}
			file.size, _ = strconv.Atoi(ts[1])
			tm, _ := strconv.Atoi(ts[2])
			file.mtime = time.Unix(int64(tm), 0)
		}
		if len(ts) == 1 && len(ts[0]) == 64 {
			// blockhash
			file.blocks = append(file.blocks, ts[0])
		}
	}
	if file != nil {
		file.download(uri)
	}
	return 0
}

func cmdRm(uri *URI, files []string) int {
	ret, err := ssh(uri, "./bfst .rm", strings.NewReader(strings.Join(files, "\n")))
	if err != nil {
		println(string(ret))
		return 1
	}
	return 0
}
