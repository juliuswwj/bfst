package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

func cmdInit(uri *URI) int {
	ret, err := uri.ssh("pwd", nil)
	if err != nil {
		println("E: ssh failed")
		return 4
	}
	str := strings.Trim(string(ret), " \t\r\n")
	subdir := uri.user
	if len(uri.path) > 0 {
		subdir += "/" + uri.path
	}
	if !strings.HasSuffix(str, subdir) {
		// run mkdir in home directory
		uri.ssh("mkdir -p "+uri.path, nil)
	}

	// touch files in target directory
	uri.ssh("echo >index; touch "+LOCKFILE, nil)

	// index files are created?
	ret, err = uri.ssh("ls", nil)
	if err != nil || strings.Index(string(ret), LOCKFILE) < 0 {
		println("E: init failed")
		return 6
	}

	print("push bfst ...")
	// push bfst to directory
	elf, err := os.Open("bfst")
	defer elf.Close()
	if err != nil {
		println("E: no bfst")
		return 7
	}
	ret, err = uri.ssh("cat >bfst; chmod 755 bfst; sha256sum bfst", elf)
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
	}
	println("")

	// call bfst build
	cnt := 0
	for i := 0; i < 256; i++ {
		fmt.Printf("\rinit=%d count=%d  ", i, cnt)
		ret, err := uri.ssh("./bfst .init", strings.NewReader(fmt.Sprintf("%02x", i)))
		if err != nil {
			println("E: .init error " + err.Error())
			return 1
		}
		//println(string(ret))
		cnt += strings.Count(string(ret), " ok")
	}
	println("")

	// remove lock
	uri.ssh("rm "+LOCKFILE, nil)
	return 0
}

func cmdLs(uri *URI, flags []string) int {
	ret, err := uri.dir(flags)
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
		mcnt := int((size + int64(len(buf)-1)) / int64(len(buf)))
		cnt1 := 0
		cnt2 := 0
		var bsz int
		for (cnt1 + cnt2) <= mcnt {
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
			if has {
				cnt1++
			} else {
				cnt2++
			}
			fmt.Printf("\r%s %d+%d/%d  ", fn, cnt1, cnt2, mcnt)

			if !has {
				_, err = uri.ssh("./bfst ."+hash, bytes.NewBuffer(buf[:bsz]))
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
		_, err = uri.ssh("./bfst .file", strings.NewReader(strings.Join(result, "\n")))
		if err != nil {
			println("E: .file command " + err.Error())
		}
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
		bs, err = uri.read(fpath)
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
	if off != fi.size {
		println("E: size not equal")
		return
	}
}

func cmdGet(uri *URI, filter []string) int {
	ret, err := uri.get(filter)
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
			file.size, _ = strconv.ParseInt(ts[1], 10, 64)
			tm, _ := strconv.ParseInt(ts[2], 10, 64)
			file.mtime = time.Unix(tm, 0)
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
	ret, err := uri.ssh("./bfst .rm", strings.NewReader(strings.Join(files, "\n")))
	if err != nil {
		println(string(ret))
		return 1
	}
	return 0
}
