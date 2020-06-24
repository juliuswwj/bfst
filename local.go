package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const LOCKFILE = "index.l"

type fileInfo struct {
	name   string
	mtime  time.Time
	size   int64
	blocks []string
}

func (fi *fileInfo) read(index map[string]int, indexFile string) {
	dat, err := ioutil.ReadFile(indexFile)
	if err != nil || len(dat) == 0 {
		return
	}

	fi.blocks = strings.Split(string(dat), "\n")

	var size int64
	for _, hash := range fi.blocks {
		sz, ok := index[hash]
		if ok {
			size += int64(sz)
		}
	}
	fi.size = size
}

func (fi *fileInfo) index() string {
	ret := fmt.Sprintf("%s %d %d\n", fi.name, fi.size, fi.mtime.Unix())
	for _, block := range fi.blocks {
		ret += fmt.Sprintf("%s\n", block)
	}
	return ret
}

func (fi *fileInfo) download(uri *URI) error {
	cachedir := os.Getenv("CACHEDIR")
	if cachedir == "" {
		cachedir = os.Getenv("HOME") + "/.bfst_cache"
	}
	if uri.proto == "file" {
		cachedir = uri.path
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
		bs, err = uri.getBlock(hash)
		if err != nil {
			println("E: download block", hash, err.Error())
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
		return errors.New("create " + fi.name)
	}
	defer f.Close()

	for i, hash := range fi.blocks {
		fmt.Printf("\r%s %d/%d  ", fi.name, i+1, len(fi.blocks))
		data := readBlock(hash)
		if data == nil {
			return errors.New("read block " + hash)
		}
		f.Write(data)
	}
	println("")

	off, _ := f.Seek(0, os.SEEK_CUR)
	if off != fi.size {
		return errors.New("size not equal")
	}
	return nil
}

func (uri *URI) localListFiles(filter []string) []*fileInfo {
	index := uri.allIndex()
	files, err := ioutil.ReadDir(uri.path)
	//println("readdir", len(index), len(files), err)
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
		if strings.LastIndex(name, ".idx") != len(name)-4 {
			continue
		}
		name = name[:len(name)-4]
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
		fi.read(index, uri.path+"/"+file.Name())
		if fi.size > 0 {
			result = append(result, fi)
		}
	}
	return result
}

func (uri *URI) localWriteIndex(index map[string]int) error {
	f, err := os.Create(uri.path + "/index")
	if err != nil {
		return err
	}
	defer f.Close()
	for k, v := range index {
		fmt.Fprintf(f, "%s %d\n", k, v)
	}
	return nil
}

func (uri *URI) localLockIndex() error {
	for i := 0; i < 300; i++ {
		_, err := os.Stat(uri.path + "/" + LOCKFILE)
		if !os.IsExist(err) {
			f, err := os.Create(uri.path + "/" + LOCKFILE)
			if err != nil {
				f.Close()
			}
			break
		}
		if i == 299 {
			return errors.New("lock timed out")
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func (uri *URI) localInit(hdr string, index map[string]int) {
	basepath := uri.path + "/" + hdr
	dirs, err := ioutil.ReadDir(basepath)
	if err != nil {
		// no dir
		return
	}
	for _, dir := range dirs {
		dn := dir.Name()
		if len(dn) != 2 || dn[0] == '.' {
			continue
		}
		path := basepath + "/" + dn
		files, err := ioutil.ReadDir(path)
		if err != nil {
			continue
		}
		for _, file := range files {
			fn := file.Name()
			fpath := path + "/" + fn
			if len(fn) != 60 {
				os.Remove(fpath)
				continue
			}
			bs, err := ioutil.ReadFile(fpath)
			if err != nil {
				os.Remove(fpath)
				continue
			}
			rhash := sha256.Sum256(bs)
			hash := hex.EncodeToString(rhash[:])
			if hash[:2] != hdr || hash[2:4] != dn || hash[4:] != fn {
				os.Remove(fpath)
				continue
			}
			index[hash] = len(bs)
		}
	}
}

func (uri *URI) localPutIndex(lines []string) error {
	if len(lines) < 2 {
		return errors.New("not enough input lines")
	}

	// read index
	index := uri.allIndex()
	if index == nil {
		return errors.New("no index")
	}

	// cal size
	var size int64
	for _, hash := range lines[1:] {
		bsz, ok := index[hash]
		if !ok {
			return errors.New("unknown block " + hash)
		}
		size += int64(bsz)
	}

	// verify size
	ts := strings.Split(lines[0], " ")
	if len(ts) != 3 {
		return errors.New("file head error")
	}
	osize, _ := strconv.ParseInt(ts[1], 10, 64)
	if size != osize {
		return errors.New("file has wrong size")
	}
	return ioutil.WriteFile(uri.path+"/"+ts[0]+".idx", []byte(strings.Join(lines[1:], "\n")), 0644)
}

func (uri *URI) cmdPut(files []string, saveLocalIndex bool) error {
	index := uri.allIndex()
	if index == nil {
		return errors.New("no index")
	}

	// put files
	buf := make([]byte, 1024*1024)
	for _, fn := range files {
		f, err := os.Open(fn)
		if err != nil {
			return err
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
		st, err := f.Stat()
		result := []string{fmt.Sprintf("%s %d %d", fn, size, st.ModTime().Unix())}

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
				err = uri.putBlock(buf[:bsz])
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
		if saveLocalIndex {
			err = ioutil.WriteFile(fn+".idx", []byte(strings.Join(result, "\n")), 0644)
		} else {
			err = uri.putIndex(result)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func getFiles(lines []string) (files []*fileInfo) {
	var file *fileInfo
	for _, line := range lines {
		ts := strings.Split(line, " ")
		if len(ts) == 3 {
			// name size mtime
			if file != nil {
				files = append(files, file)
			}
			file = &fileInfo{name: ts[0]}
			file.size, _ = strconv.ParseInt(ts[1], 10, 64)
			tm, _ := strconv.ParseInt(ts[2], 10, 64)
			file.mtime = time.Unix(tm, 0)
		}
		if len(ts) == 1 && len(ts[0]) == 64 && file != nil {
			// blockhash
			file.blocks = append(file.blocks, ts[0])
		}
	}
	if file != nil {
		files = append(files, file)
	}
	return
}

func (uri *URI) cmdGet(filter []string) error {
	ret, err := uri.getIndex(filter)
	if err != nil {
		return err
	}

	for _, file := range getFiles(strings.Split(string(ret), "\n")) {
		file.download(uri)
	}
	return nil
}

func (uri *URI) cmdUpdate(idxfile string) error {
	datafile := idxfile[:len(idxfile)-4]
	bs, err := ioutil.ReadFile(idxfile)
	if err != nil {
		return uri.cmdPut([]string{datafile}, true)
	}
	files := getFiles(strings.Split(string(bs), "\n"))
	if len(files) != 1 {
		return errors.New("invalid idx file")
	}
	st, err := os.Stat(datafile)
	if err != nil || files[0].mtime.Unix() > st.ModTime().Unix()+1 {
		return files[0].download(uri)
	}
	if files[0].mtime.Unix() < st.ModTime().Unix()-1 {
		return uri.cmdPut([]string{datafile}, true)
	}
	return nil
}
