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

const BFST_HELLO = "BFSTv1.0"
const NOSUPPORT = " is not supported"

//URI struct of file store config
type URI struct {
	proto, user, host, port, path string

	// internal
	ecnt int

	// ssh internal
	echan         chan error
	stdin, stdout chan []byte
}

//parseURI
//support
//  user@domain:port/path
//  ssh://user@domain:port/path
//  http://user@domain:port/path
//  https://user@domain:port/path
//  file:path
func parseURI(str string) *URI {
	uri := new(URI)
	n := strings.Index(str, ":")
	if n < 0 {
		if strings.Index(str, "@") < 0 {
			return nil
		}
		uri.proto = "ssh"
	} else if strings.Index(str[:n], "@") < 0 {
		uri.proto = str[:n]
		str = str[n+1:]
	}
	// remove // in uri
	if len(str) > 2 && str[:2] == "//" {
		str = str[2:]
	}

	if uri.proto == "file" {
		uri.path = str
		return uri
	}

	n = strings.Index(str, "/")
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
		uri.host = str
	} else {
		uri.user = str[:n]
		uri.host = str[n+1:]
	}
	return uri
}

func (uri *URI) str() string {
	user := ""
	if uri.user != "" {
		user = uri.user + "@"
	}
	port := ""
	if uri.port != "" {
		port = ":" + uri.port
	}
	return fmt.Sprintf("%s://%s%s%s/%s", uri.proto, user, uri.host, port, uri.path)
}

type pipeIO struct {
	ch chan []byte
	dt []byte
}

// Read for cmd.stdin
func (p *pipeIO) Read(data []byte) (n int, err error) {
	for {
		n = len(p.dt)
		if n > len(data) {
			n = len(data)
			copy(data, p.dt[:n])
			p.dt = p.dt[n:]
			return
		} else if n > 0 {
			copy(data, p.dt)
			p.dt = nil
			return
		}
		if p.ch == nil {
			err = io.EOF
			return
		}
		p.dt = <-p.ch
		if p.dt == nil {
			p.ch = nil
			err = io.EOF
			return
		}
		if len(data) < 4 {
			err = errors.New("buf header size error")
			return
		}
		n = len(p.dt)
		data[0] = 0x26
		data[1] = byte(n >> 16)
		data[2] = byte(n >> 8)
		data[3] = byte(n >> 0)
		n = 4
		return
	}
}

// Write for cmd.stdout
func (p *pipeIO) Write(data []byte) (n int, err error) {
	//fmt.Fprintf(os.Stderr, "#write %d\n", len(data))

	n = len(data)
	if data == nil {
		p.ch <- data
		return
	}

	p.dt = append(p.dt, data...)

	// search block start, discard unknown data
	for {
		if p.dt[0] == 0x26 {
			break
		}
		p.dt = p.dt[1:]
		if len(p.dt) == 0 {
			return
		}
	}

	// get data
	for len(p.dt) >= 4 {
		sz := int(p.dt[3])
		sz += int(p.dt[2]) << 8
		sz += int(p.dt[1]) << 16
		if sz > len(p.dt)-4 {
			return
		}
		p.ch <- p.dt[4 : 4+sz]
		if sz == len(p.dt)-4 {
			p.dt = nil
		} else {
			p.dt = p.dt[4+sz:]
		}
	}
	return
}

func (uri *URI) cmds(cmd string) []string {
	cmds := []string{"-T", "-C"}
	if uri.port != "" {
		cmds = append(cmds, "-p", uri.port)
	}
	if uri.path != "" {
		cmd = "cd " + uri.path + "; " + cmd
	}
	cmds = append(cmds, uri.user+"@"+uri.host, cmd)
	return cmds
}

func (uri *URI) runSSH(cmd string, stdin []byte) ([]byte, error) {
	cmds := uri.cmds(cmd)
	p := exec.Command("ssh", cmds...)
	stdout := &bytes.Buffer{}
	if stdin != nil {
		p.Stdin = bytes.NewReader(stdin)
	}
	p.Stdout = stdout
	p.Stderr = os.Stderr
	err := p.Run()
	return stdout.Bytes(), err
}

func (uri *URI) runSSH0(cmd string) error {
	cmds := uri.cmds(cmd)
	p := exec.Command("ssh", cmds...)
	p.Stdout = os.Stdout
	p.Stderr = os.Stderr
	return p.Run()
}

func (uri *URI) runRemote(cmd string, stdin []byte) ([]byte, error) {
	if uri.stdin == nil {
		uri.open()
	}
	for uri.ecnt < 10 {
		if uri.stdin != nil {
			b := []byte{byte(len(cmd))}
			b = append(b, []byte(cmd)...)
			b = append(b, stdin...)
			uri.stdin <- b

			//fmt.Fprintf(os.Stderr, "< %d\n", len(b))

			select {
			case b = <-uri.stdout:
				//fmt.Fprintf(os.Stderr, "> %d\n", len(b))
				if b != nil {
					uri.ecnt = 0
					if len(b) > 3 && string(b[:3]) == "E: " {
						return nil, errors.New(string(b[3:]))
					}
					return b, nil
				}
			case <-uri.echan:
			case <-time.After(time.Minute):
			}
		} else {
			select {
			case <-time.After(time.Second * 5):
			}
		}
		uri.ecnt++
		print("W: retry ", uri.ecnt)
		uri.close()
		err := uri.open()
		if err != nil {
			print(err.Error())
		}
		println("")
	}
	return nil, errors.New("too many retries")
}

func (uri *URI) open() (err error) {
	if uri.proto == "ssh" {
		uri.close()
		cmds := uri.cmds("./bfst .")
		uri.stdin = make(chan []byte)
		uri.stdout = make(chan []byte, 10)
		uri.echan = make(chan error)
		go func() {
			p := exec.Command("ssh", cmds...)
			p.Stdin = &pipeIO{uri.stdin, nil}
			p.Stdout = &pipeIO{uri.stdout, nil}
			p.Stderr = os.Stderr
			uri.echan <- p.Run()
		}()

		select {
		case ret := <-uri.stdout:
			if string(ret) != BFST_HELLO {
				err = errors.New("invalid BFST_HELLO")
			}
		case err = <-uri.echan:
		case <-time.After(time.Second * 5):
			err = errors.New("BFST_HELLO timed out")
		}
		if err != nil {
			uri.close()
			return errors.New("ssh failed: " + err.Error())
		}
	}
	return
}

func (uri *URI) close() {
	if uri.stdin != nil {
		uri.stdin <- nil
	}
	uri.stdin = nil
}

func (uri *URI) init() error {
	switch uri.proto {
	case "ssh":
		{
			ret, err := uri.runSSH("pwd", nil)
			if err != nil {
				return errors.New("ssh failed: " + err.Error())
			}
			str := strings.Trim(string(ret), " \t\r\n")
			subdir := uri.user
			if len(uri.path) > 0 {
				subdir += "/" + uri.path
			}
			if !strings.HasSuffix(str, subdir) {
				// run mkdir in home directory
				uri.runSSH("mkdir -p "+uri.path, nil)
			}

			// put bfst to directory
			elf, err := ioutil.ReadFile("bfst")
			if err != nil {
				return err
			}
			print("put bfst ...")
			ret, err = uri.runSSH("cat >bfst; chmod 755 bfst; sha256sum bfst", elf)
			println("")

			if err != nil {
				return errors.New("put bfst error")
			}
			rhash := sha256.Sum256(elf)
			if strings.Index(string(ret), hex.EncodeToString(rhash[:])) < 0 {
				return errors.New("verify bfst error")
			}

			err = uri.runSSH0("./bfst .init")
			if err != nil {
				return errors.New("bfst init error")
			}
			return uri.open()
		}

	case "file":
		{
			os.MkdirAll(uri.path, 0755)
			ioutil.WriteFile(uri.path+"/index", []byte(""), 0644)
			ioutil.WriteFile(uri.path+"/"+LOCKFILE, []byte(""), 0644)

			index := uri.allIndex()
			if index == nil {
				return errors.New("no index")
			}
			for i := 0; i < 256; i++ {
				fmt.Printf("\rinit=%d count=%d  ", i, len(index))
				uri.localInit(fmt.Sprintf("%02x", i), index)
			}
			println("")
			err := uri.localWriteIndex(index)
			os.Remove(uri.path + "/" + LOCKFILE)
			return err
		}

	default:
		return errors.New(uri.proto + NOSUPPORT)
	}

}

func (uri *URI) allIndex() map[string]int {
	var ret []byte
	switch uri.proto {
	case "ssh":
		ret, _ = uri.runSSH("cat index", nil)

	case "file":
		ret, _ = ioutil.ReadFile(uri.path + "/index")
	}

	if ret == nil {
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

func (uri *URI) ls(flags []string) ([]byte, error) {
	switch uri.proto {
	case "ssh":
		return uri.runRemote("ls", []byte(strings.Join(flags, "\n")))

	case "file":
		{
			files, err := uri.localListFiles(flags)
			if err != nil {
				return nil, err
			}
			ret := ""
			for _, file := range files {
				ret += fmt.Sprintf("%-20s %-12d %s\n", strings.ReplaceAll(file.mtime.Format(time.RFC3339)[:19], "T", " "), file.size, file.name)
			}
			return []byte(ret), nil

		}
	default:
		return nil, errors.New(uri.proto + NOSUPPORT)
	}
}

func (uri *URI) getIndex(flags []string) ([]byte, error) {
	switch uri.proto {
	case "ssh":
		return uri.runRemote("getIndex", []byte(strings.Join(flags, "\n")))

	case "file":
		{
			files, err := uri.localListFiles(flags)
			if err != nil {
				return nil, err
			}
			ret := ""
			for _, file := range files {
				ret += file.index()
			}
			return []byte(ret), nil
		}

	default:
		return nil, errors.New(uri.proto + NOSUPPORT)
	}
}

func (uri *URI) putIndex(lines []string) error {
	switch uri.proto {
	case "ssh":
		{
			_, err := uri.runRemote("putIndex", []byte(strings.Join(lines, "\n")))
			return err
		}
	case "file":
		{
			return uri.localPutIndex(lines)
		}
	default:
		return errors.New(uri.proto + NOSUPPORT)
	}
}

func (uri *URI) getBlock(hash string) ([]byte, error) {
	switch uri.proto {
	case "ssh":
		return uri.runRemote("getBlock", []byte(hash))
	case "file":
		{
			path := uri.path + "/" + hash[:2] + "/" + hash[2:4]
			return ioutil.ReadFile(path + "/" + hash[4:])
		}
	default:
		return nil, errors.New(uri.proto + NOSUPPORT)
	}
}

func (uri *URI) putBlock(data []byte) error {
	switch uri.proto {
	case "ssh":
		{
			_, err := uri.runRemote("putBlock", data)
			return err
		}

	case "file":
		{
			rhash := sha256.Sum256(data)
			hash := hex.EncodeToString(rhash[:])
			path := uri.path + "/" + hash[:2] + "/" + hash[2:4]
			os.MkdirAll(path, 0755)
			err := ioutil.WriteFile(path+"/"+hash[4:], data, 0644)
			if err != nil {
				return err
			}
			err = uri.localLockIndex()
			if err != nil {
				return err
			}
			defer os.Remove(uri.path + "/" + LOCKFILE)
			index := uri.allIndex()
			if index == nil {
				return errors.New("no index")
			}
			index[hash] = len(data)
			return uri.localWriteIndex(index)
		}

	default:
		return errors.New(uri.proto + NOSUPPORT)
	}
}

func (uri *URI) rm(flags []string) ([]byte, error) {
	switch uri.proto {
	case "ssh":
		return uri.runRemote("rm", []byte(strings.Join(flags, "\n")))

	case "file":
		{
			files, err := uri.localListFiles(flags)
			if err != nil {
				return nil, err
			}
			ret := ""
			for _, file := range files {
				os.Remove(uri.path + "/" + file.name + ".idx")
				ret += fmt.Sprintf("%s removed\n", file.name)
			}
			return []byte(ret), nil
		}

	default:
		return nil, errors.New(uri.proto + NOSUPPORT)
	}
}

func (uri *URI) remote() {
	read := func() []byte {
		hdr := make([]byte, 4)
		n, err := os.Stdin.Read(hdr)
		if err != nil || n != 4 || hdr[0] != 0x26 {
			return nil
		}

		n = (int(hdr[1]) << 16) + (int(hdr[2]) << 8) + (int(hdr[3]) << 0)
		data := make([]byte, n)

		i := 0
		for i < n {
			c, err := os.Stdin.Read(data[i:])
			if err != nil {
				break
			}
			i += c
		}
		return data
	}
	write := func(data []byte) {
		n := len(data)
		hdr := make([]byte, 4)
		hdr[0] = 0x26
		hdr[1] = byte(n >> 16)
		hdr[2] = byte(n >> 8)
		hdr[3] = byte(n >> 0)
		os.Stdout.Write(append(hdr, data...))
	}

	write([]byte(BFST_HELLO))

	//fmt.Fprintln(os.Stderr, "!after hello")
	for {
		data := read()
		if data == nil {
			//fmt.Fprintln(os.Stderr, "!exit loop")
			break
		}
		n := data[0] + 1
		cmd := string(data[1:n])
		data = data[n:]

		//fmt.Fprintf(os.Stderr, "!read %s %d\n", cmd, len(data))

		var bs []byte
		var err error
		switch cmd {
		case "ls":
			bs, err = uri.ls(strings.Split(string(data), "\n"))
		case "getIndex":
			bs, err = uri.getIndex(strings.Split(string(data), "\n"))
		case "putIndex":
			err = uri.putIndex(strings.Split(string(data), "\n"))
		case "getBlock":
			bs, err = uri.getBlock(string(data))
		case "putBlock":
			err = uri.putBlock(data)
		case "rm":
			bs, err = uri.rm(strings.Split(string(data), "\n"))
		default:
			err = errors.New("invalid command " + cmd)
		}

		//fmt.Fprintf(os.Stderr, "!write %d %v\n", len(bs), err)
		if err != nil {
			write([]byte("E: " + err.Error()))
		} else {
			write(bs)
		}
	}
}
