package main

import (
	"bytes"
	"errors"
	"io"
	"os/exec"
	"strings"
)

//URI struct of file store config
type URI struct {
	proto, user, host, port, path string
}

//parseURI
//support
//  user@domain:port/path
//  ssh://user@domain:port/path
//  http://user@domain:port/path
//  https://user@domain:port/path
func parseURI(str string) *URI {
	uri := new(URI)

	n := strings.Index(str, ":")
	if n < 0 {
		uri.proto = "ssh"
	} else if strings.Index(str[:n], "@") < 0 {
		uri.proto = str[:n]
		str = str[n+1:]
	}
	// remove // in uri
	if len(str) > 2 && str[:2] == "//" {
		str = str[2:]
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

func (uri *URI) ssh(cmd string, stdin io.Reader) ([]byte, error) {
	if uri.proto != "ssh" {
		return nil, errors.New("not ssh protocol")
	}

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
	p.Stderr = stdout
	err := p.Run()
	if err != nil {
		return nil, errors.New(string(stdout.Bytes()))
	}
	return stdout.Bytes(), nil
}

func (uri *URI) dir(flags []string) ([]byte, error) {
	if uri.proto == "ssh" {
		return uri.ssh("./bfst .ls", strings.NewReader(strings.Join(flags, "\n")))
	}

}

func (uri *URI) get(flags []string) ([]byte, error) {
	return uri.ssh("./bfst .get", strings.NewReader(strings.Join(flags, "\n")))
}

func (uri *URI) read(fpath string) ([]byte, error) {
	return uri.ssh("cat "+fpath, nil)
}
