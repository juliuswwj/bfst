# under ubuntu
# 1. install go
#   wget https://dl.google.com/go/go1.14.4.linux-amd64.tar.gz
#   sudo tar -C /usr/local -xzf go1.14.4.linux-amd64.tar.gz
#   export PATH=$PATH:/usr/local/go/bin
# 2. install mingw
#   sudo apt-get install gcc-multilib gcc-mingw-w64

all: bfst bfst.exe

TESTPATH=bfst_test
TESTURI=$(USER)@localhost/$(TESTPATH)
test: bfst
	- rm -rf ~/$(TESTPATH)
	./bfst $(TESTURI) init
	dd if=/dev/zero of=/tmp/zero.dat bs=1M count=4200
	./bfst $(TESTURI) put /tmp/zero.dat
	./bfst $(TESTURI) ls

bfst: $(wildcard *.go)
	go build -ldflags="-s -w" -o bfst

bfst.exe: $(wildcard *.go)
	GOOS=windows GOARCH=386 CGO_ENABLED=1 CXX=i686-w64-mingw32-g++ CC=i686-w64-mingw32-gcc go build -ldflags="-s -w" -o bfst.exe
