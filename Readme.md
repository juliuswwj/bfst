## Big File Store

### Compile
  1. Ubuntu 18.04/20.04
  2. Install Go
```
wget https://dl.google.com/go/go1.14.4.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.14.4.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
```
  3. Install mingw GCC
```
sudo apt-get install gcc-multilib gcc-mingw-w64
```

### Build
```  
make
```

### Usage
```
    bfst user@host[:port][/path] [subcommands]
subcommands =
    init
    ls [filter1 filter2 ...]
    get file1 [file2 ...]
    put file1 [file2 ...]
```