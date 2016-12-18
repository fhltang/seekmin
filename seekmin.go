// A reimplementation of md5sum that reads its files in a single
// thread but computes the hash in parallel.
//
// This approach is advantageous if single-threaded hash computation
// is slower than disk read throughput.  Since files are read
// sequentially, disk seeks are minimised.

package main

import (
	"bufio"
	"crypto/md5"
	"expvar"
	"flag"
	"fmt"
	"github.com/fhltang/bpipe"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
)

import _"net/http/pprof"


var port = flag.Int(
	"port", 0,
	"Port on which to run an HTTP server for debug stats.")
var exitOnCompletion = flag.Bool(
	"exit_on_completion", false,
	"Exit on completion.  If --port is specified, setting this to false "+
		"will cause the program to continue listening for HTTP "+
		"requets.")
var blockSize = flag.Int(
	"block_size", 65536,
	"File read block size.  A small block size allows memory to be used "+
		"more efficiently and reduces latency.  A large block size "+
		"may result in more wasted buffer memory, increased latency "+
		"but potentially higher read throughput.")
var maxBlocks = flag.Int(
	"max_blocks", 1024,
	"Maximum number of blocks used for buffer read bytes.  A large value "+
		"uses more memory at peak but reduces the frequency of reads "+
		"blocked on hash computation.")
var useNulDelim = flag.Bool(
	"null", false,
	"Use NULL as delimiter between files when reading from stdin.  "+
		"Intended to be used in conjuction with find -print0.")

const HASHER = "hasher"
const READER = "reader"

// expvar vars
var (
	goroutineStart *expvar.Map
	goroutineDone *expvar.Map
)

func init() {
	goroutineStart = expvar.NewMap("goroutine_start")
	goroutineDone = expvar.NewMap("goroutine_done")
}

func processFile(wg *sync.WaitGroup, bufMan *bpipe.BufMan, file string) {
	// For each file, we create a buffered pipe.  We sequentially
	// write into this pipe and concurrently read from the pipe,
	// computing its hash.

	pr, pw := bpipe.BufferedPipe(bufMan)

	f, err := os.Open(file)
	if err != nil {
		fmt.Printf("%s: ERROR\n", file)
		return
	}

	wg.Add(1)
	go func() {
		goroutineStart.Add(HASHER, 1)
		defer goroutineDone.Add(HASHER, 1)
		hash := md5.New()
		io.Copy(hash, pr)
		fmt.Printf("%x  %s\n", hash.Sum(nil), file)
		wg.Done()
	}()

	io.Copy(pw, f)
	pw.Close()
	f.Close()
}

func seekmin(files []string) {
	bufMan := bpipe.NewBufMan("default", *blockSize, *maxBlocks)

	var wg sync.WaitGroup

	if len(files) == 0 {
		reader := bufio.NewReader(os.Stdin)
		delim := byte('\n')
		if *useNulDelim {
			delim = 0
		}
		var file string
		var err error
		for err == nil {
			file, err = reader.ReadString(delim)
			if err == nil {
				file = file[:len(file)-1]
				processFile(&wg, bufMan, file)
			}
		}

	} else {
		for _, file := range files {
			processFile(&wg, bufMan, file)
		}
	}

	wg.Wait()
}

func main() {
	flag.Parse()

	var server_exited sync.WaitGroup
	if *port != 0 {
		log.Printf("Starting server on port %d", *port)
		go func() {
			server_exited.Add(1)
			log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
			server_exited.Done()
		}()
	}

	seekmin(flag.Args())
	if !*exitOnCompletion {
		server_exited.Wait()
	}
}
