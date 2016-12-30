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
	"github.com/fhltang/seekmin/bpipe"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

import _ "net/http/pprof"

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

// expvar vars
var (
	hasherStart = expvar.NewInt("seekmin_hasher_start")
	hasherDone  = expvar.NewInt("seekmin_hasher_done")
	readTime    = expvar.NewInt("seekmin_read_time")
	readBytes   = expvar.NewInt("seekmin_read_bytes")
	readFiles   = expvar.NewInt("seekmin_read_files")
	hashedBytes = expvar.NewInt("seekmin_hashed_bytes")
	uptime      = Uptime()
)

type UptimeVar struct {
	birthtime time.Time
}

func (this *UptimeVar) String() string {
	return fmt.Sprintf("%d", time.Since(this.birthtime)/time.Nanosecond)
}

func Uptime() *UptimeVar {
	uptime := &UptimeVar{birthtime: time.Now()}
	expvar.Publish("uptime", uptime)
	return uptime
}

func countElapsed(elapsed *expvar.Int, count *expvar.Int, f func()) {
	start := time.Now()
	f()
	delta := time.Since(start)
	elapsed.Add(int64(delta / time.Nanosecond))
	count.Add(1)
}

type SeekMin struct {
	wait sync.WaitGroup
	bufMan *bpipe.BufMan
}

func NewSeekMin(blockSize int, maxBlocks int) *SeekMin {
	return &SeekMin{
		bufMan: bpipe.NewBufMan("default", maxBlocks, blockSize),
	}
}

func (this *SeekMin) processFile(file string) {
	// For each file, we create a buffered pipe.  We sequentially
	// write into this pipe and concurrently read from the pipe,
	// computing its hash.

	pr, pw := bpipe.BufferedPipe(this.bufMan)
	defer pw.Close()

	f, err := os.Open(file)
	if err != nil {
		fmt.Printf("%s: ERROR\n", file)
		return
	}
	defer f.Close()

	this.wait.Add(1)
	go func() {
		defer this.wait.Done()
		hasherStart.Add(1)
		defer hasherDone.Add(1)
		hash := md5.New()
		count, err := io.Copy(hash, pr)
		if err != nil {
			log.Printf("Failed hashing file %s: %s", file, err)
			return
		}
		hashedBytes.Add(count)
		fmt.Printf("%x  %s\n", hash.Sum(nil), file)
	}()

	var count int64
	countElapsed(readTime, readFiles, func() {
		count, err = io.Copy(pw, f)
		if err != nil{
			log.Printf("Failed reading file %s: %s", file, err)
			return
		}
		readBytes.Add(count)
	})
}

func (this *SeekMin) Run(files []string) {
	if len(files) == 0 {
		// No files provided.  Read files from stdin.
		reader := bufio.NewReader(os.Stdin)
		delim := byte('\n')
		if *useNulDelim {
			delim = 0
		}
		var file string
		var err error
		for {
			file, err = reader.ReadString(delim)
			if err != nil {
				log.Fatalf("Failed to read filename %s: %s", file, err)
			}
			file = file[:len(file)-1]
			this.processFile(file)
		}

	} else {
		for _, file := range files {
			this.processFile(file)
		}
	}

	this.wait.Wait()
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

	seekmin := NewSeekMin(*blockSize, *maxBlocks)
	seekmin.Run(flag.Args())
	if !*exitOnCompletion {
		server_exited.Wait()
	}
}
