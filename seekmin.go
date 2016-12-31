// A reimplementation of md5sum that reads its files in a single
// thread but computes the hash in parallel.
//
// This approach is advantageous if single-threaded hash computation
// is slower than disk read throughput.  Since files are read
// sequentially, disk seeks are minimised.

package main

import (
	"bufio"
	"expvar"
	"flag"
	"fmt"
	"github.com/fhltang/seekmin/bpipe"
	"github.com/fhltang/seekmin/seekmin"
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
var numReaders = flag.Int(
	"num_readers", 1,
	"Number of reader threads.  Using 1 minimises seeks which is good "+
		"for spinning disks.  SSDs may benefit from more reader threads.")
var numHashers = flag.Int(
	"num_hashers", 4,
	"Number of hasher threads.")
var hasherQueueBound = flag.Int(
	"hasher_queue_bound", 10000,
	"How many items of work can be queued up for the hashers.  Set "+
		"this to an infeasibly large number.")

// expvar vars
var (
	uptime = Uptime()
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

type SeekMin struct {
	wait   sync.WaitGroup
	bufMan *bpipe.BufMan
}

func NewSeekMin(blockSize int, maxBlocks int) *SeekMin {
	return &SeekMin{
		bufMan: bpipe.NewBufMan("default", maxBlocks, blockSize),
	}
}

func (this *SeekMin) Run(files []string) {
	filenames := make(chan string)
	itemsToHash := make(chan seekmin.ItemToHash, *hasherQueueBound)

	// Goroutines to read files.
	for i := 0; i < *numReaders; i++ {
		go seekmin.Reader(filenames, itemsToHash, this.bufMan)
	}

	// Goroutines to hash bytes read from files.
	for i := 0; i < *numHashers; i++ {
		go seekmin.Hasher(itemsToHash, &this.wait)
	}

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
			this.wait.Add(1)
			filenames <- file
		}

	} else {
		for _, file := range files {
			this.wait.Add(1)
			filenames <- file
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
