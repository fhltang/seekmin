package seekmin

import (
	"expvar"
	"fmt"
	"github.com/fhltang/seekmin/bpipe"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

// expvar vars
var (
	readTime  = expvar.NewInt("seekmin_read_time")
	readBytes = expvar.NewInt("seekmin_read_bytes")
	readFiles = expvar.NewInt("seekmin_read_files")
)

// Implementation of a Reader goroutine that reads bytes from a file.
//
// The bytes are read from a file and then written into a buffered
// pipe.  The reader end of the buffered pipe is inserted into a queue
// to be processed by hasher goroutines.

func Reader(filenames <-chan string, itemsToHash chan<- ItemToHash, bufMan *bpipe.BufMan, pending *sync.WaitGroup) {
	for file := range filenames {
		doRead(file, itemsToHash, bufMan, pending)
	}
}

func doRead(file string, itemsToHash chan<- ItemToHash, bufMan *bpipe.BufMan, pending *sync.WaitGroup) {
	// For each file, we create a buffered pipe.  We sequentially
	// write into this pipe and concurrently read from the pipe,
	// computing its hash.

	pr, pw := bpipe.BufferedPipe(bufMan)
	defer pw.Close()

	f, err := os.Open(file)
	if err != nil {
		fmt.Printf("%s: ERROR\n", file)
		return
	}
	defer f.Close()

	itemsToHash <- ItemToHash{Reader: pr, Filename: file}

	var count int64
	countElapsed(readTime, readFiles, func() {
		count, err = io.Copy(pw, f)
		if err != nil {
			log.Printf("Failed reading file %s: %s", file, err)
			return
		}
		readBytes.Add(count)
	})
}

func countElapsed(elapsed *expvar.Int, count *expvar.Int, f func()) {
	start := time.Now()
	f()
	delta := time.Since(start)
	elapsed.Add(int64(delta / time.Nanosecond))
	count.Add(1)
}
