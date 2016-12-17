// A reimplementation of md5sum that reads its files in a single
// thread but computes the hash in parallel.
//
// This approach is advantageous if single-threaded hash computation
// is slower than disk read throughput.  Since files are read
// sequentially, disk seeks are minimised.

package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"github.com/fhltang/bpipe"
	"io"
	"os"
	"sync"
)

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

func seekmin(files []string) {
	bufMan := bpipe.NewBufMan(*blockSize, *maxBlocks)

	var wg sync.WaitGroup

	// For each file, we create a buffered pipe.  We sequentially
	// write into this pipe and concurrently read from the pipe,
	// computing its hash.
	for _, file := range files {
		pr, pw := bpipe.BufferedPipe(bufMan)

		f, err := os.Open(file)
		if err != nil {
			fmt.Printf("%s: ERROR\n", file)
			continue
		}

		wg.Add(1)
		go func(file string) {
			hash := md5.New()
			io.Copy(hash, pr)
			fmt.Printf("%x  %s\n", hash.Sum(nil), file)
			wg.Done()
		}(file)

		io.Copy(pw, f)
		pw.Close()
		f.Close()
	}

	wg.Wait()
}

func main() {
	flag.Parse()

	seekmin(flag.Args())
}
