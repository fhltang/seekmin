package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
)

var port = flag.Int(
	"port", 0,
	"Port on which to run an HTTP server for debug stats.")
var exitOnCompletion = flag.Bool(
	"exit_on_completion", false,
	"Exit on completion.  If --port is specified, setting this to false "+
		"will cause the program to continue listening for HTTP "+
		"requests.")
var devNull = flag.Bool(
	"dev_null", false,
	"If set to true, just read the files and do not compute checksum.")

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

	for _, file := range flag.Args() {
		f, err := os.Open(file)
		if err != nil {
			fmt.Printf("ERROR opening %s: %s", file, err)
			break
		}
		func () {
			defer f.Close()
			var writer io.Writer
			hash := md5.New()
			if *devNull {
				writer = ioutil.Discard
			} else {
				writer = hash
			}
			_, err := io.Copy(writer, f)
			if err != nil {
				fmt.Printf("Failed hashing %s: %s", file, err)
				return
			}
			if !*devNull {
				fmt.Printf("%x  %s\n", hash.Sum(nil), file)
			}
		}()
	}

	if !*exitOnCompletion {
		server_exited.Wait()
	}
}
