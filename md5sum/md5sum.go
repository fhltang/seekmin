package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
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
			hash:=md5.New()
			_, err := io.Copy(hash, f)
			if err != nil {
				fmt.Printf("Failed hashing %s: %s", file, err)
				return
			}
			fmt.Printf("%x  %s\n", hash.Sum(nil), file)
		}()
	}

	if !*exitOnCompletion {
		server_exited.Wait()
	}
}
