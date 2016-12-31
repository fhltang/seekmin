package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"os"
)

func main() {
	flag.Parse()

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
}
