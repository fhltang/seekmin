package seekmin

import (
	"crypto/md5"
	"expvar"
	"fmt"
	"io"
	"log"
	"sync"
)

// expvar vars
var (
	hasherStart = expvar.NewInt("seekmin_hasher_start")
	hasherDone  = expvar.NewInt("seekmin_hasher_done")
	hashedBytes = expvar.NewInt("seekmin_hashed_bytes")
)

type ItemToHash struct {
	Reader   io.Reader
	Filename string
}

func Hasher(itemsToHash chan ItemToHash, pending *sync.WaitGroup) {
	for item := range itemsToHash {
		doHash(item, pending)
	}
}

func doHash(item ItemToHash, pending *sync.WaitGroup) {
	defer pending.Done()
	hasherStart.Add(1)
	defer hasherDone.Add(1)
	hash := md5.New()
	count, err := io.Copy(hash, item.Reader)
	if err != nil {
		log.Printf("Failed hashing %s: %s", item.Filename, err)
		return
	}
	hashedBytes.Add(count)
	fmt.Printf("%x  %s\n", hash.Sum(nil), item.Filename)
}
