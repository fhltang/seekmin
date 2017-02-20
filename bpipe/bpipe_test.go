package bpipe_test

import (
	"bytes"
	"fmt"
	"github.com/fhltang/seekmin/bpipe"
	"io"
	"io/ioutil"
	"strings"
	"testing"
)

func TestNewPipe(t *testing.T) {
	bpipe.BufferedPipe(bpipe.NewBufMan("Test", 4, 100))
}

func ReadWrite(t *testing.T,
	pipeBufSize int, readBufSize int, writeBufSize int) {
	pr, pw := bpipe.BufferedPipe(bpipe.NewBufMan("Test", pipeBufSize, 100))

	var n int64
	var err error
	r := strings.NewReader("some io.Reader stream to be read\n")

	readBuf := make([]byte, readBufSize, readBufSize)
	n, err = io.CopyBuffer(pw, r, readBuf)
	if n != 33 {
		t.Errorf("Unexpected count of bytes copied: %s", n)
	}
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	pw.Close()

	writeBuf := make([]byte, writeBufSize, writeBufSize)
	var buffer bytes.Buffer
	n, err = io.CopyBuffer(&buffer, pr, writeBuf)
	if n != 33 {
		t.Errorf("Unexpected count of bytes copied: %s", n)
	}
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	if buffer.String() != "some io.Reader stream to be read\n" {
		t.Error("*" + buffer.String() + "*")
	}
}

func TestReadWrite_LargeReadBuf_LargeWriteBuf(t *testing.T) {
	ReadWrite(t, 4, 8, 8)
}

func TestReadWrite_SmallReadBuf_LargeWriteBuf(t *testing.T) {
	ReadWrite(t, 4, 2, 8)
}

func TestReadWrite_LargeReadBuf_SmallWriteBuf(t *testing.T) {
	ReadWrite(t, 4, 8, 2)
}

func TestReadWrite_SmallReadBuf_SmallWriteBuf(t *testing.T) {
	ReadWrite(t, 4, 2, 2)
}

// Baseline benchmark.  Copies 1MB of bytes to ioutil.Discard.
func BenchmarkBaseline(b *testing.B) {
	src := make([]byte, 1024*1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		io.Copy(ioutil.Discard, bytes.NewReader(src))
	}

}

// Benchmark for reading and writing from a buffered pipe.
// Copies 1MB into the pipe and then 1MB out of the pipe.
func BenchmarkSerialReadWrite(b *testing.B) {
	src := make([]byte, 1024*1024)

	blockSize := 16384
	maxBlocks := 1 + len(src)/blockSize

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pr, pw := bpipe.BufferedPipe(
			bpipe.NewBufMan("test", maxBlocks, blockSize))
		r := bytes.NewReader(src)
		io.Copy(pw, r)
		pw.Close()
		io.Copy(ioutil.Discard, pr)
	}

}

// Benchmark for concurrently reading and writing from a buffered pipe.
// Concurrently writes and reads 1MB via the pipe.
func BenchmarkConcReadWrite(b *testing.B) {
	src := make([]byte, 1024*1024)

	type TestCase struct {
		blockSize, maxBlocks int
	}

	cases := []TestCase{
		// Different maxBlocks.
		{32768, 1},
		{32768, 2},
		{32768, 3},
		{32768, 4},
		{32768, 5},
		{32768, 6},
		{32768, 7},
		{32768, 8},
		{32768, 9},
		{32768, 10},
		{32768, 11},
		{32768, 12},
		{32768, 16},
		{32768, 32},

		// Different blockSize.
		{4096, 8},
		{8192, 8},
		{16384, 8},
		{65536, 8},
	}

	for _, tc := range cases {
		DoConcReadWrite := func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				pr, pw := bpipe.BufferedPipe(
					bpipe.NewBufMan("test", tc.maxBlocks, tc.blockSize))
				go func() {
					r := bytes.NewReader(src)
					io.Copy(pw, r)
					pw.Close()
				}()
				io.Copy(ioutil.Discard, pr)
			}
		}
		b.Run(fmt.Sprintf("blockSize=%d_maxBlocks=%d", tc.blockSize, tc.maxBlocks), DoConcReadWrite)
	}

}

// Benchmark for writing to a buffered pipe.
// Copies 1MB into the pipe.
func BenchmarkWrite(b *testing.B) {
	src := make([]byte, 1024*1024)

	blockSize := 16384
	maxBlocks := 1 + len(src)/blockSize

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, pw := bpipe.BufferedPipe(
			bpipe.NewBufMan("test", maxBlocks, blockSize))
		io.Copy(pw, bytes.NewReader(src))
		pw.Close()
	}

}

// Benchmark for reading from a buffered pipe.
//
// Copies 1MB into the pipe and then 1MB out of the pipe, timing only
// the read.
func BenchmarkRead(b *testing.B) {
	src := make([]byte, 1024*1024)

	blockSize := 16384
	maxBlocks := 1 + len(src)/blockSize

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		pr, pw := bpipe.BufferedPipe(
			bpipe.NewBufMan("test", maxBlocks, blockSize))
		io.Copy(pw, bytes.NewReader(src))
		pw.Close()
		b.StartTimer()
		io.Copy(ioutil.Discard, pr)
	}

}

func BenchmarkNewBufferedPipe(b *testing.B) {
	blockSize := 16384
	maxBlocks := 1024
	bufman := bpipe.NewBufMan("test", maxBlocks, blockSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bpipe.BufferedPipe(bufman)
	}
}
