package bpipe_test

import (
	"bytes"
	"github.com/fhltang/seekmin/bpipe"
	"io"
	"strings"
	"testing"
)

func TestNewPipe(t *testing.T) {
	bpipe.BufferedPipe(bpipe.NewBufMan("Test", 4, 100))
}

func ReadWrite(t *testing.T,
	pipeBufSize int, readBufSize int, writeBufSize int) {
	pr, pw := bpipe.BufferedPipe(bpipe.NewBufMan("Test", pipeBufSize, 100))

	r := strings.NewReader("some io.Reader stream to be read\n")

	readBuf := make([]byte, readBufSize, readBufSize)
	io.CopyBuffer(pw, r, readBuf)
	pw.Close()

	writeBuf := make([]byte, writeBufSize, writeBufSize)
	var buffer bytes.Buffer
	io.CopyBuffer(&buffer, pr, writeBuf)

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
