package bpipe

import (
	"bytes"
	"container/list"
	"expvar"
	"github.com/fhltang/bpool"
	"io"
	"sync"
)

var (
	bufmanBytes = expvar.NewInt("bufman_bytes")
	bufmanBuffers = expvar.NewInt("bufman_buffers")
)

// BufMan is a buffer manager.  It manages a pool of byte slices.
type BufMan struct {
	Name string
	pool *bpool.BPool
}

// Creat a BufMan which can allocate up to `max` slices each with
// capacity `bufCap`.
func NewBufMan(name string, max int, bufCap int) *BufMan {
	newBuf := func() interface{} {
		return make([]byte, bufCap)
	}
	return &BufMan{Name: name, pool: bpool.New(max, newBuf)}
}

func (this *BufMan) Acquire() []byte {
	return this.pool.Get().([]byte)
}

func (this *BufMan) Release(buffer []byte) {
	this.pool.Put(buffer)
}

// State for a BufferedPipe.
type bufferedPipeState struct {
	// Buffer manager used to allocate byte slices.
	bufMan *BufMan

	// Condition variable to protect the following fields.
	cond *sync.Cond

	// List of bytes.Buffer.
	//
	// Stores bytes written to this pipe.  If all bytes in a
	// bytes.Buffer have been read, it is removed from the list.
	//
	// Note that the condition variable protects structural
	// manipulations of this list (i.e. delete and push-back) but
	// not its contents.
	pending *list.List

	// Error from the writer, or io.EOF if the pipe has been
	// closed by the writer.
	writerErr error
}

type BufferedPipeReader struct {
	state *bufferedPipeState
}

func (this *BufferedPipeReader) Read(p []byte) (int, error) {
	var cum int = 0
	var err error = nil
	var we error = nil
	var n int = 0

	var e *list.Element
	target := len(p)
	for cum < target {
		this.state.cond.L.Lock()
		for this.state.pending.Front() == nil && this.state.writerErr == nil {
			this.state.cond.Wait()
		}
		e = this.state.pending.Front()
		we = this.state.writerErr
		this.state.cond.L.Unlock()

		if e == nil {
			err = we
			break
		}

		buffer := e.Value.(*bytes.Buffer)
		n, err = buffer.Read(p)
		cum += n

		p = p[n:]

		if err == io.EOF {
			this.state.cond.L.Lock()
			this.state.pending.Remove(e)
			this.state.cond.L.Unlock()
			e = nil

			this.state.bufMan.Release(buffer.Bytes())
		} else if err != nil {
			break
		}
	}

	return cum, err
}

type BufferedPipeWriter struct {
	state *bufferedPipeState
}

func (this *BufferedPipeWriter) Close() {
	this.state.cond.L.Lock()
	defer this.state.cond.L.Unlock()
	this.state.writerErr = io.EOF
	this.state.cond.Signal()
}

func (this *BufferedPipeWriter) Write(p []byte) (int, error) {
	r := bytes.NewReader(p)
	return this.ReadFrom(r)
}

func (this *BufferedPipeWriter) ReadFrom(r io.Reader) (int, error) {
	var err error

	var cum int

	for {
		buffer := this.state.bufMan.Acquire()
		buffer = buffer[:cap(buffer)]

		n, er := r.Read(buffer)
		if n == 0 {
			this.state.bufMan.Release(buffer)
			break
		}
		buffer = buffer[:n]
		cum += n

		this.state.cond.L.Lock()
		this.state.pending.PushBack(bytes.NewBuffer(buffer))
		this.state.cond.L.Unlock()

		this.state.cond.Signal()

		if er == io.EOF {
			break
		}
		if er != nil {
			err = er
			break
		}
	}

	return int(cum), err
}

func BufferedPipe(bufMan *BufMan) (*BufferedPipeReader, *BufferedPipeWriter) {
	state := bufferedPipeState{
		bufMan:  bufMan,
		cond:    sync.NewCond(&sync.Mutex{}),
		pending: list.New(),
	}
	return &BufferedPipeReader{state: &state}, &BufferedPipeWriter{state: &state}
}
