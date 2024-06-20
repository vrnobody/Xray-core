package splithttp

// upload_queue is a specialized priorityqueue + channel to reorder generic
// packets by a sequence number

import (
	"io"
	"sync"
	"time"
)

type UploadQueue struct {
	readSignalSize int
	readSignal     chan struct{}

	writeMu      *sync.Mutex
	writeSignal  *CondWithTimeout
	writeTimeout time.Duration

	buffers    *sync.Map
	bufferSize uint64
	cache      []byte

	seq    uint64
	closed bool
}

func NewUploadQueue(size int) *UploadQueue {
	writeMutex := sync.Mutex{}

	return &UploadQueue{
		readSignalSize: 3,
		readSignal:     make(chan struct{}, 2*3),

		writeMu:      &writeMutex,
		writeSignal:  NewCondWithTimeout(&writeMutex),
		writeTimeout: 10 * time.Second,

		bufferSize: uint64(2 * size),
		buffers:    &sync.Map{},
		cache:      nil,

		seq:    0,
		closed: false,
	}
}

func (h *UploadQueue) Push(seq uint64, payload []byte) error {
	// notify reader
	defer h.sendReadSignal()

	if h.closed {
		return newError("splithttp packet queue closed")
	}

	// save packet to buffer
	h.buffers.Store(seq, payload)
	return nil
}

// Wait until buffer is available
func (h *UploadQueue) Wait(seq uint64) error {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()

	signal := true
	for signal && !h.closed && seq >= h.seq+h.bufferSize {
		signal = h.writeSignal.WaitWithTimeout(h.writeTimeout)
	}

	if h.closed {
		return newError("splithttp packet queue closed")
	}

	if !signal {
		return newError("splithttp wait timeout")
	}
	return nil
}

func (h *UploadQueue) Close() error {
	h.closed = true
	h.writeSignal.Broadcast()
	h.sendReadSignal()
	return nil
}

func (h *UploadQueue) sendReadSignal() {
	if len(h.readSignal) < h.readSignalSize {
		h.readSignal <- struct{}{}
	}
}

func (h *UploadQueue) Read(b []byte) (int, error) {
	for {
		// try to read from cache
		if l := len(h.cache); l > 0 {
			n := copy(b, h.cache)
			if n < l {
				h.cache = h.cache[n:]
			} else {
				h.cache = nil
			}
			return n, nil
		}

		// try to load from buffer
		if v, ok := h.buffers.LoadAndDelete(h.seq); ok {
			h.cache = v.([]byte)
			h.seq++

			// notify writer
			h.writeSignal.Broadcast()
		} else if h.closed {
			return 0, io.EOF
		} else {
			// await writer
			<-h.readSignal
		}
	}
}
