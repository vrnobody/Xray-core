package splithttp

// upload_queue is a specialized priorityqueue + channel to reorder generic
// packets by a sequence number

import (
	"io"
	"sync"
)

type Packet struct {
	Payload []byte
	Seq     uint64
}

type UploadQueue struct {
	sync.RWMutex

	pushedPackets chan *Packet
	tickets       chan struct{}
	buff          []byte
	nextSeq       uint64
	closed        bool
	maxPackets    uint64
}

func NewUploadQueue(maxPackets int) *UploadQueue {
	tickets := make(chan struct{}, maxPackets)
	for i := 0; i < maxPackets; i++ {
		tickets <- struct{}{}
	}
	return &UploadQueue{
		pushedPackets: make(chan *Packet, maxPackets+1),
		tickets:       tickets,
		buff:          nil,
		nextSeq:       0,
		closed:        false,
		maxPackets:    uint64(maxPackets),
	}
}

func (h *UploadQueue) Push(p *Packet) error {
	h.RWMutex.RLock()
	defer h.RWMutex.RUnlock()
	if h.closed {
		return newError("splithttp packet queue closed")
	}
	h.pushedPackets <- p
	return nil
}

func (h *UploadQueue) Wait() error {
	if _, more := <-h.tickets; !more {
		return newError("splithttp packet queue closed")
	}
	return nil
}

func (h *UploadQueue) signal() {
	h.RWMutex.RLock()
	defer h.RWMutex.RUnlock()
	if h.closed {
		return
	}
	h.tickets <- struct{}{}
}

func (h *UploadQueue) Close() error {
	h.RWMutex.Lock()
	h.closed = true
	h.RWMutex.Unlock()
	close(h.tickets)
	close(h.pushedPackets)
	return nil
}

func (h *UploadQueue) Read(b []byte) (int, error) {
	if h.closed && len(h.buff) < 1 && len(h.pushedPackets) < 1 {
		return 0, io.EOF
	}

	for {
		// try to read from buffer
		l := len(h.buff)
		if l > 0 {
			n := copy(b, h.buff)
			if n < l {
				h.buff = h.buff[n:]
			} else {
				h.buff = nil
			}
			return n, nil
		}

		// read chan to buffer
		p, more := <-h.pushedPackets
		if !more {
			return 0, io.EOF
		}
		if p.Seq != h.nextSeq {
			return 0, newError("packet lost")
		}
		h.buff = p.Payload
		h.nextSeq++
		h.signal()
	}
}
