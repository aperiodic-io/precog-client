package precog

// Precog's native wire formats, as precog's crates/mesh/src/proto.rs and rpc.rs define them (src/proto.rs and src/rpc.rs here).
//
//	frame := u32 len | u8 kind | u64 req_id | body           (len covers kind..body)
//	batch := "PCB2" | u16 len | stream | u16 len | writer | u64 epoch | u64 first_seq
//	       | u32 count | u32 ident | count * (u64 time | u16 key_len | u32 payload_len | key | payload)
//	       | u32 crc32c(everything before)
//	PCZ2:  header as PCB2 | u32 raw_len | count * (u64 time | u16 key_len | u32 payload_len | key)
//	       | u32 block_len | lz4 block of every payload | u32 crc32c
//
// All integers are little-endian. Control requests and responses are JSON.

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/pierrec/lz4/v4"
)

const (
	kindHello       = 1
	kindAppend      = 2
	kindAck         = 4
	kindControl     = 5
	kindControlResp = 6
	kindError       = 7
	kindBatches     = 8

	maxFrameBytes = 16 << 20
	maxBatchBytes = 8 << 20
	maxEvents     = 65536
	maxKey        = 1024
	maxName       = 255
	tombstone     = ^uint32(0) // PCB2: a dropped event
	dropped       = 1 << 31    // PCZ2: a dropped event's payload length flag
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func encodeFrame(kind byte, reqID uint64, body []byte) []byte {
	b := make([]byte, 13, 13+len(body))
	binary.LittleEndian.PutUint32(b, uint32(9+len(body)))
	b[4] = kind
	binary.LittleEndian.PutUint64(b[5:], reqID)
	return append(b, body...)
}

// encodeAppend is an Append body: u8 copies | batch.
func encodeAppend(copies byte, batch []byte) []byte {
	return append([]byte{copies}, batch...)
}

type logID struct {
	Stream, Writer string
	Epoch          uint64
}

// pending is a buffered event: (time, dedup or compaction key, payload).
type pending struct {
	time         uint64
	key, payload []byte
}

func encodeBatch(log logID, firstSeq uint64, events []pending, compress bool) ([]byte, error) {
	switch {
	case len(events) == 0 || len(events) > maxEvents:
		return nil, fmt.Errorf("precog: %d events in a batch", len(events))
	case log.Stream == "" || log.Writer == "" || len(log.Stream) > maxName || len(log.Writer) > maxName:
		return nil, errors.New("precog: stream and writer names must be 1..255 bytes")
	case firstSeq+uint64(len(events)) < firstSeq:
		return nil, errors.New("precog: sequence overflow")
	}
	le := binary.LittleEndian
	b := []byte("PCB2")
	if compress {
		b = []byte("PCZ2")
	}
	for _, n := range []string{log.Stream, log.Writer} {
		b = le.AppendUint16(b, uint16(len(n)))
		b = append(b, n...)
	}
	b = le.AppendUint64(b, log.Epoch)
	b = le.AppendUint64(b, firstSeq)
	b = le.AppendUint32(b, uint32(len(events)))
	b = le.AppendUint32(b, 0) // ident 0: the batch is its own identity
	var raw []byte
	if compress {
		for _, e := range events {
			raw = append(raw, e.payload...)
		}
		b = le.AppendUint32(b, uint32(len(raw)))
	}
	for _, e := range events {
		if len(e.key) > maxKey || len(e.payload) >= dropped {
			return nil, errors.New("precog: dedup key or payload too long")
		}
		b = le.AppendUint64(b, e.time)
		b = le.AppendUint16(b, uint16(len(e.key)))
		b = le.AppendUint32(b, uint32(len(e.payload)))
		b = append(b, e.key...)
		if !compress {
			b = append(b, e.payload...)
		}
	}
	if compress {
		block := make([]byte, lz4.CompressBlockBound(len(raw)))
		n, err := lz4.CompressBlock(raw, block, nil)
		if err != nil {
			return nil, err
		}
		b = le.AppendUint32(b, uint32(n))
		b = append(b, block[:n]...)
	}
	if len(b)+4 > maxBatchBytes {
		return nil, fmt.Errorf("precog: batch too large (%d bytes)", len(b)+4)
	}
	return le.AppendUint32(b, crc32.Checksum(b, castagnoli)), nil
}

type batch struct {
	log      logID
	end      uint64 // one past the last sequence
	events   []Event
	blockOff int // PCZ2: where the lz4 block starts
}

// reader takes little-endian fields off a byte slice; a short read sets err and yields zeros.
type reader struct {
	b   []byte
	err error
}

func (r *reader) take(n int) []byte {
	if r.err != nil || n < 0 || len(r.b) < n {
		r.err = errors.New("precog: truncated batch")
		return make([]byte, max(n, 0))
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}
func (r *reader) u16() int    { return int(binary.LittleEndian.Uint16(r.take(2))) }
func (r *reader) u32() uint32 { return binary.LittleEndian.Uint32(r.take(4)) }
func (r *reader) u64() uint64 { return binary.LittleEndian.Uint64(r.take(8)) }

// parseBatch verifies a batch (PCB1, PCB2 or PCZ2) and returns its events, without dropped ones.
func parseBatch(b []byte) (*batch, error) {
	if len(b) < 32 || len(b) > maxBatchBytes {
		return nil, errors.New("precog: bad batch length")
	}
	body := b[:len(b)-4]
	if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return nil, errors.New("precog: batch checksum mismatch")
	}
	magic := string(body[:4])
	v1, z := magic == "PCB1", magic == "PCZ2"
	if !v1 && !z && magic != "PCB2" {
		return nil, fmt.Errorf("precog: bad batch magic %q", magic)
	}
	r := &reader{b: body[4:]}
	stream := string(r.take(r.u16()))
	writer := string(r.take(r.u16()))
	p := &batch{log: logID{stream, writer, r.u64()}}
	first := r.u64()
	count := r.u32()
	if !v1 {
		r.u32() // ident
	}
	if r.err == nil && (stream == "" || writer == "" || count == 0 || count > maxEvents) {
		return nil, errors.New("precog: bad batch header")
	}
	p.end = first + uint64(count)
	ev := func(i int, t uint64, payload []byte) {
		p.events = append(p.events, Event{Stream: stream, Writer: writer, Epoch: p.log.Epoch,
			Seq: first + uint64(i), EventTimeUs: t, Payload: payload})
	}
	if !z {
		for i := range int(count) {
			t, k := r.u64(), 0
			if !v1 {
				k = r.u16()
			}
			n := r.u32()
			if n == tombstone && !v1 {
				continue
			}
			r.take(k)
			ev(i, t, r.take(int(n)))
		}
		if r.err == nil && len(r.b) != 0 {
			return nil, errors.New("precog: trailing bytes in batch")
		}
		return p, r.err
	}
	rawLen := r.u32()
	type col struct {
		t       uint64
		n       uint32
		dropped bool
	}
	cols := make([]col, 0, count)
	sum := uint64(0)
	for range count {
		t, k, n := r.u64(), r.u16(), r.u32()
		r.take(k)
		cols = append(cols, col{t, n &^ dropped, n&dropped != 0})
		sum += uint64(n &^ dropped)
	}
	blockLen := int(r.u32())
	if r.err != nil || sum != uint64(rawLen) || rawLen > 4*maxBatchBytes || blockLen != len(r.b) {
		return nil, errors.New("precog: bad compressed batch")
	}
	p.blockOff = len(body) - blockLen
	raw := make([]byte, rawLen)
	if n, err := lz4.UncompressBlock(r.b, raw); err != nil || n != len(raw) {
		return nil, fmt.Errorf("precog: damaged compressed block: %v", err)
	}
	at := 0
	for i, c := range cols {
		if !c.dropped {
			ev(i, c.t, raw[at:at+int(c.n)])
		}
		at += int(c.n)
	}
	return p, nil
}

// Control JSON: serde's `#[serde(tag = "op", rename_all = "snake_case")]` enum. Fields keep
// the Rust declaration order of every op that has them, so requests are byte-identical.
type control struct {
	Op       string      `json:"op"`
	Group    string      `json:"group,omitempty"`
	Stream   string      `json:"stream"`
	Cursor   *wireCursor `json:"cursor,omitempty"`
	MaxBytes *uint32     `json:"max_bytes,omitempty"`
	TimeMs   *uint64     `json:"time_ms,omitempty"`
}

type hello struct {
	Token  string  `json:"token"`
	NodeID *uint32 `json:"node_id"`
}

type controlResp struct {
	Op      string        `json:"op"`
	Cursor  *wireCursor   `json:"cursor"`
	Entries [][2][]uint16 `json:"entries"` // Vec<(Vec<u8>, Vec<u8>)>: arrays of numbers
}

func (r *controlResp) entries() []Entry {
	bs := func(v []uint16) []byte {
		b := make([]byte, len(v))
		for i, x := range v {
			b[i] = byte(x)
		}
		return b
	}
	es := make([]Entry, len(r.Entries))
	for i, e := range r.Entries {
		es[i] = Entry{Key: bs(e[0]), Value: bs(e[1])}
	}
	return es
}

// wireCursor is a cursor as JSON: [[writer, epoch, next seq], ...]. Never nil when sent: Rust
// wants [] for an empty cursor, not null.
type wireCursor []wireEntry

type wireEntry struct {
	Writer     string
	Epoch, Seq uint64
}

func (e wireEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{e.Writer, e.Epoch, e.Seq})
}

func (e *wireEntry) UnmarshalJSON(b []byte) error {
	var a []json.RawMessage
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	if len(a) != 3 {
		return fmt.Errorf("precog: bad cursor entry %s", b)
	}
	return errors.Join(json.Unmarshal(a[0], &e.Writer), json.Unmarshal(a[1], &e.Epoch),
		json.Unmarshal(a[2], &e.Seq))
}
