// Package precog is a Go client for Precog's native protocol, mirroring the Rust SDK
// (precog's crates/client):
//
//   - Producer: batches events of one writer log, each with a dedup id, and appends them durably
//     on 2 nodes. A stream's producers all prefer the same node (xxh3(stream) % nodes), so two
//     producer replicas' copies of an event meet one dedup index. A failed append is retried
//     with the same bytes on the next node, which is idempotent.
//   - Consumer: reads a stream from a cursor that is valid on every node, failing over between
//     nodes; starts at a committed position, the beginning, the end or a time; commits.
//   - Client.Latest: a compacted stream's newest value of every key.
package precog

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zeebo/xxh3"
)

const (
	// MaxInFlight is how many batches a producer keeps waiting for their acks.
	MaxInFlight = 8
	// MaxBatchEvents is the events per batch before IsFull asks for a flush.
	MaxBatchEvents = 4096
	// MaxBatchBytes is the bytes per batch before IsFull asks for a flush.
	MaxBatchBytes = 1 << 20

	rpcTimeout     = 1500 * time.Millisecond
	appendDeadline = 30 * time.Second
)

// DedupKey is a dedup id as it travels: its 8-byte xxh3 (an empty id stays empty: never
// deduplicated).
func DedupKey(id []byte) []byte {
	if len(id) == 0 {
		return nil
	}
	return binary.LittleEndian.AppendUint64(nil, xxh3.Hash(id))
}

// Event is one event read from a stream.
type Event struct {
	Stream, Writer string
	Epoch, Seq     uint64
	EventTimeUs    uint64
	Payload        []byte
}

// Entry is a compacted stream's newest value of a key.
type Entry struct{ Key, Value []byte }

// Log names one writer log within a stream.
type Log struct {
	Writer string
	Epoch  uint64
}

// Cursor is a position in a stream valid on every node: the next sequence per writer log.
type Cursor map[Log]uint64

func (c Cursor) wire() *wireCursor {
	w := wireCursor{}
	for l, s := range c {
		w = append(w, wireEntry{l.Writer, l.Epoch, s})
	}
	sort.Slice(w, func(i, j int) bool {
		return w[i].Writer < w[j].Writer || w[i].Writer == w[j].Writer && w[i].Epoch < w[j].Epoch
	})
	return &w
}

func cursorFromWire(w wireCursor) Cursor {
	c := Cursor{}
	for _, e := range w {
		c[Log{e.Writer, e.Epoch}] = e.Seq
	}
	return c
}

// Start is where a new consumer starts.
type Start struct {
	kind   int
	timeMs uint64
	or     *Start
}

const (
	startBeginning = iota
	startEnd
	startTime
	startCommitted
)

func Beginning() Start { return Start{kind: startBeginning} }
func End() Start       { return Start{kind: startEnd} }

// AtTime starts at the first batch logged at or after timeMs (ms since the epoch).
func AtTime(timeMs uint64) Start { return Start{kind: startTime, timeMs: timeMs} }

// Committed starts at the group's committed position, or at `or` if it has none.
func Committed(or Start) Start { return Start{kind: startCommitted, or: &or} }

// conn is one multiplexed connection: many requests in flight, responses matched by req_id.
type conn struct {
	nc      net.Conn
	wmu     sync.Mutex
	mu      sync.Mutex
	pending map[uint64]chan frame
	nextID  uint64
	err     error
	done    chan struct{}
}

type frame struct {
	kind byte
	body []byte
}

func dial(ctx context.Context, addr, token string) (*conn, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &conn{nc: nc, pending: map[uint64]chan frame{}, nextID: 1, done: make(chan struct{})}
	go c.readLoop()
	body, _ := json.Marshal(hello{Token: token})
	// the node answers an accepted hello with an empty Ack and closes the connection otherwise
	if _, err := c.call(ctx, kindHello, body); err != nil {
		c.fail(err)
		return nil, fmt.Errorf("precog: hello to %s: %w", addr, err)
	}
	return c, nil
}

func (c *conn) readLoop() {
	r := bufio.NewReaderSize(c.nc, 64<<10)
	var hdr [13]byte
	for {
		if _, err := io.ReadFull(r, hdr[:4]); err != nil {
			c.fail(err)
			return
		}
		n := binary.LittleEndian.Uint32(hdr[:4])
		if n < 9 || n > maxFrameBytes {
			c.fail(fmt.Errorf("precog: bad frame length %d", n))
			return
		}
		if _, err := io.ReadFull(r, hdr[4:13]); err != nil {
			c.fail(err)
			return
		}
		body := make([]byte, n-9)
		if _, err := io.ReadFull(r, body); err != nil {
			c.fail(err)
			return
		}
		id := binary.LittleEndian.Uint64(hdr[5:13])
		c.mu.Lock()
		ch := c.pending[id]
		delete(c.pending, id)
		c.mu.Unlock()
		if ch != nil {
			ch <- frame{hdr[4], body}
		}
	}
}

func (c *conn) fail(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == nil {
		c.err = fmt.Errorf("precog: connection closed: %w", err)
		close(c.done)
		c.nc.Close()
	}
}

func (c *conn) call(ctx context.Context, kind byte, body []byte) (frame, error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return frame{}, c.err
	}
	id := c.nextID
	c.nextID++
	ch := make(chan frame, 1)
	c.pending[id] = ch
	c.mu.Unlock()
	forget := func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}
	c.wmu.Lock()
	if d, ok := ctx.Deadline(); ok {
		_ = c.nc.SetWriteDeadline(d)
	}
	_, err := c.nc.Write(encodeFrame(kind, id, body))
	c.wmu.Unlock()
	if err != nil {
		forget()
		c.fail(err)
		return frame{}, err
	}
	select {
	case f := <-ch:
		if f.kind == kindError {
			return f, fmt.Errorf("precog: remote error: %s", f.body)
		}
		return f, nil
	case <-c.done:
		return frame{}, c.err
	case <-ctx.Done():
		forget()
		return frame{}, ctx.Err()
	}
}

// Client talks to every node of one cluster; connections are opened on first use.
type Client struct {
	addrs []string
	token string
	links []link
}

type link struct {
	mu sync.Mutex
	c  *conn
}

// Connect makes a client for the nodes at `nodes` (tcp://host:port each). It dials nothing yet.
func Connect(nodes []string, token string) (*Client, error) {
	if len(nodes) == 0 {
		return nil, errors.New("precog: no nodes")
	}
	cl := &Client{token: token, links: make([]link, len(nodes))}
	for _, n := range nodes {
		a, ok := strings.CutPrefix(n, "tcp://")
		if _, _, err := net.SplitHostPort(a); !ok || err != nil {
			return nil, fmt.Errorf("precog: bad address (want tcp://host:port): %s", n)
		}
		cl.addrs = append(cl.addrs, a)
	}
	return cl, nil
}

// Close closes every connection; in-flight requests fail.
func (cl *Client) Close() error {
	for i := range cl.links {
		l := &cl.links[i]
		l.mu.Lock()
		if l.c != nil {
			l.c.fail(errors.New("client closed"))
			l.c = nil
		}
		l.mu.Unlock()
	}
	return nil
}

// preferred is the node a stream's clients try first.
func (cl *Client) preferred(stream string) int {
	return int(xxh3.HashString(stream) % uint64(len(cl.addrs)))
}

func (cl *Client) link(ctx context.Context, i int) (*conn, error) {
	l := &cl.links[i]
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.c != nil {
		select {
		case <-l.c.done:
		default:
			return l.c, nil
		}
	}
	c, err := dial(ctx, cl.addrs[i], cl.token)
	if err != nil {
		return nil, err
	}
	l.c = c
	return c, nil
}

// call sends one request to node i; on any failure it drops the link, so a hung connection
// is not reused.
func (cl *Client) call(ctx context.Context, i int, kind byte, body []byte) (frame, error) {
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	c, err := cl.link(ctx, i)
	if err != nil {
		return frame{}, err
	}
	f, err := c.call(ctx, kind, body)
	if err != nil {
		l := &cl.links[i]
		l.mu.Lock()
		if l.c == c {
			l.c = nil
			c.fail(err)
		}
		l.mu.Unlock()
	}
	return f, err
}

// control sends a control request to the first node that answers, from `start` round-robin.
func (cl *Client) control(ctx context.Context, start int, req control) (int, controlResp, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return 0, controlResp{}, err
	}
	n := len(cl.addrs)
	for i := range n {
		node := (start + i) % n
		var f frame
		if f, err = cl.call(ctx, node, kindControl, body); err == nil {
			var r controlResp
			if f.kind != kindControlResp {
				err = fmt.Errorf("precog: unexpected frame kind %d", f.kind)
			} else if err = json.Unmarshal(f.body, &r); err == nil {
				return node, r, nil
			}
		}
		if ctx.Err() != nil {
			break
		}
	}
	return 0, controlResp{}, fmt.Errorf("precog: no node answered: %w", err)
}

// append sends batch bytes, starting at `preferred` and failing over round-robin on transport
// errors, timeouts or Unknown outcomes until appendDeadline. Retrying the same bytes is safe.
func (cl *Client) append(ctx context.Context, preferred int, batch []byte, copies byte) error {
	ctx, cancel := context.WithTimeout(ctx, appendDeadline)
	defer cancel()
	body := encodeAppend(copies, batch)
	n := len(cl.addrs)
	node := preferred % n
	var last error
	for attempt := 1; ; attempt++ {
		f, err := cl.call(ctx, node, kindAppend, body)
		switch {
		case err != nil:
			last = err
		case f.kind != kindAck || len(f.body) != 10:
			last = fmt.Errorf("precog: bad ack (kind %d, %d bytes)", f.kind, len(f.body))
		case f.body[0] <= 1: // Ok, Duplicate
			return nil
		case f.body[0] == 2 || f.body[0] == 4: // Conflict, Rejected
			return fmt.Errorf("precog: append refused (status %d)", f.body[0])
		default: // Unknown: fewer copies than asked; retry
			last = fmt.Errorf("precog: append outcome unknown (status %d, %d copies)", f.body[0], f.body[1])
		}
		if ctx.Err() != nil {
			return fmt.Errorf("precog: append outcome unknown after %d attempts: %w", attempt, last)
		}
		node = (node + 1) % n
		if attempt%n == 0 {
			select {
			case <-time.After(50 * time.Millisecond):
			case <-ctx.Done():
			}
		}
	}
}

// Producer returns a writer log of `stream` named `writer`, with a new (time-based) epoch.
// A Producer is not safe for concurrent use.
func (cl *Client) Producer(stream, writer string) *Producer {
	return &Producer{
		cl:       cl,
		log:      logID{stream, writer, uint64(time.Now().UnixNano())},
		copies:   byte(min(len(cl.addrs), 2)),
		compress: true,
	}
}

// Consumer returns a reader of `stream` for `group`, starting at `start`.
func (cl *Client) Consumer(ctx context.Context, stream, group string, start Start) (*Consumer, error) {
	cur, err := cl.resolve(ctx, stream, group, start)
	if err != nil {
		return nil, err
	}
	c := cl.ConsumerAt(stream, cur)
	c.group = group
	return c, nil
}

// ConsumerAt returns a reader from a cursor the caller kept; it has no group to commit to.
func (cl *Client) ConsumerAt(stream string, cursor Cursor) *Consumer {
	c := &Consumer{cl: cl, stream: stream, cursor: Cursor{}, node: cl.preferred(stream)}
	for l, s := range cursor {
		c.cursor[l] = s
	}
	return c
}

func (cl *Client) resolve(ctx context.Context, stream, group string, s Start) (Cursor, error) {
	req := control{Op: "cursor_at", Stream: stream}
	switch s.kind {
	case startBeginning:
		return Cursor{}, nil
	case startEnd:
		t := ^uint64(0)
		req.TimeMs = &t
	case startTime:
		req.TimeMs = &s.timeMs
	case startCommitted:
		req = control{Op: "position", Group: group, Stream: stream}
	}
	_, r, err := cl.control(ctx, cl.preferred(stream), req)
	switch {
	case err != nil:
		return nil, err
	case r.Op == "cursor" && r.Cursor != nil:
		return cursorFromWire(*r.Cursor), nil
	case r.Op == "cursor" && s.kind == startCommitted:
		return cl.resolve(ctx, stream, group, *s.or)
	}
	return nil, fmt.Errorf("precog: unexpected %s response", r.Op)
}

// Latest is a compacted stream's newest value of every key.
func (cl *Client) Latest(ctx context.Context, stream string) ([]Entry, error) {
	_, r, err := cl.control(ctx, cl.preferred(stream), control{Op: "latest", Stream: stream})
	if err != nil {
		return nil, err
	}
	if r.Op != "latest" {
		return nil, fmt.Errorf("precog: unexpected %s response", r.Op)
	}
	return r.entries(), nil
}

// Producer appends events of one writer log in batches.
type Producer struct {
	cl       *Client
	log      logID
	nextSeq  uint64
	events   []pending
	bytes    int
	copies   byte
	compress bool
	inFlight []chan error // oldest first
}

// SetCompression turns payload compression (one lz4 block per batch, the default) on or off.
func (p *Producer) SetCompression(on bool) { p.compress = on }

// Send buffers an event with its dedup id (live-ingestion's redpanda-dedup-key; empty: never
// deduplicated), sent as its 8-byte hash. Flush sends it; flush when IsFull.
func (p *Producer) Send(timeUs uint64, dedupID, payload []byte) {
	p.Put(timeUs, DedupKey(dedupID), payload)
}

// Put buffers an event of a compacted stream: key is its compaction key, kept as is.
func (p *Producer) Put(timeUs uint64, key, payload []byte) {
	p.bytes += 14 + len(key) + len(payload)
	p.events = append(p.events, pending{timeUs, append([]byte(nil), key...), append([]byte(nil), payload...)})
}

// IsFull reports whether the buffered events should be flushed.
func (p *Producer) IsFull() bool {
	return len(p.events) >= MaxBatchEvents || p.bytes >= MaxBatchBytes
}

// InFlight is the number of batches sent and not yet acknowledged.
func (p *Producer) InFlight() int { return len(p.inFlight) }

// Flush sends the buffered events as one batch, keeping up to MaxInFlight batches waiting for
// their acks (it waits for the oldest beyond that); Sync waits for all of them. An append
// keeps retrying the same bytes for up to 30s whatever ctx does; ctx bounds only the wait.
func (p *Producer) Flush(ctx context.Context) error {
	if len(p.events) == 0 {
		return nil
	}
	for len(p.inFlight) >= MaxInFlight {
		if err := p.waitOldest(ctx); err != nil {
			return err
		}
	}
	b, err := encodeBatch(p.log, p.nextSeq, p.events, p.compress)
	if err != nil {
		return err
	}
	ch := make(chan error, 1)
	cl, node, copies := p.cl, p.cl.preferred(p.log.Stream), p.copies
	go func() { ch <- cl.append(context.Background(), node, b, copies) }()
	p.inFlight = append(p.inFlight, ch)
	p.nextSeq += uint64(len(p.events))
	p.events, p.bytes = nil, 0
	return nil
}

// Sync flushes and waits until every batch sent so far is acknowledged (durable on 2 nodes).
func (p *Producer) Sync(ctx context.Context) error {
	if err := p.Flush(ctx); err != nil {
		return err
	}
	for len(p.inFlight) > 0 {
		if err := p.waitOldest(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (p *Producer) waitOldest(ctx context.Context) error {
	select {
	case err := <-p.inFlight[0]:
		p.inFlight = p.inFlight[1:]
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Consumer reads one stream. It is not safe for concurrent use.
type Consumer struct {
	cl     *Client
	stream string
	group  string
	cursor Cursor
	node   int
}

// Poll returns the next events (about maxBytes of batches), per writer in sequence order;
// none when caught up. Fails over to the next node on an error.
func (c *Consumer) Poll(ctx context.Context, maxBytes uint32) ([]Event, error) {
	// a batch can hold no event for this reader (dedup dropped them all, or the cursor is past
	// them): read on, so an empty poll still means caught up
	for {
		evs, batches, err := c.poll(ctx, maxBytes)
		if err != nil || len(evs) > 0 || batches == 0 {
			return evs, err
		}
	}
}

// poll is one read: the events and the number of batches that came back.
func (c *Consumer) poll(ctx context.Context, maxBytes uint32) ([]Event, int, error) {
	body, err := json.Marshal(control{Op: "read_cursor", Stream: c.stream, Cursor: c.cursor.wire(), MaxBytes: &maxBytes})
	if err != nil {
		return nil, 0, err
	}
	n := len(c.cl.addrs)
	for i := range n {
		node := (c.node + i) % n
		var f frame
		if f, err = c.cl.call(ctx, node, kindControl, body); err == nil {
			if f.kind == kindBatches {
				c.node = node
				return c.take(f.body)
			}
			err = fmt.Errorf("precog: unexpected frame kind %d", f.kind)
		}
		if ctx.Err() != nil {
			break
		}
	}
	return nil, 0, fmt.Errorf("precog: no node answered: %w", err)
}

func (c *Consumer) take(body []byte) ([]Event, int, error) {
	var out []Event
	batches := 0
	for len(body) >= 4 {
		n := int(binary.LittleEndian.Uint32(body))
		if len(body)-4 < n {
			return nil, 0, errors.New("precog: truncated batches")
		}
		b, err := parseBatch(body[4 : 4+n])
		if err != nil {
			return nil, 0, err
		}
		batches++
		body = body[4+n:]
		l := Log{b.log.Writer, b.log.Epoch}
		from := c.cursor[l]
		for _, e := range b.events {
			if e.Seq >= from {
				out = append(out, e)
			}
		}
		c.cursor[l] = b.end
	}
	return out, batches, nil
}

// Cursor is where the consumer is: a position valid on every node (a copy).
func (c *Consumer) Cursor() Cursor {
	cur := Cursor{}
	for l, s := range c.cursor {
		cur[l] = s
	}
	return cur
}

// Commit stores the position as the group's (after the caller has made what it read durable).
func (c *Consumer) Commit(ctx context.Context) error {
	if c.group == "" {
		return errors.New("precog: a ConsumerAt has no group to commit to")
	}
	_, r, err := c.cl.control(ctx, c.node, control{Op: "commit", Group: c.group, Stream: c.stream, Cursor: c.cursor.wire()})
	if err != nil {
		return err
	}
	if r.Op != "committed" {
		return fmt.Errorf("precog: unexpected %s response", r.Op)
	}
	return nil
}
