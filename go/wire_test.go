package precog

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/zeebo/xxh3"
)

// golden is testdata/golden.json, written by precog's crates/mesh/tests/go_fixtures.rs from the Rust
// encoders: the Go client must produce and accept exactly these bytes.
type golden struct {
	PCB2        string            `json:"pcb2"`
	PCZ2        string            `json:"pcz2"`
	PCZ2Matches string            `json:"pcz2_matches"`
	PCB2Dropped string            `json:"pcb2_dropped"`
	PCZ2Dropped string            `json:"pcz2_dropped"`
	AppendFrame string            `json:"append_frame"`
	Hello       string            `json:"hello"`
	Controls    map[string]string `json:"controls"`
	Responses   map[string]string `json:"responses"`
	XXH3        []struct {
		In   string `json:"in"`
		Hash string `json:"hash"`
	} `json:"xxh3"`
}

func loadGolden(t *testing.T) golden {
	t.Helper()
	b, err := os.ReadFile("testdata/golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var g golden
	if err := json.Unmarshal(b, &g); err != nil {
		t.Fatal(err)
	}
	return g
}

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The inputs go_fixtures.rs encodes.
var fixtureLog = logID{Stream: "raw.trades.x", Writer: "producer-a", Epoch: 1_700_000_000_123_456_789}

func smallEvents() []pending {
	return []pending{
		{time: 1_700_000_000_000_001, key: DedupKey([]byte("trade-1")), payload: []byte("t1")},
		{time: 2, key: nil, payload: nil},
		{time: 3, key: []byte("k"), payload: []byte("hello")},
	}
}

func matchEvents() []pending {
	rep := []byte(strings.Repeat("abc", 40))
	return []pending{
		{time: 10, key: []byte("a"), payload: rep},
		{time: 11, key: []byte("b"), payload: rep},
		{time: 12, key: []byte("c"), payload: rep},
	}
}

// Dedup ids and the preferred node hash with xxh3-64; a different hash than Rust's would
// send two replicas' copies of an event to different dedup indexes and to different nodes.
func TestXXH3MatchesRust(t *testing.T) {
	g := loadGolden(t)
	for _, v := range g.XXH3 {
		want, _ := strconv.ParseUint(v.Hash, 10, 64)
		if got := xxh3.Hash(unhex(t, v.In)); got != want {
			t.Errorf("xxh3(%s) = %d, Rust says %d", v.In, got, want)
		}
	}
	k := DedupKey([]byte("trade-1"))
	if len(k) != 8 || binary.LittleEndian.Uint64(k) != 14356838779693445574 {
		t.Errorf("DedupKey(trade-1) = %x", k)
	}
	if len(DedupKey(nil)) != 0 {
		t.Error("an empty id must stay empty: never deduplicated")
	}
}

// A batch's bytes are its identity: a retry must be byte-identical, and the broker parses
// exactly the Rust layout.
func TestBatchesAreByteIdenticalToRust(t *testing.T) {
	g := loadGolden(t)
	for _, c := range []struct {
		compress bool
		want     string
	}{{false, g.PCB2}, {true, g.PCZ2}} {
		got, err := encodeBatch(fixtureLog, 42, smallEvents(), c.compress)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, unhex(t, c.want)) {
			t.Errorf("compress=%v:\n got %x\nwant %s", c.compress, got, c.want)
		}
	}
}

// lz4 compressors may pick different matches, so a compressed block need not be
// byte-identical; the header and columns must be, and each side must read the other's block.
func TestCompressedBlocksInteroperate(t *testing.T) {
	g := loadGolden(t)
	rust := unhex(t, g.PCZ2Matches)
	ours, err := encodeBatch(fixtureLog, 0, matchEvents(), true)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range [][]byte{rust, ours} {
		p, err := parseBatch(b)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.events) != 3 {
			t.Fatalf("%d events", len(p.events))
		}
		for i, e := range p.events {
			if e.Seq != uint64(i) || e.EventTimeUs != uint64(10+i) || !bytes.Equal(e.Payload, matchEvents()[i].payload) {
				t.Errorf("event %d: %+v", i, e)
			}
		}
	}
	hdr := len(rust) - 4 - blockLen(t, rust)
	if !bytes.Equal(ours[:hdr], rust[:hdr]) {
		t.Errorf("header and columns differ:\n got %x\nwant %x", ours[:hdr], rust[:hdr])
	}
}

// blockLen is the PCZ2 lz4 block's length plus its u32 length prefix.
func blockLen(t *testing.T, b []byte) int {
	t.Helper()
	p, err := parseBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	return len(b) - 4 - p.blockOff + 4
}

// The broker drops duplicate events by rewriting the batch (a PCB2 tombstone, a PCZ2 dropped
// flag); a reader must skip them and keep the others' sequence numbers.
func TestDroppedEventsAreSkipped(t *testing.T) {
	g := loadGolden(t)
	for _, c := range []struct {
		hex  string
		seqs []uint64
	}{{g.PCB2Dropped, []uint64{42, 44}}, {g.PCZ2Dropped, []uint64{0, 2}}} {
		p, err := parseBatch(unhex(t, c.hex))
		if err != nil {
			t.Fatal(err)
		}
		if len(p.events) != 2 || p.events[0].Seq != c.seqs[0] || p.events[1].Seq != c.seqs[1] {
			t.Errorf("events %+v, want seqs %v", p.events, c.seqs)
		}
		if p.end != c.seqs[1]+1 {
			t.Errorf("end %d", p.end)
		}
	}
}

func TestCorruptBatchesAreRefused(t *testing.T) {
	b := unhex(t, loadGolden(t).PCB2)
	for i := range b {
		v := bytes.Clone(b)
		v[i] ^= 0x40
		if _, err := parseBatch(v); err == nil {
			t.Fatalf("flip at %d undetected", i)
		}
	}
}

func TestFramesAndControlJSONMatchRust(t *testing.T) {
	g := loadGolden(t)
	frame := encodeFrame(kindAppend, 7, encodeAppend(2, unhex(t, g.PCB2)))
	if !bytes.Equal(frame, unhex(t, g.AppendFrame)) {
		t.Errorf("append frame\n got %x\nwant %s", frame, g.AppendFrame)
	}
	cur := wireCursor{{"producer-a", 7, 3}, {"b", 1, 0}}
	u32 := func(v uint32) *uint32 { return &v }
	u64 := func(v uint64) *uint64 { return &v }
	for name, c := range map[string]any{
		"read_cursor":       control{Op: "read_cursor", Stream: "s", Cursor: &cur, MaxBytes: u32(1024)},
		"read_cursor_empty": control{Op: "read_cursor", Stream: "s", Cursor: &wireCursor{}, MaxBytes: u32(1)},
		"cursor_at":         control{Op: "cursor_at", Stream: "s", TimeMs: u64(^uint64(0))},
		"commit":            control{Op: "commit", Group: "g", Stream: "s", Cursor: &cur},
		"position":          control{Op: "position", Group: "g", Stream: "s"},
		"latest":            control{Op: "latest", Stream: "s"},
		"hello":             hello{Token: "tok"},
	} {
		b, _ := json.Marshal(c)
		want := g.Controls[name]
		if name == "hello" {
			want = g.Hello
		}
		if string(b) != want {
			t.Errorf("%s: got %s want %s", name, b, want)
		}
	}
	var r controlResp
	if err := json.Unmarshal([]byte(g.Responses["cursor"]), &r); err != nil || r.Op != "cursor" || r.Cursor == nil || len(*r.Cursor) != 2 || (*r.Cursor)[0] != cur[0] {
		t.Errorf("cursor: %+v %v", r, err)
	}
	r = controlResp{}
	if err := json.Unmarshal([]byte(g.Responses["no_cursor"]), &r); err != nil || r.Cursor != nil {
		t.Errorf("no_cursor: %+v %v", r, err)
	}
	r = controlResp{}
	if err := json.Unmarshal([]byte(g.Responses["committed"]), &r); err != nil || r.Op != "committed" {
		t.Errorf("committed: %+v %v", r, err)
	}
	r = controlResp{}
	if err := json.Unmarshal([]byte(g.Responses["latest"]), &r); err != nil {
		t.Fatal(err)
	}
	if es := r.entries(); len(es) != 1 || string(es[0].Key) != "k1" || !bytes.Equal(es[0].Value, []byte("v\xff")) {
		t.Errorf("latest: %+v", es)
	}
}
