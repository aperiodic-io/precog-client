package precog

// The Go client end to end against a real 3-node cluster of precog-node processes, as
// live-ingestion's producers and derived_seed use Redpanda today: two producer replicas send
// the same events with a dedup id; a reader resumes from a committed position on another
// node, or starts at a time; a compacted stream serves its newest values. Needs
// target/release/precog-node (or $PRECOG_NODE); skipped without it.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type cluster struct {
	addrs []string
	token string
	procs map[int]*exec.Cmd
}

func (c *cluster) kill(i int) {
	if p := c.procs[i]; p != nil {
		_ = p.Process.Kill()
		_ = p.Wait()
		delete(c.procs, i)
	}
}

func nodeBinary(t *testing.T) string {
	bin := os.Getenv("PRECOG_NODE")
	if _, err := os.Stat(bin); bin != "" && err != nil {
		t.Fatalf("PRECOG_NODE=%s: %v", bin, err) // CI sets it: never skip there
	}
	if bin == "" {
		bin = "../target/release/precog-node"
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("no precog-node at %s (set PRECOG_NODE: the binary is in ghcr.io/aperiodic-io/precog at /usr/local/bin/precog-node)", bin)
	}
	abs, _ := filepath.Abs(bin)
	return abs
}

// freeBase is a port p with p, p+1 and p+2 free (precog-node init numbers ports from a base).
func freeBase(t *testing.T) int {
	for range 50 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		base := l.Addr().(*net.TCPAddr).Port
		l.Close()
		ok := base < 65000
		for i := 1; ok && i < 3; i++ {
			l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+i))
			if ok = err == nil; ok {
				l.Close()
			}
		}
		if ok {
			return base
		}
	}
	t.Fatal("no three free ports")
	return 0
}

// startCluster writes configs for 3 nodes (stream "state" compacted) and starts nodes `up`;
// the others' addresses stay dead.
func startCluster(t *testing.T, up ...int) *cluster {
	bin := nodeBinary(t)
	dir := t.TempDir()
	base := freeBase(t)
	out, err := exec.Command(bin, "init", "--nodes", "3", "--dir", dir, "--base-port", fmt.Sprint(base),
		"--segment-mb", "1", "--rpc-timeout-ms", "300").CombinedOutput()
	if err != nil {
		t.Fatalf("init: %v %s", err, out)
	}
	var cf struct {
		Token string   `json:"token"`
		Nodes []string `json:"nodes"`
	}
	b, _ := os.ReadFile(filepath.Join(dir, "cluster.json"))
	if err := json.Unmarshal(b, &cf); err != nil {
		t.Fatal(err)
	}
	c := &cluster{addrs: cf.Nodes, token: cf.Token, procs: map[int]*exec.Cmd{}}
	t.Cleanup(func() {
		for i := range c.procs {
			c.kill(i)
		}
	})
	for _, i := range up {
		path := filepath.Join(dir, fmt.Sprintf("node-%d.json", i))
		var cfg map[string]any
		b, _ := os.ReadFile(path)
		if err := json.Unmarshal(b, &cfg); err != nil {
			t.Fatal(err)
		}
		cfg["streams"] = map[string]any{"state": map[string]any{"compacted": true}}
		b, _ = json.Marshal(cfg)
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
		log, _ := os.Create(filepath.Join(dir, fmt.Sprintf("n%d.log", i)))
		cmd := exec.Command(bin, "serve", "--config", path, "--threads", "2")
		cmd.Env = append(os.Environ(), "RUST_LOG=warn")
		cmd.Stdout, cmd.Stderr = log, log
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		c.procs[i] = cmd
	}
	for _, i := range up {
		addr := strings.TrimPrefix(c.addrs[i], "tcp://")
		deadline := time.Now().Add(10 * time.Second)
		for {
			conn, err := net.Dial("tcp", addr)
			if err == nil {
				conn.Close()
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d not listening: %v", i, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	return c
}

func connect(t *testing.T, c *cluster) *Client {
	cl, err := Connect(c.addrs, c.token)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	return cl
}

func drain(t *testing.T, ctx context.Context, r *Consumer, maxBytes uint32) []Event {
	var got []Event
	for {
		evs, err := r.Poll(ctx, maxBytes)
		if err != nil {
			t.Fatal(err)
		}
		if len(evs) == 0 {
			return got
		}
		got = append(got, evs...)
	}
}

func payloads(evs []Event) []string {
	var s []string
	for _, e := range evs {
		s = append(s, string(e.Payload))
	}
	return s
}

func numbered(n int) []string {
	var s []string
	for i := range n {
		s = append(s, fmt.Sprintf("t%d", i))
	}
	return s
}

func TestReplicasPublishTheSameEventsAndAReaderSeesEachOnceInWriterOrder(t *testing.T) {
	c := startCluster(t, 0, 1, 2)
	ctx := context.Background()
	cl := connect(t, c)
	a := cl.Producer("raw.trades.x", "producer-a")
	b := cl.Producer("raw.trades.x", "producer-b")
	b.SetCompression(false) // the broker takes both batch formats
	for i := range 50 {
		id := fmt.Sprintf("trade-%d", i)
		a.Send(uint64(i), []byte(id), []byte(fmt.Sprintf("t%d", i)))
		b.Send(uint64(i), []byte(id), []byte(fmt.Sprintf("t%d", i)))
		if i%10 == 9 {
			if err := a.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			if err := b.Sync(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	r, err := cl.Consumer(ctx, "raw.trades.x", "archiver", Beginning())
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, ctx, r, 4096)
	seen := map[string]bool{}
	for _, p := range payloads(got) {
		seen[p] = true
	}
	if len(seen) != 50 || len(got) != 50 {
		t.Fatalf("want every event once: %d distinct of %d: %v", len(seen), len(got), payloads(got))
	}
	for _, w := range []string{"producer-a", "producer-b"} {
		last := int64(-1)
		for _, e := range got {
			if e.Writer == w {
				if int64(e.Seq) <= last {
					t.Fatalf("%s out of order at seq %d", w, e.Seq)
				}
				last = int64(e.Seq)
			}
		}
	}
}

// Replica b's copy of a's event is a batch dedup emptied; a reader in small steps reaches it,
// and an empty poll there would read as caught up before b's next event.
func TestAPollIsEmptyOnlyWhenCaughtUpEvenPastABatchDedupEmptied(t *testing.T) {
	c := startCluster(t, 0, 1, 2)
	ctx := context.Background()
	cl := connect(t, c)
	a, b := cl.Producer("s", "a"), cl.Producer("s", "b")
	for _, s := range []struct {
		p       *Producer
		id, val string
	}{{a, "id0", "t0"}, {b, "id0", "t0"}, {b, "id1", "t1"}} {
		s.p.Send(1, []byte(s.id), []byte(s.val))
		if err := s.p.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	r, err := cl.Consumer(ctx, "s", "g", Beginning())
	if err != nil {
		t.Fatal(err)
	}
	if got := payloads(drain(t, ctx, r, 1)); fmt.Sprint(got) != "[t0 t1]" {
		t.Fatalf("got %v", got)
	}
}

func TestACommittedPositionResumesOnAnotherNode(t *testing.T) {
	c := startCluster(t, 1, 2) // node 0 is down: every client fails over
	ctx := context.Background()
	cl := connect(t, c)
	p := cl.Producer("raw.trades.x", "producer-a")
	for i := range 40 {
		p.Send(uint64(i), []byte(fmt.Sprintf("id%d", i)), []byte(fmt.Sprintf("t%d", i)))
		if i%5 == 4 {
			if err := p.Sync(ctx); err != nil { // batches of 5: the read unit
				t.Fatal(err)
			}
		}
	}
	r, err := cl.Consumer(ctx, "raw.trades.x", "archiver", Beginning())
	if err != nil {
		t.Fatal(err)
	}
	first, err := r.Poll(ctx, 300)
	if err != nil || len(first) == 0 || len(first) >= 40 {
		t.Fatalf("first poll: %d events, %v", len(first), err)
	}
	if err := r.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// the node that took the commit dies; a new archiver resumes through the other one
	// (the commit is acknowledged on 2 copies: both live nodes)
	c.kill(r.node)
	cl2 := connect(t, c)
	r2, err := cl2.Consumer(ctx, "raw.trades.x", "archiver", Committed(Beginning()))
	if err != nil {
		t.Fatal(err)
	}
	rest := drain(t, ctx, r2, 1<<20)
	all := append(payloads(first), payloads(rest)...)
	if fmt.Sprint(all) != fmt.Sprint(numbered(40)) {
		t.Fatalf("lost or repeated events: %v", all)
	}
	// brrrrr keeps its own cursor in its checkpoint and resumes from it
	r3 := cl2.ConsumerAt("raw.trades.x", r.Cursor())
	if got := payloads(drain(t, ctx, r3, 1<<20)); fmt.Sprint(got) != fmt.Sprint(payloads(rest)) {
		t.Fatalf("from a kept cursor: %v", got)
	}
}

func TestANewGroupStartsAtATime(t *testing.T) {
	c := startCluster(t, 0, 1, 2)
	ctx := context.Background()
	cl := connect(t, c)
	p := cl.Producer("raw.trades.x", "producer-a")
	p.Send(1, []byte("old"), []byte("old"))
	if err := p.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	since := uint64(time.Now().UnixMilli())
	p.Send(2, []byte("new"), []byte("new"))
	if err := p.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	r, err := cl.Consumer(ctx, "raw.trades.x", "seeded", Committed(AtTime(since)))
	if err != nil {
		t.Fatal(err)
	}
	if got := payloads(drain(t, ctx, r, 1<<20)); fmt.Sprint(got) != "[new]" {
		t.Fatalf("from %d: %v", since, got)
	}
}

// Latest is served by the Rust node from the batches it decompressed itself, so this is also
// the round trip of a Go-compressed block (with lz4 matches) through Rust.
func TestLatestOnACompactedStream(t *testing.T) {
	c := startCluster(t, 0, 1, 2)
	ctx := context.Background()
	cl := connect(t, c)
	s := cl.Producer("state", "collector")
	v2 := strings.Repeat("v2-", 100)
	s.Put(1, []byte("k1"), []byte("v1"))
	s.Put(2, []byte("k1"), []byte(v2))
	s.Put(3, []byte("k2"), []byte("w"))
	if err := s.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	es, err := cl.Latest(ctx, "state")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, e := range es {
		got[string(e.Key)] = string(e.Value)
	}
	if len(es) != 2 || got["k1"] != v2 || got["k2"] != "w" {
		t.Fatalf("latest: %v", got)
	}
}
