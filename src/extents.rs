//! Context extents: an event stored as what it adds to what its writer log already holds
//! (docs/ARCHITECTURE.md, "Context extents"). Opt-in per stream (`Producer::set_extents`,
//! `Consumer::set_extents`); the broker stores the result as any events.
//!
//! A writer log keeps an *arena*: the bytes its events added, in order. An event is a *recipe*
//! (copies from the arena, copies from the log's previous event, short inline runs) and a
//! *literal*, the bytes it adds to the arena. An agent's request that repeats the conversation
//! so far costs what is new in it.
//!
//! An arena does not grow for ever: a *reset* event starts a new one and names nothing before
//! it. Every event says how far back its arena starts, so a reader that begins anywhere goes
//! back to that event, and one whose arena's start is gone (retention) passes over to the next
//! reset.
//!
//! One event's bytes (before the chunk tag of `Producer::set_chunked`, which this mode uses):
//!
//! ```text
//! VERSION (1 byte) | back | n_ops | op* | event_len | xxh3-64 of the event (8 bytes LE) | literal
//! op: 0 off len        copy from the arena (as it is after this event's literal)
//!     1 len bytes      inline
//!     3 off len        copy from the log's previous event
//! ```
//!
//! `back`, `n_ops`, `off`, `len` and `event_len` are LEB128 varints. `back` is the distance in
//! sequence numbers from this event's first chunk to the first chunk of its arena's reset
//! event (0: this event is the reset). Op 2 is kept for a tenant's shared blob and refused.

use crate::chunks::{Assembly, MAX_EVENT_BYTES};
use crate::proto::{Event, LogId};
use anyhow::{Context, Result, bail, ensure};
use std::collections::HashMap;
use xxhash_rust::xxh3::xxh3_64;

/// The first byte of every event of this mode. A change to the format above changes it, and a
/// reader refuses a version it does not know.
pub const VERSION: u8 = 1;
/// A run shorter than this that nothing matches stays in the recipe: a changed counter, the
/// closing brackets. A longer one joins the arena.
pub const INLINE_MAX: usize = 48;
/// A reader refuses an arena larger than this: a writer that never resets must not grow it.
pub const ARENA_HARD_MAX: usize = 1 << 30;
/// A reader of a stream keeps at most about this many bytes of arenas and previous events, over
/// all the stream's writer logs; past it the logs it read longest ago lose theirs, and go back
/// to their arena's start when they are next read (a log on its way back keeps its own until it
/// is there, so several going back at once exceed this).
pub const READER_BYTES: usize = 256 << 20;

/// An anchor is a window of this many bytes whose rolling hash hits a 1-in-64 pattern; where an
/// event's hash is an anchor's, the bytes are compared, and must be the arena's for at least
/// `MIN_MATCH` bytes back from there to be copied.
const WINDOW: usize = 64;
const MIN_MATCH: usize = 32;
const ANCHOR_MASK: u64 = 0x3f << 24;

/// When a producer starts a new arena. Both bound what a reader that starts in the middle reads
/// first, what a retention trim inside an arena makes unreadable (the rest of that arena's
/// events, per writer log), and what the two ends hold in memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Extents {
    /// A reset after this many events.
    pub reset_events: u32,
    /// A reset once the arena holds this many bytes (at most `ARENA_HARD_MAX / 2`).
    pub arena_bytes: usize,
}

impl Default for Extents {
    fn default() -> Extents {
        Extents {
            reset_events: 64,
            arena_bytes: 32 << 20,
        }
    }
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    // (ten bytes hold 64 bits: a loop that cannot run on, whatever is done to its test)
    for _ in 0..10 {
        if v < 0x80 {
            break;
        }
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// A varint at `b[*i..]`: at most 10 bytes, and no bits past the 64th.
fn get_varint(b: &[u8], i: &mut usize) -> Result<u64> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let c = *b.get(*i).context("a recipe ends inside a number")?;
        *i += 1;
        ensure!(shift < 63 || c <= 1, "a number over 64 bits");
        v |= ((c & 0x7f) as u64) << shift;
        if c < 0x80 {
            break;
        }
    }
    Ok(v)
}

fn get_len(b: &[u8], i: &mut usize) -> Result<usize> {
    Ok(usize::try_from(get_varint(b, i)?)?)
}

// ponytail: byte at a time (about a gigabyte a second); compare words if a profile of large
// events shows these two
fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

fn common_suffix(a: &[u8], b: &[u8]) -> usize {
    (a.iter().rev().zip(b.iter().rev()))
        .take_while(|(x, y)| x == y)
        .count()
}

const fn gear() -> [u64; 256] {
    let mut t = [0u64; 256];
    let (mut i, mut x) = (0, 0x9E37_79B9_7F4A_7C15u64);
    while i < 256 {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        t[i] = x.wrapping_mul(0x2545_F491_4F6C_DD1D);
        i += 1;
    }
    t
}
static GEAR: [u64; 256] = gear();

/// Rolls the gear hash over `d` from `h` and calls `f(hash, end)` where the hash at `end` hits
/// the anchor pattern; `f` may skip ahead (the hash then starts again). Returns the hash after
/// `d`. After `WINDOW` bytes the hash depends on the last `WINDOW` bytes only (each byte's
/// term is shifted out).
fn anchors_of(mut h: u64, d: &[u8], mut f: impl FnMut(u64, usize) -> Option<usize>) -> u64 {
    // (at most a step per byte: it ends whatever `f` answers and whatever is done to it)
    let mut i = 0;
    for _ in 0..d.len() {
        let Some(&b) = d.get(i) else { break };
        h = (h << 1).wrapping_add(GEAR[b as usize]);
        i += 1;
        if h & ANCHOR_MASK == 0
            && let Some(to) = f(h, i)
        {
            (i, h) = (to, 0);
        }
    }
    h
}

#[derive(Clone, Copy, Debug)]
enum Op {
    /// From the arena.
    Copy { off: usize, len: usize },
    /// From the previous event.
    Prev { off: usize, len: usize },
    /// `len` bytes at `at` of the event being encoded.
    Inline { at: usize, len: usize },
}

/// One writer log's encoder: its arena, its previous event, and where the arena's anchors are.
/// It holds about 1.4 times the arena (at most `Extents::arena_bytes` and one event's literal)
/// plus the previous event; `bytes` says how much.
#[derive(Debug)]
pub struct Encoder {
    cfg: Extents,
    arena: Vec<u8>,
    prev: Vec<u8>,
    /// An anchor window's hash, and where in the arena the window ends.
    anchors: HashMap<u64, u32>,
    /// The rolling hash at the arena's end: windows across two literals are anchors too.
    arena_h: u64,
    /// Events since the reset, and the reset's sequence number. `None`: the next event resets.
    since: Option<(u32, u64)>,
}

impl Encoder {
    pub fn new(cfg: Extents) -> Encoder {
        Encoder {
            cfg: Extents {
                arena_bytes: cfg.arena_bytes.min(ARENA_HARD_MAX / 2),
                ..cfg
            },
            arena: vec![],
            prev: vec![],
            anchors: HashMap::new(),
            arena_h: 0,
            since: None,
        }
    }

    /// Forgets the arena: the next event is a reset. For a stream that has gone idle.
    pub fn park(&mut self) {
        *self = Encoder::new(self.cfg);
    }

    /// The bytes this encoder holds, about.
    pub fn bytes(&self) -> usize {
        self.arena.capacity() + self.prev.capacity() + self.anchors.capacity() * 13
    }

    /// The event as it is stored. `seq`: the sequence number its first chunk gets; a log's
    /// events are encoded in sequence order, and every one encoded must be appended (the next
    /// one names its bytes).
    pub fn encode(&mut self, seq: u64, ev: &[u8]) -> Result<Vec<u8>> {
        ensure!(
            ev.len() <= MAX_EVENT_BYTES,
            "an event of {} bytes is over {MAX_EVENT_BYTES}",
            ev.len()
        );
        let (n, base) = match self.since {
            Some((n, base))
                if n < self.cfg.reset_events
                    && self.arena.len() < self.cfg.arena_bytes
                    && seq > base =>
            {
                (n, base)
            }
            _ => {
                self.park();
                (0, seq)
            }
        };
        self.since = Some((n + 1, base));
        // what the previous event has in place, then what the arena has anywhere
        let mut p = common_prefix(ev, &self.prev);
        let mut s = common_suffix(&ev[p..], &self.prev[p..]);
        if p + s < INLINE_MAX {
            (p, s) = (0, 0);
        }
        let mut found = vec![];
        if p > 0 {
            found.push(Op::Prev { off: 0, len: p });
        }
        self.matches(ev, p, ev.len() - s, &mut found);
        if s > 0 {
            let off = self.prev.len() - s;
            found.push(Op::Prev { off, len: s });
        }
        // long unmatched runs join the arena, where the next event finds them
        let mut literal = vec![];
        let mut out = vec![VERSION];
        put_varint(&mut out, seq - base);
        put_varint(&mut out, found.len() as u64);
        for op in found {
            let op = match op {
                Op::Inline { at, len } if len >= INLINE_MAX => {
                    let off = self.arena.len() + literal.len();
                    literal.extend_from_slice(&ev[at..at + len]);
                    Op::Copy { off, len }
                }
                op => op,
            };
            match op {
                Op::Copy { off, len } | Op::Prev { off, len } => {
                    out.push(if matches!(op, Op::Copy { .. }) { 0 } else { 3 });
                    put_varint(&mut out, off as u64);
                    put_varint(&mut out, len as u64);
                }
                Op::Inline { at, len } => {
                    out.push(1);
                    put_varint(&mut out, len as u64);
                    out.extend_from_slice(&ev[at..at + len]);
                }
            }
        }
        put_varint(&mut out, ev.len() as u64);
        out.extend_from_slice(&xxh3_64(ev).to_le_bytes());
        out.extend_from_slice(&literal);
        let (base, anchors) = (self.arena.len(), &mut self.anchors);
        self.arena_h = anchors_of(self.arena_h, &literal, |h, end| {
            if base + end >= WINDOW {
                anchors.insert(h, (base + end) as u32);
            }
            None
        });
        self.arena.extend_from_slice(&literal);
        self.prev.clear();
        self.prev.extend_from_slice(ev);
        Ok(out)
    }

    /// Ops for `ev[from..to]`, which the previous event does not have in place: what the arena
    /// holds is copied, the rest is inline.
    fn matches(&self, ev: &[u8], from: usize, to: usize, ops: &mut Vec<Op>) {
        // `lit`: where the unmatched run starts
        let mut lit = from;
        anchors_of(0, &ev[from..to], |h, end| {
            let e = from + end;
            let at = *self.anchors.get(&h)? as usize;
            let back = common_suffix(&ev[lit..e], &self.arena[..at]);
            if back < MIN_MATCH {
                return None; // other bytes with this hash
            }
            let fwd = common_prefix(&ev[e..to], &self.arena[at..]);
            if lit < e - back {
                let len = e - back - lit;
                ops.push(Op::Inline { at: lit, len });
            }
            let (off, len) = (at - back, back + fwd);
            ops.push(Op::Copy { off, len });
            lit = e + fwd;
            Some(lit - from)
        });
        if lit < to {
            let len = to - lit;
            ops.push(Op::Inline { at: lit, len });
        }
    }
}

/// How far back an event's arena starts, and the rest of the event (its recipe and literal).
pub fn header(payload: &[u8]) -> Result<(u64, &[u8])> {
    let version = *payload.first().context("an event without a version")?;
    ensure!(
        version == VERSION,
        "extents version {version}, this reader knows {VERSION}"
    );
    let mut i = 1;
    let back = get_varint(payload, &mut i)?;
    Ok((back, &payload[i..]))
}

/// One writer log's decoder: the arena and the previous event. After an error it is of no use:
/// the log is read again from a reset.
#[derive(Debug, Default)]
pub struct Decoder {
    arena: Vec<u8>,
    prev: Vec<u8>,
}

impl Decoder {
    /// The bytes this decoder holds.
    pub fn bytes(&self) -> usize {
        self.arena.capacity() + self.prev.capacity()
    }

    /// The event of a recipe and literal (`header`'s rest), which must be the log's next. The
    /// bytes are a writer's: any are decoded or refused. Nothing is allocated for what they
    /// only claim: a recipe is checked whole (every copy inside the arena or the previous
    /// event, the event at most `MAX_EVENT_BYTES`, the arena at most `ARENA_HARD_MAX`) before
    /// a byte is copied, and the event is yielded only if it hashes to what the recipe says.
    pub fn decode(&mut self, body: &[u8]) -> Result<Vec<u8>> {
        let mut i = 0;
        let n = get_varint(body, &mut i)?;
        // (tag, offset, length); an inline run's offset is into `body`. Every op takes at
        // least two of the recipe's bytes, so the list is bounded by the recipe
        let (mut ops, mut total) = (vec![], 0usize);
        for _ in 0..n {
            let tag = *body.get(i).context("a recipe ends inside an op")?;
            i += 1;
            let (off, len) = match tag {
                0 | 3 => (get_len(body, &mut i)?, get_len(body, &mut i)?),
                1 => {
                    let len = get_len(body, &mut i)?;
                    ensure!(len <= body.len() - i, "an inline run past the recipe");
                    i += len;
                    (i - len, len)
                }
                _ => bail!("recipe op {tag}"),
            };
            ensure!(len > 0, "an empty op");
            total = total
                .checked_add(len)
                .filter(|&t| t <= MAX_EVENT_BYTES)
                .with_context(|| format!("an event over {MAX_EVENT_BYTES} bytes"))?;
            ops.push((tag, off, len));
        }
        ensure!(get_len(body, &mut i)? == total, "an event's length");
        let hash = body.get(i..i + 8).context("a recipe without its hash")?;
        let literal = &body[i + 8..];
        let arena = self.arena.len() + literal.len();
        ensure!(arena <= ARENA_HARD_MAX, "an arena over {ARENA_HARD_MAX}");
        for &(tag, off, len) in &ops {
            let within = match tag {
                0 => arena,
                3 => self.prev.len(),
                _ => continue,
            };
            ensure!(
                off.checked_add(len).is_some_and(|end| end <= within),
                "a copy of {len} bytes at {off} of {within}"
            );
        }
        self.arena.extend_from_slice(literal);
        let mut out = Vec::with_capacity(total);
        for (tag, off, len) in ops {
            let src = match tag {
                0 => &self.arena,
                3 => &self.prev,
                _ => body,
            };
            out.extend_from_slice(&src[off..off + len]);
        }
        ensure!(xxh3_64(&out).to_le_bytes() == hash, "an event's hash");
        self.prev.clone_from(&out);
        Ok(out)
    }
}

/// A node rebuilds at most this many stored bytes for one event (`rebuild`): an arena larger
/// than this is read with the SDK, which keeps it.
pub const REBUILD_BYTES: usize = 64 << 20;

/// Where the arena of the event that starts at `seq` starts, from the event as it is stored
/// (its first chunk). `None`: `seq` is not the start of an event.
pub fn arena_start(first_chunk: &Event) -> Result<Option<u64>> {
    let Some((&tag, rest)) = first_chunk.payload.split_first() else {
        bail!("an event without a chunk tag");
    };
    if tag != crate::chunks::WHOLE && tag != crate::chunks::FIRST {
        return Ok(None);
    }
    let back = header(rest)?.0;
    let base = first_chunk.seq.checked_sub(back);
    Ok(Some(base.context("an arena that starts before its log")?))
}

/// The event that starts at sequence `seq`, rebuilt from its writer log's stored events from
/// `base` on (where its arena starts: `arena_start`), in the order given. `None`: the events
/// are not all there (one is missing between `base` and the event's last chunk, or the first
/// is not a reset): spec/extents/ExtentsRead.tla, `Whole`. An error: the events are there and
/// are not an extents log's, or are over `REBUILD_BYTES`.
pub fn rebuild(
    events: impl IntoIterator<Item = Event>,
    base: u64,
    seq: u64,
) -> Result<Option<Vec<u8>>> {
    let (mut asm, mut dec, mut next, mut read) = (Assembly::default(), None, base, 0usize);
    for e in events {
        if e.seq != next {
            return Ok(None);
        }
        (next, read) = (e.seq + 1, read + e.payload.len());
        ensure!(read <= REBUILD_BYTES, "an arena over {REBUILD_BYTES} bytes");
        let Some(e) = asm.push(e)? else { continue };
        let (back, body) = header(&e.payload)?;
        if e.seq == base {
            dec = Some(Decoder::default());
        }
        // (the arena is `base`'s: an event that says otherwise is not of it, and the event at
        // `base` that is not a reset says otherwise)
        let Some(dec) = dec.as_mut().filter(|_| e.seq - base == back) else {
            return Ok(None);
        };
        let event = dec.decode(body)?;
        if e.seq >= seq {
            // (past it: `seq` is inside an event, not the start of one)
            return Ok((e.seq == seq).then_some(event));
        }
    }
    Ok(None)
}

/// What a reader does with one event of a writer log.
#[derive(Debug, PartialEq)]
enum Step {
    /// Nothing to yield: a chunk of an event not yet whole, an event rebuilt on the way back to
    /// where the reader stood, or one passed over.
    None,
    Event(Event),
    /// The reader has no arena for this event: it reads the log again from this sequence
    /// number (where the arena starts), and drops what else it holds of the log.
    Rewind(u64),
}

/// A reader's state for one writer log.
#[derive(Default)]
struct LogReader {
    asm: Assembly,
    /// The decoder, and the sequence number its next event starts at.
    dec: Option<(Decoder, u64)>,
    /// The arena of the events being read cannot be had: they are passed over to a reset.
    lost: bool,
    /// The lowest sequence number a rewind went back to since the last reset that was read.
    tried: Option<u64>,
    /// Events that start below this were yielded, or passed over, before a rewind.
    from: u64,
    /// Where a reader that stops now resumes.
    resume: u64,
    used: u64,
}

impl LogReader {
    fn bytes(&self) -> usize {
        self.dec.as_ref().map_or(0, |d| d.0.bytes())
    }

    fn push(&mut self, e: Event, passed: &mut u64) -> Result<Step> {
        let end = e.seq + 1;
        let step = self.step(e, passed);
        if !matches!(step, Ok(Step::Rewind(_))) {
            // no later than the start of an event whose chunks it holds, and never back
            self.resume = self.resume.max(self.asm.boundary().unwrap_or(end));
        }
        step
    }

    fn step(&mut self, e: Event, passed: &mut u64) -> Result<Step> {
        let end = e.seq + 1;
        let Some(e) = self.asm.push(e)? else {
            return Ok(Step::None);
        };
        let lose = |r: &mut LogReader| {
            (r.dec, r.lost) = (None, true);
        };
        let (back, body) = header(&e.payload).inspect_err(|_| lose(self))?;
        if back == 0 {
            (self.dec, self.lost, self.tried) = (Some((Decoder::default(), e.seq)), false, None);
        } else if self.lost {
            *passed += 1;
            return Ok(Step::None);
        }
        let Some((dec, next)) = self.dec.as_mut().filter(|d| d.1 == e.seq) else {
            // no arena (the reader started here, or dropped it), or events are missing
            // between its last and this one. Missing ones do not come back; an arena is read
            // again from its start, once: if this is the read from there, the start is gone
            let base = e.seq.checked_sub(back);
            let again = (self.dec.is_none() && base.is_some())
                .then_some(base)
                .flatten()
                .filter(|&b| self.tried.is_none_or(|t| b < t));
            lose(self);
            let Some(base) = again else {
                *passed += 1;
                ensure!(base.is_some(), "an arena that starts before its log");
                return Ok(Step::None);
            };
            (self.lost, self.tried, self.asm) = (false, Some(base), Assembly::default());
            self.from = self.from.max(e.seq);
            self.resume = self.resume.max(self.from);
            return Ok(Step::Rewind(base));
        };
        match dec.decode(body) {
            Ok(payload) => {
                *next = end;
                Ok(match e.seq >= self.from {
                    true => Step::Event(Event {
                        payload: payload.into(),
                        ..e
                    }),
                    false => Step::None,
                })
            }
            Err(err) => {
                lose(self);
                Err(err.context(format!(
                    "{}/{} seq {}: its events are passed over to the next reset",
                    e.stream, e.writer, e.seq
                )))
            }
        }
    }
}

/// A reader's state for a stream: every writer log's, within a budget of memory.
pub struct Readers {
    logs: HashMap<LogId, LogReader>,
    budget: usize,
    held: usize,
    tick: u64,
    /// Events passed over: their arena's start was gone, or one before them was missing or
    /// damaged.
    pub passed_over: u64,
    refused: Option<anyhow::Error>,
}

impl Default for Readers {
    fn default() -> Readers {
        Readers::with_budget(READER_BYTES)
    }
}

impl Readers {
    /// Readers that keep about `budget` bytes (the log being read keeps its own whatever it
    /// is).
    pub fn with_budget(budget: usize) -> Readers {
        Readers {
            logs: HashMap::new(),
            budget,
            held: 0,
            tick: 0,
            passed_over: 0,
            refused: None,
        }
    }

    /// The next event of `log` as the broker serves it, in sequence order. An error leaves the
    /// reader at the log's next reset.
    fn push(&mut self, log: &LogId, e: Event) -> Result<Step> {
        self.tick += 1;
        let r = self.logs.entry(log.clone()).or_default();
        let before = r.bytes();
        r.used = self.tick;
        let step = r.push(e, &mut self.passed_over);
        self.held = self.held - before + r.bytes();
        // ponytail: a scan of the stream's logs per eviction; keep them in use order if a
        // reader of thousands of live logs spends its time here
        while self.held > self.budget {
            let idle = (self.logs.iter_mut())
                // (not one on its way back to where it stood: it would never get there)
                .filter(|(l, r)| *l != log && r.dec.as_ref().is_some_and(|d| d.1 > r.from))
                .min_by_key(|(_, r)| r.used);
            let Some((_, r)) = idle else { break };
            self.held -= r.bytes();
            r.dec = None;
        }
        step
    }

    /// One batch's events of `log` past the reader's cursor: the whole events they complete
    /// go to `out`, and their bytes come off `room`. `Some(seq)`: the reader reads the log
    /// again from `seq`, and the rest of the batch is dropped: it has to go back for an
    /// arena, or `room` is used up (0) and `seq` is the event after the last it yielded. The
    /// first error is kept for `take_error`.
    pub fn batch(
        &mut self,
        log: &LogId,
        events: impl IntoIterator<Item = Event>,
        out: &mut Vec<Event>,
        room: &mut usize,
    ) -> Option<u64> {
        for e in events {
            let next = e.seq + 1;
            match self.push(log, e) {
                Ok(Step::None) => {}
                Ok(Step::Event(e)) => {
                    *room = room.saturating_sub(e.payload.len());
                    out.push(e);
                    if *room == 0 {
                        return Some(next);
                    }
                }
                Ok(Step::Rewind(to)) => return Some(to),
                Err(e) => self.refused = self.refused.take().or(Some(e)),
            }
        }
        None
    }

    pub fn take_error(&mut self) -> Option<anyhow::Error> {
        self.refused.take()
    }

    /// Where a reader that stops now resumes reading `log`: after the last event it yielded
    /// or passed over, and no later than the start of an event it holds chunks of.
    pub fn resume(&self, log: &LogId) -> Option<u64> {
        self.logs.get(log).map(|r| r.resume)
    }
}

/// Sessions as agents write them, for the tests: each event is one of two conversations so far
/// (a header and its messages), which the steps extend, edit near the top, cut in the middle or
/// replace.
#[cfg(test)]
mod sessions {
    use proptest::prelude::*;

    #[derive(Clone, Debug)]
    pub enum Turn {
        /// the conversation gets a message
        Say(Vec<u8>),
        /// a byte of its header changes
        Edit(u8),
        /// its oldest message is dropped
        Forget,
        /// the other conversation goes on in this stream
        Switch,
        /// an event that shares nothing
        Other(Vec<u8>),
        /// the same request again
        Again,
        /// the producer lets go of its arena
        Park,
    }

    pub fn turns(n: usize) -> impl Strategy<Value = Vec<Turn>> {
        let bytes = |n| prop::collection::vec(any::<u8>(), 0..n);
        prop::collection::vec(
            prop_oneof![
                8 => bytes(260).prop_map(Turn::Say),
                1 => any::<u8>().prop_map(Turn::Edit),
                1 => Just(Turn::Forget),
                2 => Just(Turn::Switch),
                1 => bytes(400).prop_map(Turn::Other),
                1 => Just(Turn::Again),
                1 => Just(Turn::Park),
            ],
            0..n,
        )
    }

    /// The events of `turns` (`None`: a park before the next one).
    pub fn events(turns: &[Turn]) -> Vec<Option<Vec<u8>>> {
        let mut convs = [(vec![b'a'; 70], vec![]), (vec![b'b'; 90], vec![])];
        let (mut at, mut last) = (0, vec![]);
        let mut out = vec![];
        for t in turns {
            let (head, msgs): &mut (Vec<u8>, Vec<Vec<u8>>) = &mut convs[at];
            match t {
                Turn::Say(m) => msgs.push(m.clone()),
                Turn::Edit(b) => head[3] = *b,
                Turn::Forget => drop((!msgs.is_empty()).then(|| msgs.remove(0))),
                Turn::Switch => at = 1 - at,
                Turn::Other(_) | Turn::Again => {}
                Turn::Park => {
                    out.push(None);
                    continue;
                }
            }
            let (head, msgs) = &convs[at];
            last = match t {
                Turn::Other(b) => b.clone(),
                Turn::Again => last,
                _ => [&head[..], &msgs.concat()[..], b"]}"].concat(),
            };
            out.push(Some(last.clone()));
        }
        out
    }

    pub fn limits() -> impl Strategy<Value = super::Extents> {
        (1u32..7, 0usize..3000).prop_map(|(reset_events, arena_bytes)| super::Extents {
            reset_events,
            arena_bytes,
        })
    }
}

/// The codec as properties: whatever a session's events and the producer's limits, they come
/// back byte for byte; and whatever bytes a reader is handed, it rebuilds an event or refuses,
/// and accepts only a recipe that a decoder spelled out the slow way accepts.
#[cfg(test)]
mod props {
    use super::sessions::*;
    use super::*;
    use proptest::prelude::*;

    /// Encodes `events` as one writer log's, a sequence number each: what is stored.
    fn stored(cfg: Extents, events: &[Option<Vec<u8>>]) -> Vec<Vec<u8>> {
        let mut enc = Encoder::new(cfg);
        let mut out = vec![];
        for e in events {
            match e {
                Some(e) => out.push(enc.encode(out.len() as u64, e).unwrap()),
                None => enc.park(),
            }
        }
        out
    }

    /// A decoder that has read `stored`.
    fn replay(stored: &[Vec<u8>]) -> Decoder {
        let mut dec = Decoder::default();
        for s in stored {
            let (back, body) = header(s).unwrap();
            if back == 0 {
                dec = Decoder::default();
            }
            dec.decode(body).unwrap();
        }
        dec
    }

    proptest! {
        /// Why: the mode is only of use if every event comes back as it was sent, whatever
        /// the session did (grew, changed near the top, dropped old output, interleaved,
        /// repeated) and wherever the producer's limits made it start a new arena; and the
        /// limits must hold: an arena starts at least every `reset_events` events and stays
        /// within `arena_bytes` and one event.
        #[test]
        fn any_session_round_trips_within_its_limits(turns in turns(40), cfg in limits()) {
            let events = events(&turns);
            let sent: Vec<&Vec<u8>> = events.iter().flatten().collect();
            let mut enc = Encoder::new(cfg);
            let (mut dec, mut seq) = (Decoder::default(), 0u64);
            for e in &events {
                let Some(e) = e else {
                    enc.park();
                    continue;
                };
                let s = enc.encode(seq, e).unwrap();
                let (back, body) = header(&s).unwrap();
                prop_assert!(back < cfg.reset_events as u64 && back <= seq);
                if back == 0 {
                    dec = Decoder::default();
                }
                prop_assert_eq!(&dec.decode(body).unwrap(), e);
                let most = cfg.arena_bytes + e.len();
                prop_assert!(dec.arena.len() <= most, "{} of {}", dec.arena.len(), most);
                prop_assert_eq!(&dec.arena, &enc.arena);
                prop_assert!(enc.bytes() >= enc.arena.len() + e.len());
                prop_assert_eq!(dec.bytes(), dec.arena.capacity() + dec.prev.capacity());
                seq += 1;
            }
            prop_assert_eq!(seq as usize, sent.len());
        }

        /// Why: the reader's bytes are a writer's, over a network. Whatever they are, an event
        /// is rebuilt or refused, never a panic, and a refusal or an event is all there is.
        #[test]
        fn any_bytes_are_decoded_or_refused(
            turns in turns(6), bytes in prop::collection::vec(any::<u8>(), 0..200), cut: prop::sample::Index,
        ) {
            let mut dec = replay(&stored(Extents::default(), &events(&turns)));
            let _ = header(&bytes).map(|(_, body)| dec.decode(body));
            let _ = dec.decode(&bytes[cut.index(bytes.len() + 1)..]);
        }

        /// Why: bytes that pass the syntax reach the checks that matter: a copy may name only
        /// bytes the arena or the previous event holds. Any well-formed recipe is accepted
        /// exactly when a decoder spelled out the slow way accepts it, with the same event.
        #[test]
        fn a_recipe_is_accepted_only_if_every_copy_is_inside_what_the_log_holds(
            arena in prop::collection::vec(any::<u8>(), 0..60),
            prev in prop::collection::vec(any::<u8>(), 0..60),
            ops in prop::collection::vec(
                (prop_oneof![Just(0u8), Just(1), Just(3), 0u8..6], 0u64..80, 0u64..40, any::<bool>()), 0..6),
            literal in prop::collection::vec(any::<u8>(), 0..30),
            lie in prop_oneof![4 => Just(0u64), 1 => 1u64..3],
        ) {
            let mut dec = Decoder { arena: arena.clone(), prev: prev.clone() };
            let after = [&arena[..], &literal[..]].concat();
            // the slow way: each op's bytes, or None where it names what is not there
            let (mut body, mut want) = (vec![], Some(vec![]));
            put_varint(&mut body, ops.len() as u64);
            for &(tag, off, len, far) in &ops {
                // (now and then an offset no arena reaches)
                let off = if far { off << 57 } else { off };
                body.push(tag);
                let src = match tag {
                    0 => &after,
                    3 => &prev,
                    _ => &literal, // inline: any bytes
                };
                if tag == 1 {
                    let run = &src[..(len as usize).min(src.len())];
                    put_varint(&mut body, run.len() as u64);
                    body.extend_from_slice(run);
                    want = want.filter(|_| !run.is_empty()).map(|w| [&w[..], run].concat());
                    continue;
                }
                put_varint(&mut body, off);
                put_varint(&mut body, len);
                let run = usize::try_from(off).ok()
                    .and_then(|o| src.get(o..o.checked_add(len as usize)?))
                    .filter(|r| !r.is_empty() && (tag == 0 || tag == 3));
                want = want.zip(run).map(|(w, r)| [&w[..], r].concat());
            }
            let built = want.clone().unwrap_or_default();
            put_varint(&mut body, built.len() as u64 + lie);
            body.extend_from_slice(&xxh3_64(&built).to_le_bytes());
            body.extend_from_slice(&literal);
            let got = dec.decode(&body);
            match want.filter(|_| lie == 0) {
                Some(w) => {
                    prop_assert_eq!(got.unwrap(), w.clone());
                    prop_assert_eq!((dec.arena, dec.prev), (after, w));
                }
                None => prop_assert!(got.is_err(), "{:?}", got),
            }
        }

        /// Why: the event's hash is what stands between a damaged or mismatched event and a
        /// reader's caller. Any one byte changed, or the end cut off, and the event is refused
        /// (or is still the same event: a number written another way).
        #[test]
        fn a_damaged_event_is_refused(
            turns in turns(8), at: prop::sample::Index, flip in 1u8..=255, cut: bool,
        ) {
            let events = events(&turns);
            let sent: Vec<&Vec<u8>> = events.iter().flatten().collect();
            let stored = stored(Extents::default(), &events);
            let Some((last, before)) = stored.split_last() else { return Ok(()) };
            let mut dec = match header(last).unwrap().0 {
                0 => Decoder::default(),
                _ => replay(before),
            };
            let at = at.index(last.len());
            let mut bad = last.clone();
            match cut {
                true => bad.truncate(at),
                false => bad[at] ^= flip,
            }
            if let Ok(e) = header(&bad).and_then(|(_, body)| dec.decode(body)) {
                prop_assert_eq!(&e, *sent.last().unwrap());
            }
        }

        /// Why: every number of the format is a varint: each must come back as written, and a
        /// reader must not take bytes that encode no 64-bit number for one.
        #[test]
        fn a_number_round_trips_and_one_over_64_bits_is_refused(v: u64, more in 0u8..3) {
            let mut b = vec![];
            put_varint(&mut b, v);
            let mut i = 0;
            prop_assert_eq!(get_varint(&b, &mut i).unwrap(), v);
            prop_assert_eq!(i, b.len());
            prop_assert!(get_varint(&b[..b.len() - 1], &mut 0).is_err(), "cut short");
            // 9 full bytes, then a 10th that says more than the 64th bit, or goes on
            let mut long = vec![0xff; 9];
            long.push([2, 0x81, 0x7f][more as usize]);
            long.push(0);
            prop_assert!(get_varint(&long, &mut 0).is_err());
        }
    }

    fn session(calls: usize) -> Vec<Vec<u8>> {
        // a conversation of `calls` requests, each the one before and 300 new bytes
        let mut x = 7u64;
        let mut rand = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        };
        let mut conv = b"{\"system\":\"you are an agent\",\"messages\":[".to_vec();
        (0..calls)
            .map(|_| {
                conv.extend((0..300).map(|_| rand()));
                [&conv[..], b"]}"].concat()
            })
            .collect()
    }

    #[test]
    fn a_conversation_that_grows_is_stored_about_once() {
        // Why: this is what the mode is for. 40 requests of a conversation that grows by 300
        // bytes hold 250 KB; what is new in them is 12 KB. Stored: that, a recipe per event,
        // and the conversation again at each reset.
        let events = session(40);
        let logical: usize = events.iter().map(|e| e.len()).sum();
        let stored = |reset_events| {
            let mut enc = Encoder::new(Extents {
                reset_events,
                ..Default::default()
            });
            let stored = (events.iter().enumerate()).map(|(i, e)| enc.encode(i as u64, e).unwrap());
            stored.map(|s| s.len()).sum::<usize>()
        };
        assert_eq!(logical, 247_720);
        assert!(stored(64) < 12_000 + 40 * 30, "{}", stored(64));
        // a reset every 8 events stores the conversation so far 5 times
        assert!((30_000..45_000).contains(&stored(8)), "{}", stored(8));
        // an event that shares nothing costs itself and under 20 bytes
        let mut enc = Encoder::new(Extents::default());
        assert!(enc.encode(0, &events[0]).unwrap().len() < events[0].len() + 20);
    }

    #[test]
    fn a_recipe_s_claims_cost_nothing_before_they_are_checked() {
        // Why: a reader must not allocate what the bytes merely claim. A recipe of a few
        // bytes that claims 2^60 ops, a copy of 2^62 bytes, an event of 2^40 bytes, or more
        // of an arena than a reader holds, is refused, and the decoder holds what it held.
        let mut dec = Decoder::default();
        dec.decode(
            header(
                &Encoder::new(Extents::default())
                    .encode(0, &[7; 100])
                    .unwrap(),
            )
            .unwrap()
            .1,
        )
        .unwrap();
        let held = dec.bytes();
        let recipe = |parts: &[u64], tail: &[u8]| {
            let mut b = vec![];
            parts.iter().for_each(|&v| put_varint(&mut b, v));
            [&b[..], tail].concat()
        };
        let hash = [0u8; 8];
        for (bad, why) in [
            (recipe(&[1 << 60, 0, 0, 100], &[]), "ops it does not have"),
            (
                recipe(&[1, 0, 1 << 62, 1 << 62, 1 << 63], &hash),
                "a copy past the arena",
            ),
            (
                recipe(&[1, 0, 0, 1 << 40, 1 << 40], &hash),
                "an event over the bound",
            ),
            (
                recipe(&[1, 0, 0, usize::MAX as u64, 0], &hash),
                "a length that overflows",
            ),
            (
                recipe(&[1, 3, 50, 51, 51], &hash),
                "a copy past the previous event",
            ),
            (recipe(&[1, 1, 9], &[1, 2]), "an inline run past the recipe"),
            (recipe(&[1, 0, 0, 0, 0], &hash), "an empty op"),
            (
                recipe(&[1, 2, 0, 1, 1], &hash),
                "a shared blob, which this version has not",
            ),
            (recipe(&[1, 0, 0, 100, 100], &[0; 7]), "no hash"),
            (
                recipe(&[1, 0, 0, 100, 99], &hash),
                "a length that is not its ops'",
            ),
        ] {
            assert!(dec.decode(&bad).is_err(), "{why}");
            assert_eq!(dec.bytes(), held, "{why}");
        }
        // 300 copies of the arena are an event over the bound only when the arena is large;
        // here they are a legitimate 30 KB event, and its hash is what refuses this one
        let mut many = vec![];
        put_varint(&mut many, 300);
        (0..300).for_each(|_| many.extend([0, 0, 100]));
        put_varint(&mut many, 30_000);
        assert!(dec.decode(&[&many[..], &hash].concat()).is_err());
        // an arena past the reader's bound is refused before it is kept (the pages of this
        // one are never touched)
        let mut dec = Decoder {
            arena: vec![0; ARENA_HARD_MAX],
            prev: vec![],
        };
        let empty = xxh3_64(&[]).to_le_bytes();
        assert!(dec.decode(&recipe(&[0, 0], &empty)).unwrap().is_empty());
        let one_more = [recipe(&[0, 0], &empty), vec![0]].concat();
        assert!(dec.decode(&one_more).is_err() && dec.arena.len() == ARENA_HARD_MAX);
        assert!(header(&[]).is_err() && header(&[VERSION + 1, 0]).is_err());
        assert!(header(&[VERSION]).is_err(), "no distance back");
    }

    #[test]
    fn an_arena_has_an_anchor_about_every_64_bytes() {
        // Why: the index is what an open session costs in memory beside its arena, and what
        // finds a message that moved. One window in 64 is an anchor: more would grow the
        // index to the arena's own size, fewer would miss short messages.
        let mut enc = Encoder::new(Extents::default());
        let events = session(200);
        enc.encode(0, events.last().unwrap()).unwrap();
        let (arena, anchors) = (enc.arena.len(), enc.anchors.len());
        assert!(arena > 60_000, "{arena}");
        assert!(
            (arena / 96..arena / 48).contains(&anchors),
            "{anchors} in {arena}"
        );
        assert!(
            enc.bytes() >= arena + arena + anchors * 12,
            "the arena, the event, the index"
        );
        assert!(gear() == GEAR, "the table, built again at run time");
        // which windows are anchors is pinned: a change to the hash or its table changes what
        // every open session costs, and should be a decision
        let mut ends = vec![];
        let h = anchors_of(0, &events[20], |_, end| {
            ends.extend((end as u32).to_le_bytes());
            None
        });
        assert_eq!(
            (ends.len() / 4, xxh3_64(&ends), h),
            (94, 8773833970214974281, 8422712523066560196)
        );
        // and a message that moved (nothing before it in place) is found by them
        let moved = [
            b"a new header, then the old conversation: ",
            &events[150][41..],
        ]
        .concat();
        assert!(enc.encode(1, &moved).unwrap().len() < 200);
    }

    /// The tags of a stored event's ops, and its literal's length.
    fn ops_of(stored: &[u8]) -> (Vec<u8>, usize) {
        let body = header(stored).unwrap().1;
        let (mut i, mut tags) = (0, vec![]);
        for _ in 0..get_varint(body, &mut i).unwrap() {
            tags.push(body[i]);
            i += 1;
            let a = get_len(body, &mut i).unwrap();
            match tags.last() {
                Some(1) => i += a,
                _ => drop(get_varint(body, &mut i)),
            }
        }
        get_varint(body, &mut i).unwrap();
        (tags, body.len() - i - 8)
    }

    /// `n` bytes that repeat nothing.
    fn noise(seed: u8, n: usize) -> Vec<u8> {
        let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ seed as u64;
        (0..n)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    }

    #[test]
    fn what_an_event_shares_and_adds_goes_where_the_format_says() {
        // Why: the thresholds decide what a session costs. Bytes the previous event has in
        // place are named as that from 48 bytes on (below, naming them costs more than it
        // saves and pins the reader to the previous event for nothing); a new run of 48 bytes
        // or more joins the arena, where later events find it, and a shorter one stays in the
        // recipe; an arena starts again once it holds `arena_bytes`.
        let first = noise(1, 300);
        let stored_first = Encoder::new(Extents::default()).encode(0, &first).unwrap();
        let second = |edit: std::ops::Range<usize>| {
            let mut enc = Encoder::new(Extents::default());
            enc.encode(0, &first).unwrap();
            let mut e = first.clone();
            e[edit].iter_mut().for_each(|b| *b ^= 0xff);
            let stored = enc.encode(1, &e).unwrap();
            let mut dec = replay(std::slice::from_ref(&stored_first));
            assert_eq!(dec.decode(header(&stored).unwrap().1).unwrap(), e);
            ops_of(&stored)
        };
        assert_eq!(ops_of(&stored_first), (vec![0], 300), "all of it is new");
        // 10 bytes changed in the middle: the two sides from the previous event, 10 inline
        assert_eq!(second(100..110), (vec![3, 1, 3], 0));
        assert_eq!(
            second(100..147),
            (vec![3, 1, 3], 0),
            "47 new bytes stay inline"
        );
        assert_eq!(second(100..148), (vec![3, 0, 3], 48), "48 join the arena");
        // all but the first 48 bytes changed: those are worth naming; 47 are not
        assert_eq!(second(48..300), (vec![3, 0], 252));
        assert_eq!(second(47..300), (vec![0], 300));
        // an earlier request again, after another one (two conversations in one stream):
        // nothing of it is in place in the previous event, all of it is in the arena
        let other = noise(4, 300);
        let mut enc = Encoder::new(Extents::default());
        let stored: Vec<Vec<u8>> = ([&first, &other, &first].iter().enumerate())
            .map(|(i, e)| enc.encode(i as u64, e).unwrap())
            .collect();
        assert_eq!(
            ops_of(&stored[2]),
            (vec![0], 0),
            "one copy, from the arena's start"
        );
        assert_eq!(replay(&stored).prev, first);
        // a request that keeps the previous one's start, then has new bytes, an old request,
        // new bytes and another old one: each old one is one copy, each new run joins the arena
        let (a, b, start) = (noise(5, 400), noise(12, 150), noise(7, 100));
        let before = [&start[..], &noise(8, 300)].concat();
        let mixed = [&start[..], &noise(9, 60), &a, &noise(10, 60), &b].concat();
        let mut enc = Encoder::new(Extents::default());
        let stored: Vec<Vec<u8>> = ([&a, &b, &before, &mixed].iter().enumerate())
            .map(|(i, e)| enc.encode(i as u64, e).unwrap())
            .collect();
        assert_eq!(ops_of(&stored[3]), (vec![3, 0, 0, 0, 0], 120));
        assert_eq!(replay(&stored).prev, mixed);
        // the arena's bound: at it, the next event starts a new one
        let back_of_second = |arena_bytes| {
            let mut enc = Encoder::new(Extents {
                reset_events: 9,
                arena_bytes,
            });
            enc.encode(0, &first).unwrap();
            header(&enc.encode(1, &first).unwrap()).unwrap().0
        };
        assert_eq!((back_of_second(300), back_of_second(301)), (0, 1));
    }

    #[test]
    fn other_bytes_with_an_anchor_s_hash_are_not_copied() {
        // Why: the index is by hash, and a hash is not the bytes. Where an event's window has
        // an anchor's hash, what is copied is what compares equal, and nothing if that is
        // less than 32 bytes back: never a copy of bytes the arena does not hold, never an
        // empty one (a reader refuses those).
        let event = noise(2, 400);
        let mut hit = None;
        anchors_of(0, &event, |h, end| {
            hit = hit.or((end >= 100).then_some((h, end)));
            None
        });
        let (h, end) = hit.unwrap();
        for (same, copied) in [(0, false), (31, false), (32, true), (60, true)] {
            // an arena that ends with the event's `same` bytes before the window's end
            let arena = [&noise(3, 200)[..], &event[end - same..end]].concat();
            let mut enc = Encoder::new(Extents::default());
            let first = enc.encode(0, &arena).unwrap();
            enc.anchors.insert(h, arena.len() as u32);
            let stored = enc.encode(1, &event).unwrap();
            let mut dec = replay(&[first]);
            assert_eq!(dec.decode(header(&stored).unwrap().1).unwrap(), event);
            let want = match copied {
                true => (vec![0, 0, 0], 400 - same),
                false => (vec![0], 400),
            };
            assert_eq!(ops_of(&stored), want, "{same} bytes the same");
        }
    }

    #[test]
    fn an_event_over_the_readers_bound_is_not_encoded_and_a_sequence_that_goes_back_resets() {
        // Why: a reader refuses an event over `MAX_EVENT_BYTES`, so a producer must not store
        // one; and an encoder handed a sequence number at or below its arena's start (a caller's
        // mistake) must start a new arena rather than write a distance back that is wrong.
        let mut enc = Encoder::new(Extents::default());
        assert!(enc.encode(0, &vec![0; MAX_EVENT_BYTES + 1]).is_err());
        assert_eq!(enc.bytes(), 0, "and nothing is kept of it");
        let e = [9u8; 200];
        let back = |enc: &mut Encoder, seq| header(&enc.encode(seq, &e).unwrap()).unwrap().0;
        assert_eq!(
            [back(&mut enc, 5), back(&mut enc, 9), back(&mut enc, 5)],
            [0, 4, 0]
        );
        // (the same number again is a new arena too: a reader starts one at distance 0)
        let again = enc.encode(5, &e).unwrap();
        assert_eq!(replay(&[again]).prev, e);
        // the arena's bound is at most half the readers' hard one
        let huge = Extents {
            reset_events: 1,
            arena_bytes: usize::MAX,
        };
        assert_eq!(Encoder::new(huge).cfg.arena_bytes, ARENA_HARD_MAX / 2);
    }
}

/// Model-based test of the reader (`Readers`, with the chunk reassembly under it) against a
/// reference: two writer logs of sessions, stored as chunks of any size, read from any position
/// in batches of any size, with a retention trim anywhere in each log, with little or much
/// memory, stopping anywhere and resuming from the position it would commit. The reference is
/// the rule spelled out: a reader yields exactly the events that start at or after its position
/// and whose arena starts at or after the trim, each once, whole, in order.
#[cfg(test)]
mod model {
    use super::sessions::*;
    use super::*;
    use proptest::prelude::*;

    /// One writer log as the broker holds it: its events (chunks), and per event sent where it
    /// starts, where its arena starts, and its bytes.
    struct Log {
        id: LogId,
        chunks: Vec<Event>,
        sent: Vec<(u64, u64, Vec<u8>)>,
    }

    fn log(writer: &str, cfg: Extents, turns: &[Turn], max: usize) -> Log {
        let id = LogId {
            stream: "s".into(),
            writer: writer.into(),
            epoch: 1,
        };
        let (mut enc, mut chunks, mut sent) = (Encoder::new(cfg), vec![], vec![]);
        for e in events(turns) {
            let Some(e) = e else {
                enc.park();
                continue;
            };
            let first = chunks.len() as u64;
            let stored = enc.encode(first, &e).unwrap();
            sent.push((first, first - header(&stored).unwrap().0, e));
            for payload in crate::chunks::chunks(&stored, max) {
                chunks.push(Event {
                    stream: id.stream.clone(),
                    writer: id.writer.clone(),
                    epoch: id.epoch,
                    seq: chunks.len() as u64,
                    event_time_us: 0,
                    payload: payload.into(),
                });
            }
        }
        Log { id, chunks, sent }
    }

    /// What the broker serves of a log: from the trim on, without the events in `missing`.
    struct View<'a> {
        log: &'a Log,
        trim: u64,
        missing: &'a [bool],
    }

    impl View<'_> {
        fn from(&self, cursor: u64, n: usize) -> Vec<Event> {
            let gone = |seq: u64| self.missing.get(seq as usize).copied().unwrap_or(false);
            (self.log.chunks.iter())
                .filter(|e| e.seq >= cursor.max(self.trim) && !gone(e.seq))
                .take(n)
                .cloned()
                .collect()
        }
    }

    /// A consumer's loop (`Consumer::take`): reads the logs from `at` in batches of `n` events,
    /// for at most `rounds` rounds, or to the end. Returns what it yielded per log and the
    /// position it would commit. A batch's events are yielded until they come to `room` bytes
    /// (the bytes a poll rebuilds at most): the log is read on from there.
    fn read(
        views: &[View],
        at: &[u64],
        n: usize,
        budget: usize,
        rounds: usize,
        room: usize,
    ) -> (Vec<Vec<Vec<u8>>>, Vec<u64>) {
        let mut readers = Readers::with_budget(budget);
        let (mut cursor, mut resume) = (at.to_vec(), at.to_vec());
        let mut got = vec![vec![]; views.len()];
        for round in 0.. {
            assert!(round < 20_000, "a reader that goes back for ever");
            let mut read = false;
            for (i, v) in views.iter().enumerate() {
                let batch = v.from(cursor[i], n);
                let Some(last) = batch.last() else { continue };
                if round >= rounds {
                    return (got, resume);
                }
                read = true;
                cursor[i] = last.seq + 1;
                let (mut out, mut left) = (vec![], room);
                let back = readers.batch(&v.log.id, batch, &mut out, &mut left);
                if let Some(to) = back {
                    // back for an arena, or on from the cut: the event after the last yielded
                    assert!(to <= cursor[i] && (left == 0 || to < cursor[i]));
                    assert!(left > 0 || out.last().is_some_and(|e| e.seq < to));
                    cursor[i] = to;
                }
                if back.is_none() || left == 0 {
                    resume[i] = resume[i].max(readers.resume(&v.log.id).unwrap());
                }
                got[i].extend(out.into_iter().map(|e| e.payload.to_vec()));
                readers
                    .take_error()
                    .map_or(Ok(()), Err)
                    .expect("events as they were stored");
            }
            if !read {
                break;
            }
        }
        (got, resume)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(192))]

        /// Why: a reader may start anywhere (a committed position, a time, after a reload), the
        /// broker may have trimmed any prefix of a log, a consumer's memory is bounded, and it
        /// may stop anywhere and resume. Through all of it, it must yield exactly the events
        /// the rule says, and the position it commits must lose and repeat nothing.
        #[test]
        fn a_reader_from_anywhere_yields_exactly_the_events_whose_arena_is_there(
            a in turns(24), b in turns(12), cfg in limits(), max in prop_oneof![3usize..40, Just(1 << 20)],
            start in any::<[prop::sample::Index; 2]>(), trim in any::<[prop::sample::Index; 2]>(),
            n in 1usize..9, small: bool, stop in 0usize..40,
            room in prop_oneof![1usize..3000, Just(usize::MAX)],
        ) {
            let logs = [log("a", cfg, &a, max), log("b", cfg, &b, max)];
            let at = |ix: &[prop::sample::Index; 2], i: usize| ix[i].index(logs[i].chunks.len() + 1) as u64;
            let (start, trim) = ([at(&start, 0), at(&start, 1)], [at(&trim, 0), at(&trim, 1)]);
            let views = [0, 1].map(|i| View { log: &logs[i], trim: trim[i], missing: &[] });
            let budget = if small { 0 } else { usize::MAX };
            let want = |i: usize, from: u64| -> Vec<Vec<u8>> {
                (logs[i].sent.iter())
                    .filter(|(first, base, _)| *first >= from && *base >= trim[i])
                    .map(|x| x.2.clone())
                    .collect()
            };
            let (got, end) = read(&views, &start, n, budget, usize::MAX, room);
            for i in 0..2 {
                prop_assert_eq!(&got[i], &want(i, start[i]), "log {}", i);
                // (a log trimmed whole serves nothing: the reader stays where it started)
                let len = logs[i].chunks.len() as u64;
                let end_of = if trim[i] < len { len.max(start[i]) } else { start[i] };
                prop_assert_eq!(end[i], end_of, "all read");
            }
            // a reader that stops, and another from the position it would commit
            let (first, resume) = read(&views, &start, n, budget, stop, room);
            let (rest, _) = read(&views, &resume, n, budget, usize::MAX, room);
            for i in 0..2 {
                prop_assert!(resume[i] >= start[i]);
                let both = [&first[i][..], &rest[i][..]].concat();
                prop_assert_eq!(&both, &want(i, start[i]), "log {} resumed at {}", i, resume[i]);
            }
        }

        /// Why: the broker does not drop events of this mode, but a reader does not rest on
        /// that: whatever events are missing from what it is served (a broker that
        /// deduplicated or compacted, a writer that broke the rules), it yields only events
        /// that were sent, whole, in order, each at most once, and it comes to an end.
        #[test]
        fn whatever_events_are_missing_a_reader_yields_only_events_that_were_sent(
            a in turns(24), cfg in limits(), max in prop_oneof![3usize..40, Just(1 << 20)],
            missing in prop::collection::vec(prop::bool::weighted(0.1), 0..200),
            start: prop::sample::Index, n in 1usize..9, small: bool,
        ) {
            let log = log("a", cfg, &a, max);
            let start = start.index(log.chunks.len() + 1) as u64;
            let view = View { log: &log, trim: 0, missing: &missing };
            let budget = if small { 0 } else { usize::MAX };
            // (errors are expected here: a hole inside an event's chunks is a refusal)
            let mut readers = Readers::with_budget(budget);
            let (mut cursor, mut got) = (start, vec![]);
            for round in 0.. {
                prop_assert!(round < 20_000, "a reader that goes back for ever");
                let batch = view.from(cursor, n);
                let Some(last) = batch.last() else { break };
                cursor = last.seq + 1;
                let mut out = vec![];
                if let Some(to) = readers.batch(&log.id, batch, &mut out, &mut { usize::MAX }) {
                    cursor = to;
                }
                got.extend(out.into_iter().map(|e| (e.seq, e.payload.to_vec())));
            }
            let mut sent = log.sent.iter();
            for (seq, e) in &got {
                prop_assert!(*seq >= start);
                prop_assert!(sent.any(|x| x.0 == *seq && x.2 == *e), "not sent, or out of order: {}", seq);
            }
        }
    }

    proptest! {
        /// Why: a node rebuilds one event for a reader that holds no arena, from what the
        /// node holds: retention may have trimmed any prefix of the log, and a node that is
        /// behind lacks any of its events. It must answer with the event exactly when it
        /// holds every stored event from the arena's start to the event's last chunk, and
        /// with nothing otherwise: never other bytes, and never an error for what is only
        /// not there (spec/extents/ExtentsRead.tla, `Whole`).
        #[test]
        fn a_node_rebuilds_an_event_exactly_when_it_holds_its_whole_arena(
            a in turns(24), cfg in limits(), max in prop_oneof![3usize..40, Just(1 << 20)],
            trim: prop::sample::Index, missing in prop::collection::vec(prop::bool::weighted(0.04), 0..200),
            pick: prop::sample::Index, inside: prop::sample::Index,
        ) {
            let log = log("a", cfg, &a, max);
            if log.sent.is_empty() {
                return Ok(());
            }
            let trim = trim.index(log.chunks.len() + 1) as u64;
            let view = View { log: &log, trim, missing: &missing };
            let at = pick.index(log.sent.len());
            let (first, base, event) = &log.sent[at];
            let end = log.sent.get(at + 1).map_or(log.chunks.len() as u64, |next| next.0);
            let held = |seq: u64| seq >= trim && !missing.get(seq as usize).copied().unwrap_or(false);
            // the stored event says where its arena starts
            let stored = &log.chunks[*first as usize];
            prop_assert_eq!(arena_start(stored).unwrap(), Some(*base));
            let got = rebuild(view.from(*base, usize::MAX), *base, *first).unwrap();
            match (*base..end).all(held) {
                true => prop_assert_eq!(got.as_ref(), Some(event)),
                false => prop_assert_eq!(got, None),
            }
            // a sequence number inside an event is not an event's
            let seq = *first + 1 + inside.index((end - first) as usize) as u64;
            if seq < end {
                prop_assert_eq!(arena_start(&log.chunks[seq as usize]).unwrap(), None);
                let all = View { log: &log, trim: 0, missing: &[] };
                prop_assert_eq!(rebuild(all.from(*base, usize::MAX), *base, seq).unwrap(), None);
            }
        }
    }

    #[test]
    fn a_node_refuses_to_rebuild_what_is_not_an_extents_log_or_is_too_large() {
        // Why: a node rebuilds from a writer's bytes, on a reader's request. An arena over
        // its bound is refused before it is held; events that are not of the mode are an
        // error (the reader asked for the wrong stream), not a missing event; and an arena
        // that is not the one the reader's event names is not used.
        let cfg = Extents {
            reset_events: 3,
            arena_bytes: 1 << 20,
        };
        let turns: Vec<Turn> = (0..5).map(|i| Turn::Say(vec![i; 100])).collect();
        let l = log("a", cfg, &turns, 1 << 20); // arenas start at 0 and 3
        let event = |seq: u64, payload: Vec<u8>| Event {
            seq,
            payload: payload.into(),
            ..l.chunks[0].clone()
        };
        let huge = event(0, vec![0; REBUILD_BYTES + 1]);
        let e = rebuild([huge], 0, 0).unwrap_err().to_string();
        assert!(e.contains("an arena over"), "{e}");
        assert!(
            rebuild([event(0, vec![0, 9, 9])], 0, 0).is_err(),
            "another version"
        );
        assert!(arena_start(&event(0, vec![])).is_err(), "no chunk tag");
        assert!(
            arena_start(&event(2, vec![0, VERSION, 3])).is_err(),
            "before its log"
        );
        // event 4's arena starts at 3: from there it is rebuilt; from another arena's start
        // (0), from an event that is not a reset (1) or from itself (4) it is not
        let from = |base: usize| rebuild(l.chunks[base..].to_vec(), base as u64, 4).unwrap();
        assert_eq!(from(3).as_ref(), Some(&l.sent[4].2));
        assert_eq!((from(0), from(1), from(4)), (None, None, None));
        assert_eq!(
            rebuild(l.chunks.clone(), 0, 9).unwrap(),
            None,
            "past the log's end"
        );
    }

    #[test]
    fn a_reader_past_its_budget_lets_go_of_the_log_it_read_longest_ago() {
        // Why: a consumer of a stream with many writer logs (every producer is one) must not
        // hold every arena it ever read. Past its budget it drops the arena of the log it
        // read longest ago, and only as many as it must; that log goes back to its arena's
        // start when it is read again, and loses nothing.
        let cfg = Extents {
            reset_events: 100,
            arena_bytes: 1 << 20,
        };
        let turns: Vec<Turn> = (0..6).map(|i| Turn::Say(vec![i; 200])).collect();
        let logs = ["a", "b", "c"].map(|w| log(w, cfg, &turns, 1 << 20));
        let one = |r: &mut Readers, i: usize, seq: usize| {
            let mut out = vec![];
            let back = r.batch(&logs[i].id, [logs[i].chunks[seq].clone()], &mut out, &mut {
                usize::MAX
            });
            (back, out.len())
        };
        // (several times: which log goes must not rest on a map's order)
        for _ in 0..8 {
            // what two logs hold after three events each is the budget, exactly
            let mut r = Readers::default();
            for seq in 0..3 {
                assert_eq!(
                    (one(&mut r, 0, seq), one(&mut r, 1, seq)),
                    ((None, 1), (None, 1))
                );
            }
            let two = r.held;
            assert!(two > 2000, "{two}");
            let mut r = Readers::with_budget(two);
            for seq in 0..3 {
                assert_eq!(
                    (one(&mut r, 0, seq), one(&mut r, 1, seq)),
                    ((None, 1), (None, 1))
                );
            }
            assert_eq!(
                r.held, two,
                "at the budget, not over it: nothing is dropped"
            );
            // a third log: over it. The log read longest ago is `a`; `b` keeps its arena
            assert_eq!(one(&mut r, 2, 0), (None, 1));
            assert!(r.held <= two && r.held > two / 2, "{} of {two}", r.held);
            assert_eq!(one(&mut r, 1, 3), (None, 1), "b reads on");
            assert_eq!(
                one(&mut r, 0, 3),
                (Some(0), 0),
                "a goes back to its arena's start"
            );
            // on its way back it keeps its arena whatever the budget, until it is where it stood
            for seq in 0..3 {
                assert_eq!(one(&mut r, 0, seq), (None, 0), "rebuilt, not yielded again");
                assert_eq!(one(&mut r, 2, seq + 1), (None, 1));
            }
            assert_eq!(
                one(&mut r, 0, 3),
                (None, 1),
                "and yields the event it went back for"
            );
        }
    }

    #[test]
    fn a_reader_goes_back_once_passes_over_what_has_no_start_and_reports_damage() {
        // Why: the cases the model reaches now and then, each time, with what the caller sees.
        let cfg = Extents {
            reset_events: 3,
            arena_bytes: 1 << 20,
        };
        let turns: Vec<Turn> = (0..7).map(|i| Turn::Say(vec![i; 100])).collect();
        let l = log("a", cfg, &turns, 1 << 20); // arenas start at 0, 3 and 6
        let view = |trim| View {
            log: &l,
            trim,
            missing: &[],
        };
        let sent = |from: usize| {
            l.sent[from..]
                .iter()
                .map(|x| x.2.clone())
                .collect::<Vec<_>>()
        };
        // from event 5: back to 3, then 5 and 6; with the log trimmed to 4: only 6, and the
        // position a reader would commit is past the events it passed over
        assert_eq!(
            read(&[view(0)], &[5], 2, usize::MAX, usize::MAX, usize::MAX).0[0],
            sent(5)
        );
        let mut readers = Readers::default();
        let mut out = vec![];
        assert_eq!(
            readers.batch(&l.id, view(4).from(5, 9), &mut out, &mut { usize::MAX }),
            Some(3)
        );
        assert_eq!(
            readers.resume(&l.id),
            Some(5),
            "at the event it went back for"
        );
        assert_eq!(
            readers.batch(&l.id, view(4).from(3, 9), &mut out, &mut { usize::MAX }),
            None
        );
        assert_eq!((out.len(), &out[0].payload[..]), (1, &l.sent[6].2[..]));
        assert_eq!(
            (readers.passed_over, readers.resume(&l.id)),
            (2, Some(7)),
            "4 and 5"
        );
        assert!(readers.take_error().is_none(), "retention is not an error");
        // a damaged event is an error, once, and the reader goes on at the next reset
        let mut damaged = view(0).from(0, 9);
        let p = damaged[1].payload.to_vec();
        damaged[1].payload = [&p[..p.len() - 1], &[p[p.len() - 1] ^ 1]].concat().into();
        let (mut readers, mut out) = (Readers::default(), vec![]);
        assert_eq!(
            readers.batch(&l.id, damaged, &mut out, &mut { usize::MAX }),
            None
        );
        let got: Vec<u64> = out.iter().map(|e| e.seq).collect();
        assert_eq!(got, [0, 3, 4, 5, 6]);
        let e = readers.take_error().unwrap().to_string();
        assert!(
            e.contains("s/a seq 1") && readers.take_error().is_none(),
            "{e}"
        );
        assert_eq!(readers.passed_over, 1, "event 2");
        // a reader keeps the arenas of the logs it reads, in its default budget: two logs
        // read in turn do not go back
        let other = log("b", cfg, &turns, 1 << 20);
        let (mut readers, mut out) = (Readers::default(), vec![]);
        for e in l.chunks.iter().zip(&other.chunks) {
            assert_eq!(
                readers.batch(&l.id, [e.0.clone()], &mut out, &mut { usize::MAX }),
                None
            );
            assert_eq!(
                readers.batch(&other.id, [e.1.clone()], &mut out, &mut { usize::MAX }),
                None
            );
        }
        assert_eq!((out.len(), readers.held > 1000), (14, true));
        // a log that claims ever earlier arenas (no producer writes one): the reader goes
        // back only to a start earlier than the one it went back to, so it comes to an end
        let claims = |seq: u64, back: u8| Event {
            seq,
            payload: vec![0, VERSION, back].into(), // a whole event, its distance back
            ..l.chunks[0].clone()
        };
        let (mut readers, mut out) = (Readers::default(), vec![]);
        assert_eq!(
            readers.batch(&l.id, [claims(9, 4)], &mut out, &mut { usize::MAX }),
            Some(5)
        );
        assert_eq!(
            readers.batch(&l.id, [claims(5, 3)], &mut out, &mut { usize::MAX }),
            Some(2),
            "earlier: once more"
        );
        assert_eq!(
            readers.batch(&l.id, [claims(2, 0)], &mut out, &mut { usize::MAX }),
            None,
            "a start, and not a recipe"
        );
        assert!(readers.take_error().is_some());
        let (mut readers, mut out) = (Readers::default(), vec![]);
        assert_eq!(
            readers.batch(&l.id, [claims(9, 4)], &mut out, &mut { usize::MAX }),
            Some(5)
        );
        assert_eq!(
            readers.batch(&l.id, [claims(6, 1), claims(7, 1)], &mut out, &mut {
                usize::MAX
            }),
            None,
            "not earlier"
        );
        assert_eq!((readers.passed_over, out.len()), (2, 0));
        // an event of another mode (no version this reader knows) is an error too
        let mut other = view(0).from(0, 1);
        other[0].payload = vec![0, 9, 9].into(); // chunk tag, then not a version
        assert_eq!(
            readers.batch(&l.id, other, &mut out, &mut { usize::MAX }),
            None
        );
        assert!(
            readers
                .take_error()
                .unwrap()
                .to_string()
                .contains("version 9")
        );
        // an arena that claims to start before its log
        let mut early = view(0).from(4, 1);
        early[0].seq = 0;
        let (mut readers, mut out) = (Readers::default(), vec![]);
        assert_eq!(
            readers.batch(&l.id, early, &mut out, &mut { usize::MAX }),
            None
        );
        assert!(readers.take_error().is_some() && out.is_empty());
    }
}
