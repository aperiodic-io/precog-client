//! Wire and on-disk formats.
//!
//! A *batch* is the unit of append, replication, storage and repair. It is
//! immutable once created by a writer and is identified by
//! `(stream, writer, epoch, first_seq)`; it covers sequences
//! `first_seq .. first_seq + count`. The same bytes are sent by the client,
//! replicated to peers and written to every node's WAL, so no node re-encodes
//! anything on the hot path.
//!
//! ```text
//! batch := magic "PCB1" | u16 stream_len | stream | u16 writer_len | writer
//!        | u64 epoch | u64 first_seq | u32 count
//!        | count * ( u64 event_time_us | u32 payload_len | payload )
//!        | u32 crc32c(everything before)
//! ```
//!
//! Event time is carried per event and is *not* used for identity,
//! deduplication or durability. Readers sort by `(event_time_us, writer,
//! epoch, seq)`.

use anyhow::{Context, Result, bail, ensure};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

/// Batch format 2: every event has an optional dedup key, and a dropped event is a tombstone.
pub const BATCH_MAGIC: &[u8; 4] = b"PCB2";
/// Format 1 (no keys) still parses.
const BATCH_MAGIC_V1: &[u8; 4] = b"PCB1";
/// A payload length that marks a dropped event (no key, no payload).
const TOMBSTONE: u32 = u32::MAX;
/// Format 2 with every payload in one lz4 block after the event columns (keys stay outside it,
/// so the broker deduplicates without decompressing): header as PCB2, raw length u32, then
/// (time u64, key len u16, payload len u32, key) per event, block len u32, block, CRC-32C.
pub const BATCH_MAGIC_Z: &[u8; 4] = b"PCZ2";
/// In PCZ2, a payload length with this bit set is a dropped event: its bytes stay in the block
/// (dropping never recompresses), its key is gone, readers skip it.
const DROPPED: u32 = 1 << 31;
/// An event as `walk` yields it: (seq, time, key, payload).
type Walked<'a> = (u64, u64, &'a [u8], Bytes);
/// An event as `keyed` yields it: (seq, time, key, payload).
pub type Keyed = (u64, u64, Vec<u8>, Bytes);
/// A PCZ2 event column: (time, key, payload length, dropped).
type ZColumn<'a> = (u64, &'a [u8], u32, bool);
/// `(time, Some((key, payload)))`, or `(time, None)` for a tombstone.
type RawEvent<'a> = (u64, Option<(&'a [u8], &'a [u8])>);
pub const MAX_KEY: usize = 1024;
pub const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_EVENTS_PER_BATCH: u32 = 65_536;
pub const MAX_NAME: usize = 255;
/// Most writer logs a cursor from the wire names (`Subscribe::cursor`, `Control::ReadCursor`):
/// a feed holds its cursor for as long as it is open and a read copies it, an entry a log,
/// and a feed is not counted by a node's admission. A stream has a writer log per producer
/// and epoch (a handful for a session), so 4,096 is far above an honest reader's, and it
/// bounds a feed's cursor to about a megabyte at the longest writer names. A reader of a
/// stream with more names the logs it has read furthest in: a log its cursor does not name
/// is read from its start.
pub const MAX_CURSOR_LOGS: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LogId {
    pub stream: String,
    pub writer: String,
    pub epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BatchId {
    pub log: LogId,
    pub first_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub stream: String,
    pub writer: String,
    pub epoch: u64,
    pub seq: u64,
    pub event_time_us: u64,
    #[serde(with = "payload_str")]
    pub payload: Bytes,
}

mod payload_str {
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(b: &Bytes, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&String::from_utf8_lossy(b))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Bytes, D::Error> {
        Ok(Bytes::from(String::deserialize(d)?))
    }
}

/// A parsed, checksum-verified batch. Holds the original bytes.
#[derive(Clone, Debug)]
pub struct Batch {
    pub id: BatchId,
    pub count: u32,
    pub bytes: Bytes,
    /// Offset of the first event record inside `bytes`.
    events_off: usize,
    /// Format 1: events without keys.
    v1: bool,
    /// PCZ2: (offset of the block, uncompressed payload bytes).
    z: Option<(usize, u32)>,
    /// The batch's identity checksum: its own CRC, or the CRC of the batch it was rewritten from
    /// (dedup tombstones), so a producer's retry of the original bytes still matches.
    pub ident: u32,
    /// Events stored with their payload (`kept`), counted as `parse` validates them.
    kept: u32,
}

impl Batch {
    /// Events without dedup keys: format 1, which has no key or tombstone fields to write or check.
    pub fn encode(log: &LogId, first_seq: u64, events: &[(u64, &[u8])]) -> Result<Bytes> {
        ensure!(!events.is_empty(), "empty batch");
        ensure!(
            events.len() as u64 <= MAX_EVENTS_PER_BATCH as u64,
            "too many events"
        );
        ensure!(
            log.stream.len() <= MAX_NAME && log.writer.len() <= MAX_NAME,
            "name too long"
        );
        ensure!(
            !log.stream.is_empty() && !log.writer.is_empty(),
            "empty name"
        );
        first_seq
            .checked_add(events.len() as u64)
            .context("sequence overflow")?;
        let payload: usize = events.iter().map(|(_, p)| 12 + p.len()).sum();
        let len = 4 + 2 + log.stream.len() + 2 + log.writer.len() + 8 + 8 + 4 + payload + 4;
        ensure!(len <= MAX_BATCH_BYTES, "batch too large ({len} bytes)");
        let mut b = BytesMut::with_capacity(len);
        b.put_slice(BATCH_MAGIC_V1);
        b.put_u16_le(log.stream.len() as u16);
        b.put_slice(log.stream.as_bytes());
        b.put_u16_le(log.writer.len() as u16);
        b.put_slice(log.writer.as_bytes());
        b.put_u64_le(log.epoch);
        b.put_u64_le(first_seq);
        b.put_u32_le(events.len() as u32);
        for (t, p) in events {
            b.put_u64_le(*t);
            b.put_u32_le(p.len() as u32);
            b.put_slice(p);
        }
        let crc = crc32c::crc32c(&b);
        b.put_u32_le(crc);
        debug_assert_eq!(b.len(), len);
        Ok(b.freeze())
    }

    /// Events of `(time, dedup key, payload)`; an empty key is never deduplicated.
    pub fn encode_keyed(
        log: &LogId,
        first_seq: u64,
        events: &[(u64, &[u8], &[u8])],
    ) -> Result<Bytes> {
        let events: Vec<RawEvent<'_>> = events.iter().map(|&(t, k, p)| (t, Some((k, p)))).collect();
        Self::encode_events(log, first_seq, 0, &events)
    }

    /// `None` is a tombstone.
    /// `ident` 0: the batch is its own identity.
    fn encode_events(
        log: &LogId,
        first_seq: u64,
        ident: u32,
        events: &[RawEvent<'_>],
    ) -> Result<Bytes> {
        ensure!(!events.is_empty(), "empty batch");
        ensure!(
            events.len() as u64 <= MAX_EVENTS_PER_BATCH as u64,
            "too many events"
        );
        ensure!(
            log.stream.len() <= MAX_NAME && log.writer.len() <= MAX_NAME,
            "name too long"
        );
        ensure!(
            !log.stream.is_empty() && !log.writer.is_empty(),
            "empty name"
        );
        first_seq
            .checked_add(events.len() as u64)
            .context("sequence overflow")?;
        for (_, e) in events {
            if let Some((k, p)) = e {
                ensure!(k.len() <= MAX_KEY, "dedup key too long");
                ensure!(p.len() < TOMBSTONE as usize, "payload too large");
            }
        }
        let payload: usize = events
            .iter()
            .map(|(_, e)| 14 + e.map_or(0, |(k, p)| k.len() + p.len()))
            .sum();
        let len = 4 + 2 + log.stream.len() + 2 + log.writer.len() + 8 + 8 + 4 + 4 + payload + 4;
        ensure!(len <= MAX_BATCH_BYTES, "batch too large ({len} bytes)");
        let mut b = BytesMut::with_capacity(len);
        b.put_slice(BATCH_MAGIC);
        b.put_u16_le(log.stream.len() as u16);
        b.put_slice(log.stream.as_bytes());
        b.put_u16_le(log.writer.len() as u16);
        b.put_slice(log.writer.as_bytes());
        b.put_u64_le(log.epoch);
        b.put_u64_le(first_seq);
        b.put_u32_le(events.len() as u32);
        b.put_u32_le(ident);
        for (t, e) in events {
            b.put_u64_le(*t);
            let (k, p) = e.unwrap_or((b"", b""));
            b.put_u16_le(k.len() as u16);
            b.put_u32_le(if e.is_some() {
                p.len() as u32
            } else {
                TOMBSTONE
            });
            b.put_slice(k);
            b.put_slice(p);
        }
        let crc = crc32c::crc32c(&b);
        b.put_u32_le(crc);
        debug_assert_eq!(b.len(), len);
        Ok(b.freeze())
    }

    /// The stream a batch's bytes name, from its header alone: nothing else is validated
    /// (`parse`). None: too short to name one.
    pub fn stream_of(bytes: &[u8]) -> Option<&[u8]> {
        let n = u16::from_le_bytes(bytes.get(4..6)?.try_into().ok()?) as usize;
        bytes.get(6..6 + n)
    }

    /// Parses and fully validates (checksum, structure, bounds) a batch.
    pub fn parse(bytes: Bytes) -> Result<Batch> {
        ensure!(bytes.len() <= MAX_BATCH_BYTES, "batch too large");
        ensure!(bytes.len() >= 4 + 2 + 2 + 20 + 4, "batch too short");
        let (body, crc) = bytes.split_at(bytes.len() - 4);
        let crc = u32::from_le_bytes(crc.try_into().unwrap());
        ensure!(crc32c::crc32c(body) == crc, "batch checksum mismatch");
        let mut c = body;
        let v1 = &c[..4] == BATCH_MAGIC_V1;
        let compressed = &c[..4] == BATCH_MAGIC_Z;
        ensure!(
            v1 || compressed || &c[..4] == BATCH_MAGIC,
            "bad batch magic"
        );
        c.advance(4);
        let stream = take_str(&mut c)?;
        let writer = take_str(&mut c)?;
        ensure!(c.remaining() >= 20, "truncated header");
        let epoch = c.get_u64_le();
        let first_seq = c.get_u64_le();
        let count = c.get_u32_le();
        let ident = if v1 {
            0
        } else {
            ensure!(c.remaining() >= 4, "truncated header");
            c.get_u32_le()
        };
        // ponytail: a batch whose own CRC is 0 (1 in 2^32) cannot be named as an identity, so a
        // retry of it after a dedup rewrite is refused as a conflict; widen ident if that matters
        let ident = if ident == 0 { crc } else { ident };
        ensure!(
            count > 0 && count <= MAX_EVENTS_PER_BATCH,
            "bad event count"
        );
        first_seq
            .checked_add(count as u64)
            .context("sequence overflow")?;
        let raw_len = if compressed {
            ensure!(c.remaining() >= 4, "truncated header");
            c.get_u32_le()
        } else {
            0
        };
        let events_off = body.len() - c.remaining();
        let mut z = None;
        let mut kept = 0;
        if compressed {
            let mut sum = 0u64;
            for _ in 0..count {
                ensure!(c.remaining() >= 14, "truncated event");
                c.advance(8);
                let (k, n) = (c.get_u16_le() as usize, c.get_u32_le());
                ensure!(
                    k <= MAX_KEY && (n & DROPPED == 0 || k == 0),
                    "bad event column"
                );
                ensure!(c.remaining() >= k, "truncated key");
                c.advance(k);
                sum += (n & !DROPPED) as u64;
                kept += (n & DROPPED == 0) as u32;
            }
            ensure!(
                sum == raw_len as u64 && raw_len as usize <= MAX_BATCH_BYTES * 4,
                "bad raw length"
            );
            ensure!(c.remaining() >= 4, "truncated block");
            let n = c.get_u32_le() as usize;
            ensure!(c.remaining() == n, "bad block length");
            z = Some((body.len() - n, raw_len));
        } else {
            for _ in 0..count {
                let n = if v1 {
                    // format 1: time and payload length only
                    ensure!(c.remaining() >= 12, "truncated event");
                    c.advance(8);
                    let n = c.get_u32_le();
                    ensure!(n != TOMBSTONE, "bad payload length");
                    kept += 1;
                    n as usize
                } else {
                    let (k, n) = take_event(&mut c, false)?;
                    kept += n.is_some() as u32;
                    k + n.unwrap_or(0)
                };
                ensure!(c.remaining() >= n, "truncated payload");
                c.advance(n);
            }
            ensure!(c.remaining() == 0, "trailing bytes in batch");
        }
        Ok(Batch {
            id: BatchId {
                log: LogId {
                    stream,
                    writer,
                    epoch,
                },
                first_seq,
            },
            count,
            bytes,
            events_off,
            v1,
            z,
            ident,
            kept,
        })
    }

    /// Events of `(time, dedup key, payload)` with every payload in one lz4 block (PCZ2).
    pub fn encode_compressed(
        log: &LogId,
        first_seq: u64,
        events: &[(u64, &[u8], &[u8])],
    ) -> Result<Bytes> {
        let raw: Vec<u8> = events
            .iter()
            .flat_map(|(_, _, p)| p.iter().copied())
            .collect();
        let cols: Vec<ZColumn<'_>> = events
            .iter()
            .map(|&(t, k, p)| (t, k, p.len() as u32, false))
            .collect();
        Self::encode_z(
            log,
            first_seq,
            0,
            &cols,
            raw.len() as u32,
            &lz4_flex::block::compress(&raw),
        )
    }

    fn encode_z(
        log: &LogId,
        first_seq: u64,
        ident: u32,
        cols: &[ZColumn<'_>],
        raw_len: u32,
        block: &[u8],
    ) -> Result<Bytes> {
        ensure!(
            !cols.is_empty() && cols.len() <= MAX_EVENTS_PER_BATCH as usize,
            "bad event count"
        );
        ensure!(
            !log.stream.is_empty() && !log.writer.is_empty(),
            "empty name"
        );
        ensure!(
            log.stream.len() <= MAX_NAME && log.writer.len() <= MAX_NAME,
            "name too long"
        );
        first_seq
            .checked_add(cols.len() as u64)
            .context("sequence overflow")?;
        let mut b = BytesMut::with_capacity(64 + cols.len() * 24 + block.len());
        b.put_slice(BATCH_MAGIC_Z);
        for n in [&log.stream, &log.writer] {
            b.put_u16_le(n.len() as u16);
            b.put_slice(n.as_bytes());
        }
        b.put_u64_le(log.epoch);
        b.put_u64_le(first_seq);
        b.put_u32_le(cols.len() as u32);
        b.put_u32_le(ident);
        b.put_u32_le(raw_len);
        for &(t, k, n, dropped) in cols {
            ensure!(
                k.len() <= MAX_KEY && n & DROPPED == 0,
                "dedup key or payload too long"
            );
            b.put_u64_le(t);
            let k = if dropped { &b""[..] } else { k };
            b.put_u16_le(k.len() as u16);
            // n < DROPPED (checked above): adding the flag sets its bit
            b.put_u32_le(if dropped { n + DROPPED } else { n });
            b.put_slice(k);
        }
        b.put_u32_le(block.len() as u32);
        b.put_slice(block);
        ensure!(b.len() + 4 <= MAX_BATCH_BYTES, "batch too large");
        let crc = crc32c::crc32c(&b);
        b.put_u32_le(crc);
        Ok(b.freeze())
    }

    /// PCZ2 event columns.
    fn columns(&self) -> impl Iterator<Item = ZColumn<'_>> + '_ {
        let mut c = &self.bytes[self.events_off..];
        (0..self.count).map(move |_| {
            let t = c.get_u64_le();
            let (k, n) = (c.get_u16_le() as usize, c.get_u32_le());
            let key = &c[..k];
            c.advance(k);
            (t, key, n & !DROPPED, n & DROPPED != 0)
        })
    }

    /// Every event but dropped ones as (seq, time, key, payload); decompresses a PCZ2 block.
    fn walk(&self) -> Result<Vec<Walked<'_>>> {
        let first = self.id.first_seq;
        let Some((off, raw_len)) = self.z else {
            return Ok(self
                .raw()
                .enumerate()
                .filter_map(|(i, (t, e))| {
                    e.map(|(k, p)| (first + i as u64, t, k, self.bytes.slice_ref(p)))
                })
                .collect());
        };
        let block = &self.bytes[off..self.bytes.len() - 4];
        let raw = Bytes::from(
            lz4_flex::block::decompress(block, raw_len as usize)
                .context("damaged compressed block")?,
        );
        anyhow::ensure!(
            raw.len() == raw_len as usize,
            "compressed block has the wrong length"
        );
        let mut at = 0usize;
        let mut out = Vec::with_capacity(self.count as usize);
        for (i, (t, k, n, dropped)) in self.columns().enumerate() {
            let n = n as usize;
            if !dropped {
                out.push((first + i as u64, t, k, raw.slice(at..at + n)));
            }
            at += n;
        }
        Ok(out)
    }

    /// The batch's events without dropped ones; an error if a compressed block is damaged.
    pub fn try_events(&self) -> Result<Vec<Event>> {
        Ok(self
            .walk()?
            .into_iter()
            .map(|(seq, t, _, payload)| Event {
                stream: self.id.log.stream.clone(),
                writer: self.id.log.writer.clone(),
                epoch: self.id.log.epoch,
                seq,
                event_time_us: t,
                payload,
            })
            .collect())
    }

    /// `(time, key, payload)` of every event, `None` for a tombstone.
    fn raw(&self) -> impl Iterator<Item = RawEvent<'_>> + '_ {
        let mut c = &self.bytes[self.events_off..self.bytes.len() - 4];
        (0..self.count).map(move |_| {
            let t = u64::from_le_bytes(c[..8].try_into().unwrap());
            let (k, n) = take_event(&mut c, self.v1).expect("validated by parse");
            let e = n.map(|n| (&c[..k], &c[k..k + n]));
            c.advance(k + n.unwrap_or(0));
            (t, e)
        })
    }

    /// `(seq, time, key, payload)` of every event but tombstones; an empty key for keyless
    /// events.
    pub fn keyed(&self) -> Result<Vec<Keyed>> {
        Ok(self
            .walk()?
            .into_iter()
            .map(|(seq, t, k, p)| (seq, t, k.to_vec(), p))
            .collect())
    }

    /// Each event's dedup key; `None` for no key or a dropped event. Never decompresses.
    pub fn keys(&self) -> Box<dyn Iterator<Item = Option<&[u8]>> + '_> {
        if self.z.is_some() {
            return Box::new(
                self.columns()
                    .map(|(_, k, _, dropped)| Some(k).filter(|k| !dropped && !k.is_empty())),
            );
        }
        Box::new(
            self.raw()
                .map(|(_, e)| e.map(|(k, _)| k).filter(|k| !k.is_empty())),
        )
    }

    /// This batch with the flagged events dropped (same identity and sequence numbers). A PCZ2
    /// block is kept as it is: dropping never recompresses.
    pub fn without(&self, drop: &[bool]) -> Result<Bytes> {
        ensure!(drop.len() == self.count as usize, "one flag per event");
        if !drop.contains(&true) {
            return Ok(self.bytes.clone());
        }
        if let Some((off, raw_len)) = self.z {
            let cols: Vec<ZColumn<'_>> = self
                .columns()
                .zip(drop)
                .map(|((t, k, n, d), &x)| (t, k, n, d || x))
                .collect();
            return Self::encode_z(
                &self.id.log,
                self.id.first_seq,
                self.ident,
                &cols,
                raw_len,
                &self.bytes[off..self.bytes.len() - 4],
            );
        }
        let events: Vec<_> = self
            .raw()
            .zip(drop)
            .map(|((t, e), &d)| (t, e.filter(|_| !d)))
            .collect();
        Self::encode_events(&self.id.log, self.id.first_seq, self.ident, &events)
    }

    /// Events stored with their payload: neither dropped as a duplicate nor a tombstone (a
    /// PCZ2 block has none).
    pub fn kept(&self) -> u64 {
        self.kept as u64
    }

    pub fn end_seq(&self) -> u64 {
        self.id.first_seq + self.count as u64
    }

    /// The batch's events, without dropped ones. A damaged compressed block (only a broken
    /// producer can write one: the CRC covers it) yields none; `try_events` reports it.
    pub fn events(&self) -> impl Iterator<Item = Event> + use<> {
        self.try_events().unwrap_or_default().into_iter()
    }
}

/// Skips the event time; returns (key length, payload length), `None` for a tombstone.
fn take_event(c: &mut &[u8], v1: bool) -> Result<(usize, Option<usize>)> {
    if v1 {
        ensure!(c.remaining() >= 12, "truncated event");
        c.advance(8);
        let n = c.get_u32_le();
        ensure!(n != TOMBSTONE, "bad payload length");
        return Ok((0, Some(n as usize)));
    }
    ensure!(c.remaining() >= 14, "truncated event");
    c.advance(8);
    let k = c.get_u16_le() as usize;
    let n = c.get_u32_le();
    ensure!(k <= MAX_KEY, "dedup key too long");
    if n == TOMBSTONE {
        ensure!(k == 0, "tombstone with a key");
        return Ok((0, None));
    }
    Ok((k, Some(n as usize)))
}

fn take_str(c: &mut &[u8]) -> Result<String> {
    ensure!(c.remaining() >= 2, "truncated name");
    let n = c.get_u16_le() as usize;
    ensure!(
        n > 0 && n <= MAX_NAME && c.remaining() >= n,
        "bad name length"
    );
    let s = std::str::from_utf8(&c[..n])
        .context("name not utf8")?
        .to_owned();
    c.advance(n);
    Ok(s)
}

// ---------------------------------------------------------------------------
// RPC frames
// ---------------------------------------------------------------------------
//
// frame := u32 len | u8 kind | u64 req_id | body          (len covers kind..body)
//
// Hot-path bodies (Append / Replicate / Ack) are hand-encoded; the rare
// control messages use JSON.

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Hello = 1,
    Append = 2,
    Replicate = 3,
    Ack = 4,
    Control = 5,
    ControlResp = 6,
    Error = 7,
    /// Response to `Control::Fetch`: repeated `u32 len | batch`.
    Batches = 8,
    /// Open a live subscription (JSON `Subscribe`); answered by `Push` frames
    /// carrying the subscription's req_id, each one raw batch.
    Subscribe = 9,
    Push = 10,
    /// A message of the control plane's log between two nodes (`consensus::Wire`), and its
    /// answer: only a peer sends one.
    Raft = 11,
}

impl Kind {
    pub fn from_u8(v: u8) -> Result<Kind> {
        Ok(match v {
            1 => Kind::Hello,
            2 => Kind::Append,
            3 => Kind::Replicate,
            4 => Kind::Ack,
            5 => Kind::Control,
            6 => Kind::ControlResp,
            7 => Kind::Error,
            8 => Kind::Batches,
            9 => Kind::Subscribe,
            10 => Kind::Push,
            11 => Kind::Raft,
            _ => bail!("unknown frame kind {v}"),
        })
    }
}

#[derive(Clone, Debug)]
pub struct Frame {
    pub kind: Kind,
    pub req_id: u64,
    pub body: Bytes,
}

impl Frame {
    pub fn new(kind: Kind, req_id: u64, body: Bytes) -> Self {
        Frame { kind, req_id, body }
    }
    pub fn encode_into(&self, out: &mut BytesMut) {
        out.reserve(13 + self.body.len());
        out.put_u32_le((9 + self.body.len()) as u32);
        out.put_u8(self.kind as u8);
        out.put_u64_le(self.req_id);
        out.put_slice(&self.body);
    }
}

/// Append body: u8 copies | batch
pub fn encode_append(copies: u8, batch: &Bytes) -> Bytes {
    let mut b = BytesMut::with_capacity(1 + batch.len());
    b.put_u8(copies);
    b.put_slice(batch);
    b.freeze()
}

pub fn decode_append(mut body: Bytes) -> Result<(u8, Bytes)> {
    ensure!(!body.is_empty(), "empty append");
    let copies = body.get_u8();
    Ok((copies, body))
}

/// Replicate body: u32 origin node | batch
pub fn encode_replicate(origin: u32, batch: &Bytes) -> Bytes {
    let mut b = BytesMut::with_capacity(4 + batch.len());
    b.put_u32_le(origin);
    b.put_slice(batch);
    b.freeze()
}

pub fn decode_replicate(mut body: Bytes) -> Result<(u32, Bytes)> {
    ensure!(body.len() >= 4, "short replicate");
    let origin = body.get_u32_le();
    Ok((origin, body))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AckStatus {
    /// Stored durably by `copies` distinct failure domains.
    Ok = 0,
    /// Identical batch already present (idempotent retry). Same guarantees as Ok.
    Duplicate = 1,
    /// The batch identity is already used with different content.
    Conflict = 2,
    /// Fewer copies than requested were confirmed; the batch may or may not
    /// survive. Retrying the exact same batch is safe.
    Unknown = 3,
    /// Rejected before any side effect.
    Rejected = 4,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ack {
    pub status: AckStatus,
    /// Number of distinct failure domains with a durable copy at reply time.
    pub copies: u8,
    /// Bitmask of node ids (< 32) that confirmed a durable copy.
    pub nodes: u32,
    /// crc32c of the batch the replying node stored.
    pub crc: u32,
}

impl Ack {
    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(10);
        b.put_u8(self.status as u8);
        b.put_u8(self.copies);
        b.put_u32_le(self.nodes);
        b.put_u32_le(self.crc);
        b.freeze()
    }
    pub fn decode(mut b: Bytes) -> Result<Ack> {
        ensure!(b.len() == 10, "bad ack length");
        let status = match b.get_u8() {
            0 => AckStatus::Ok,
            1 => AckStatus::Duplicate,
            2 => AckStatus::Conflict,
            3 => AckStatus::Unknown,
            4 => AckStatus::Rejected,
            v => bail!("bad ack status {v}"),
        };
        Ok(Ack {
            status,
            copies: b.get_u8(),
            nodes: b.get_u32_le(),
            crc: b.get_u32_le(),
        })
    }
    pub fn is_success(&self) -> bool {
        matches!(self.status, AckStatus::Ok | AckStatus::Duplicate)
    }
    /// Whether this is node `node`'s own word that it holds the batch with this crc durably.
    pub fn confirms(&self, crc: u32, node: u32) -> bool {
        self.is_success() && self.crc == crc && self.nodes == 1 << node
    }
}

/// Storage key of a writer log: `u16 len | stream | u16 len | writer | u64 epoch`.
pub fn encode_log_key(log: &LogId) -> Vec<u8> {
    let mut v = Vec::with_capacity(12 + log.stream.len() + log.writer.len());
    v.extend_from_slice(&(log.stream.len() as u16).to_le_bytes());
    v.extend_from_slice(log.stream.as_bytes());
    v.extend_from_slice(&(log.writer.len() as u16).to_le_bytes());
    v.extend_from_slice(log.writer.as_bytes());
    v.extend_from_slice(&log.epoch.to_le_bytes());
    v
}

/// The key of a record that replaces a conflicting batch under the same identity (a node that
/// held bytes a majority of nodes do not): the log key and one byte `1`. Recovery keeps the
/// replacement. A node that predates it refuses such a log ("bad log key") instead of reading it.
pub fn encode_replacing_key(log: &LogId) -> Vec<u8> {
    let mut v = encode_log_key(log);
    v.push(1);
    v
}

pub fn decode_log_key(b: &[u8]) -> Result<LogId> {
    Ok(decode_record_key(b)?.0)
}

/// A record's writer log, and whether the record replaces a conflicting batch.
pub fn decode_record_key(mut b: &[u8]) -> Result<(LogId, bool)> {
    let stream = take_str(&mut b)?;
    let writer = take_str(&mut b)?;
    let replaces = match b.len() {
        8 => false,
        9 if b[8] == 1 => true,
        _ => bail!("bad log key"),
    };
    Ok((
        LogId {
            stream,
            writer,
            epoch: u64::from_le_bytes(b[..8].try_into().unwrap()),
        },
        replaces,
    ))
}

pub fn batch_crc(batch: &Bytes) -> u32 {
    u32::from_le_bytes(batch[batch.len() - 4..].try_into().unwrap())
}

/// Hello body: JSON.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Hello {
    pub token: String,
    /// Set when the connecting side is a mesh node (peer link).
    pub node_id: Option<u32>,
}

/// Live subscription request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Subscribe {
    /// Only this stream (all streams if None).
    pub stream: Option<String>,
    /// Start from the node's first durable batch (`true`) or from now.
    pub from_start: bool,
    /// A feed of one stream from this position (`Node::cursor_feed`): what a cursor read from
    /// it gives, then every batch after, per writer log in sequence. `from_start` is not
    /// looked at. A reader that lost its feed opens one again, on any node, from the
    /// position after the last batch it received.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<WireCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Control {
    Status,
    /// Event-time ordered snapshot read of one stream from local durable state.
    Read {
        stream: String,
        from_time_us: u64,
        limit: u32,
    },
    /// One writer log's batches from `from_seq`, answered as `Kind::Batches` (repair).
    FetchLog {
        log: LogId,
        from_seq: u64,
        max_bytes: u32,
    },
    /// Raw batch bytes for the given ids (missing ones are skipped).
    Fetch {
        ids: Vec<BatchId>,
    },
    /// Run one anti-entropy pass against every peer now.
    Repair,
    /// The stream's batches past `cursor`, answered as `Kind::Batches` (see `Store::read_cursor`).
    ReadCursor {
        stream: String,
        cursor: WireCursor,
        max_bytes: u32,
    },
    /// A cursor at the first batch logged at or after `time_ms` (`u64::MAX`: the end).
    CursorAt {
        stream: String,
        time_ms: u64,
    },
    /// Store a reader group's position.
    Commit {
        group: String,
        stream: String,
        cursor: WireCursor,
    },
    /// A reader group's committed position.
    Position {
        group: String,
        stream: String,
    },
    /// A compacted stream's newest value of every key.
    Latest {
        stream: String,
    },
    /// One event of an extents stream, rebuilt by the node (`Node::read_event`): the event
    /// whose first stored event has sequence `seq` in `log`. Answered as `Kind::Batches`
    /// holding the event's bytes and nothing else, or as `ControlResp::Event`.
    ReadEvent {
        log: LogId,
        seq: u64,
    },
    /// Deletes the stream: every writer log of it there is, on every node, for good
    /// (`Node::delete_stream`). Answered with the epoch it is deleted below: a writer of a new
    /// stream of that name needs an epoch at or above it.
    Delete {
        stream: String,
    },
    /// The names of the streams with this prefix that hold something, in order, after
    /// `after`: at most `limit` (`Node::streams`). `local`: what this node holds alone, which
    /// is what the node that was asked asks its peers.
    Streams {
        prefix: String,
        #[serde(default)]
        after: String,
        limit: u32,
        #[serde(default)]
        local: bool,
    },
    /// What the streams with this prefix hold on this node, and how many hold something
    /// (`Store::stored`): the usage meter's sample. A node answers for itself; the asker asks
    /// each.
    Stored {
        prefix: String,
    },
    /// A command of the control plane (`control::Command`): committed by the nodes' log before
    /// it is answered. For the cluster's own token only.
    Tenant {
        command: crate::control::Command,
    },
    /// The tenants, and how many keys each holds, as this node's copy has them.
    Tenants,
    /// What a tenant's key opens, as this node's copy of the control plane says now: a
    /// gateway asks for the keys presented to it. For the cluster's own token only. The token
    /// is a secret: a request is never logged.
    Scope {
        token: String,
    },
    /// The stream's nodes by this node's list, in the order a request for it tries them
    /// (`Node::nodes_of`): a client asks them itself, not through a node that passes it on.
    Nodes {
        stream: String,
    },
    /// The list of the cluster's nodes this node goes by (`Node::adopt`).
    Placement,
    /// Go by this list of nodes from now on, if it is newer than the node's (a higher
    /// version). Every node of the cluster is sent it, those it adds and those it leaves out.
    Place {
        version: u64,
        nodes: Vec<Member>,
        #[serde(flatten)]
        rules: Rules,
    },
    /// A node that hands a stream off asks one of the stream's nodes which of these batches
    /// of a writer log, (first sequence, identity) each, it lacks. A node that does not hold
    /// the stream itself answers with an error: it vouches for nothing.
    Lacks {
        log: LogId,
        batches: Vec<(u64, u32)>,
    },
    /// Repair: the frontiers of the logs with a batch in this node's log from LSN `from`, at
    /// most `max` records of it (`Store::inventory`). `incarnation`: the boot of this node the
    /// asker last followed; after another, LSNs past where its log ended at that boot's open
    /// may hold other records, so it answers from there at the latest.
    Inventory {
        from: u64,
        max: u32,
        incarnation: u64,
    },
}

/// A cursor within one stream: (writer, epoch, next sequence) per writer log.
pub type WireCursor = Vec<(String, u64, u64)>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ControlResp {
    Status(Status),
    Read {
        events: Vec<Event>,
        coverage: Vec<Frontier>,
    },
    Repair {
        fetched: u64,
        errors: Vec<String>,
    },
    Cursor {
        cursor: Option<WireCursor>,
    },
    Committed,
    Latest {
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    },
    Inventory(Inventory),
    Deleted {
        epoch: u64,
    },
    /// A page of stream names (`Control::Streams`), in order. `more`: there are names after
    /// the last of these; ask again with it as `after`.
    Streams {
        names: Vec<String>,
        more: bool,
    },
    /// A command committed (`Control::Tenant`); `refused`: why the state did not take it.
    Tenant {
        refused: Option<String>,
    },
    Tenants {
        tenants: Vec<(String, usize)>,
    },
    /// What the key opens (`Control::Scope`); None: it is not taken (no such key, revoked,
    /// expired, or the node's copy is not confirmed within the lease).
    Scope {
        scope: Option<crate::control::Scope>,
    },
    /// What a prefix stores on the node that answers (`Control::Stored`).
    Stored {
        streams: u64,
        bytes: u64,
    },
    /// A stream's nodes (`Control::Nodes`). The node that answers knows its own address only
    /// from a list it adopted: under its config's list its `addr` is empty, and it is the
    /// address the asker reached it at.
    Nodes {
        nodes: Vec<Member>,
    },
    Placement {
        version: u64,
        nodes: Vec<Member>,
        #[serde(flatten)]
        rules: Rules,
    },
    /// Whether the node took the list it was sent (it did not if its own is as new).
    Placed {
        adopted: bool,
    },
    Lacks {
        seqs: Vec<u64>,
    },
    /// No event for a `ReadEvent`. `gone`: retention took it or its arena's start, for
    /// good; else this node does not hold all of it (another may).
    Event {
        gone: bool,
    },
}

/// The placement scheme this version computes (`placement::nodes_of`): rendezvous hashing by
/// xxh3-64 of the stream's name seeded with the node's id, one node per failure domain.
pub const PLACEMENT_SCHEME: u8 = 1;

/// How a list of nodes places streams: part of the list, so every node that goes by the list
/// places a stream alike whatever its own config says, and a node that does not know the
/// scheme refuses the list (`Node::adopt_list`) and does not place streams by another.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rules {
    pub scheme: u8,
    /// The nodes a stream is on, unless `copies` names it.
    pub target_copies: u8,
    /// The nodes a stream, or a class of streams (`"acme/*"`), is on.
    #[serde(default)]
    pub copies: std::collections::BTreeMap<String, u8>,
}

/// A node of the cluster: its id, its failure domain and where its peers reach it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    pub id: u32,
    pub domain: String,
    pub addr: String,
}

impl ControlResp {
    /// The cursor an answer carries, if it carries one.
    pub fn cursor(self) -> Option<WireCursor> {
        match self {
            ControlResp::Cursor { cursor } => cursor,
            _ => None,
        }
    }

    /// The sequences a node answered it lacks (`Control::Lacks`).
    pub fn lacks(self) -> Option<Vec<u64>> {
        match self {
            ControlResp::Lacks { seqs } => Some(seqs),
            _ => None,
        }
    }

    /// The epoch a delete answered with.
    pub fn deleted(&self) -> Option<u64> {
        match self {
            ControlResp::Deleted { epoch } => Some(*epoch),
            _ => None,
        }
    }
}

/// An answer to `Control::Inventory`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Inventory {
    /// This boot of the node.
    pub incarnation: u64,
    /// The next LSN to ask for, and the end of what is durable now.
    pub next: u64,
    pub end: u64,
    pub logs: Vec<Frontier>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Status {
    pub node_id: u32,
    pub domain: String,
    pub batches: u64,
    pub events: u64,
    pub bytes: u64,
    pub wal_bytes: u64,
    pub fsyncs: u64,
    /// Segment files in the node's log.
    #[serde(default)]
    pub segments: u64,
    /// Bytes the last startup had to scan to recover (only the active segment).
    #[serde(default)]
    pub recovery_scanned_bytes: u64,
    /// Events dropped as duplicates of an event seen within the dedup window.
    #[serde(default)]
    pub dedup_dropped: u64,
    /// Dedup ids held, and how far back they reach (the window, or less at the entry cap).
    #[serde(default)]
    pub dedup_entries: u64,
    #[serde(default)]
    pub dedup_window_s: u64,
    pub logs: Vec<Frontier>,
    /// Streams in RAM, and streams that are only a summary on disk (dormant).
    #[serde(default)]
    pub streams_hot: u64,
    #[serde(default)]
    pub streams_dormant: u64,
}

/// Per writer-log coverage on one node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frontier {
    pub log: LogId,
    /// All sequences `< contiguous` are present locally.
    pub contiguous: u64,
    /// One past the highest sequence present locally.
    pub high: u64,
}

pub fn hex_encode(b: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push(H[(x >> 4) as usize] as char);
        s.push(H[(x & 15) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_key_says_whether_it_replaces_a_conflicting_batch() {
        let log = LogId {
            stream: "s".into(),
            writer: "w".into(),
            epoch: 7,
        };
        assert_eq!(
            decode_record_key(&encode_log_key(&log)).unwrap(),
            (log.clone(), false)
        );
        let k = encode_replacing_key(&log);
        assert_eq!(decode_record_key(&k).unwrap(), (log.clone(), true));
        assert_eq!(decode_log_key(&k).unwrap(), log);
        let mut bad = k.clone();
        *bad.last_mut().unwrap() = 2;
        assert!(
            decode_record_key(&bad).is_err(),
            "an unknown flag is refused"
        );
        bad.push(1);
        assert!(decode_record_key(&bad).is_err(), "so is a longer key");
    }

    fn log() -> LogId {
        LogId {
            stream: "s".into(),
            writer: "w".into(),
            epoch: 7,
        }
    }

    #[test]
    fn batch_roundtrip() {
        let b = Batch::encode(&log(), 10, &[(5, b"a"), (3, b"bc")]).unwrap();
        let p = Batch::parse(b).unwrap();
        assert_eq!(p.id.first_seq, 10);
        assert_eq!(p.end_seq(), 12);
        let ev: Vec<_> = p.events().collect();
        assert_eq!(ev[1].seq, 11);
        assert_eq!(ev[1].event_time_us, 3);
        assert_eq!(&ev[1].payload[..], b"bc");
    }

    #[test]
    fn batch_corruption_detected() {
        let b = Batch::encode(&log(), 0, &[(1, b"hello")]).unwrap();
        for i in 0..b.len() {
            let mut v = b.to_vec();
            v[i] ^= 0x40;
            assert!(
                Batch::parse(Bytes::from(v)).is_err(),
                "flip at {i} undetected"
            );
        }
        assert!(Batch::parse(b.slice(..b.len() - 1)).is_err());
    }

    #[test]
    fn keyed_batches_carry_a_dedup_key_per_event() {
        let b = Batch::encode_keyed(
            &log(),
            4,
            &[(1, b"k1", b"a"), (2, b"", b"bb"), (3, b"k3", b"")],
        )
        .unwrap();
        let p = Batch::parse(b).unwrap();
        let keys: Vec<Option<&[u8]>> = p.keys().collect();
        assert_eq!(keys, vec![Some(&b"k1"[..]), None, Some(&b"k3"[..])]);
        let ev: Vec<_> = p.events().collect();
        assert_eq!(ev.len(), 3);
        assert_eq!((&ev[1].payload[..], ev[2].seq), (&b"bb"[..], 6));
    }

    #[test]
    fn dropping_events_keeps_their_sequence_numbers_and_shrinks_the_batch() {
        // Dedup replaces a duplicate with a tombstone: readers skip it, but the writer's sequence
        // stays contiguous, so a cursor past it is exact on every node.
        let big = vec![7u8; 1000];
        let b = Batch::encode_keyed(
            &log(),
            10,
            &[(1, b"a", &big), (2, b"b", b"keep"), (3, b"c", &big)],
        )
        .unwrap();
        let p = Batch::parse(b.clone()).unwrap();
        let f = Batch::parse(p.without(&[true, false, true]).unwrap()).unwrap();
        assert_eq!((f.id.clone(), f.count, f.end_seq()), (p.id.clone(), 3, 13));
        assert!(
            f.bytes.len() < b.len() - 1900,
            "tombstones carry no payload"
        );
        let ev: Vec<_> = f.events().collect();
        assert_eq!(ev.len(), 1);
        assert_eq!((ev[0].seq, &ev[0].payload[..]), (11, &b"keep"[..]));
        assert_eq!(
            f.keys().collect::<Vec<_>>(),
            vec![None, Some(&b"b"[..]), None]
        );
        // dropping nothing returns the batch as it was
        assert_eq!(p.without(&[false, false, false]).unwrap(), b);
        assert!(p.without(&[true]).is_err(), "one flag per event");
    }

    #[test]
    fn a_batch_keeps_its_identity_when_events_are_dropped() {
        // A producer's retry of the bytes it sent must match what the broker stored after dedup,
        // on every node: the rewritten batch carries the original's checksum as its identity.
        let b = Batch::encode_keyed(&log(), 0, &[(1, b"a", b"x"), (2, b"b", b"y")]).unwrap();
        let p = Batch::parse(b.clone()).unwrap();
        assert_eq!(p.ident, batch_crc(&b));
        let f = Batch::parse(p.without(&[true, false]).unwrap()).unwrap();
        assert_ne!(batch_crc(&f.bytes), batch_crc(&b));
        assert_eq!(f.ident, p.ident);
        // dropping more from an already rewritten batch keeps the first identity
        let g = Batch::parse(f.without(&[false, true]).unwrap()).unwrap();
        assert_eq!(g.ident, p.ident);
    }

    fn trades(n: u64) -> Vec<(u64, Vec<u8>, Vec<u8>)> {
        (0..n)
            .map(|i| {
                let p = format!(r#"{{"exchange":"binance","symbol":"BTCUSDT","trade_id":{i},"price":"6{i}.25","qty":"0.0{i}","side":"buy"}}"#);
                (i, format!("id{i}").into_bytes(), p.into_bytes())
            })
            .collect()
    }

    #[test]
    fn compressed_batches_read_back_and_dedup_needs_no_decompression() {
        let evs = trades(200);
        let refs: Vec<(u64, &[u8], &[u8])> =
            evs.iter().map(|(t, k, p)| (*t, &k[..], &p[..])).collect();
        let plain = Batch::encode_keyed(&log(), 7, &refs).unwrap();
        let z = Batch::encode_compressed(&log(), 7, &refs).unwrap();
        assert!(z.len() * 2 < plain.len(), "{} vs {}", z.len(), plain.len());
        let b = Batch::parse(z).unwrap();
        assert_eq!((b.count, b.end_seq()), (200, 207));
        let keys: Vec<&[u8]> = b.keys().map(|k| k.unwrap()).collect();
        assert_eq!(keys[3], b"id3");
        let got = b.try_events().unwrap();
        assert_eq!(got.len(), 200);
        assert_eq!((got[5].seq, &got[5].payload[..]), (12, &evs[5].2[..]));
        // dedup drops events without recompressing: same block, events hidden, identity kept
        let mut drop = vec![false; 200];
        drop[0] = true;
        drop[199] = true;
        let f = Batch::parse(b.without(&drop).unwrap()).unwrap();
        assert_eq!((f.ident, f.count), (b.ident, 200));
        let kept = f.try_events().unwrap();
        assert_eq!(kept.len(), 198);
        assert_eq!((kept[0].seq, &kept[0].payload[..]), (8, &evs[1].2[..]));
        assert_eq!(f.keys().filter(|k| k.is_some()).count(), 198);
    }

    #[test]
    fn a_compressed_batchs_keyless_events_have_no_dedup_key() {
        // an empty key means "no key": such events must never be deduplicated against each other
        let z = Batch::encode_compressed(
            &log(),
            0,
            &[(1, b"", b"a"), (2, b"id", b"b"), (3, b"", b"a")],
        )
        .unwrap();
        let b = Batch::parse(z).unwrap();
        assert_eq!(b.keys().collect::<Vec<_>>(), [None, Some(&b"id"[..]), None]);
    }

    #[test]
    fn a_damaged_compressed_block_is_an_error_not_silence() {
        let evs = trades(10);
        let refs: Vec<(u64, &[u8], &[u8])> =
            evs.iter().map(|(t, k, p)| (*t, &k[..], &p[..])).collect();
        let z = Batch::encode_compressed(&log(), 0, &refs).unwrap();
        for i in 0..z.len() {
            let mut v = z.to_vec();
            v[i] ^= 0x40;
            assert!(
                Batch::parse(Bytes::from(v)).is_err(),
                "flip at {i} undetected"
            );
        }
        // a producer bug: garbage block under a valid checksum
        let mut v = z[..z.len() - 4].to_vec();
        let n = v.len();
        for x in &mut v[n - 8..] {
            *x = 0xff;
        }
        let c = crc32c::crc32c(&v);
        v.extend_from_slice(&c.to_le_bytes());
        let b = Batch::parse(Bytes::from(v)).unwrap();
        assert!(b.try_events().is_err());
    }

    #[test]
    fn version_1_batches_still_read_as_keyless() {
        // PCB1: magic, names, epoch, first_seq, count, then (time u64, len u32, payload) per event
        let l = log();
        let mut v = b"PCB1".to_vec();
        for n in [&l.stream, &l.writer] {
            v.extend_from_slice(&(n.len() as u16).to_le_bytes());
            v.extend_from_slice(n.as_bytes());
        }
        v.extend_from_slice(&l.epoch.to_le_bytes());
        v.extend_from_slice(&3u64.to_le_bytes());
        v.extend_from_slice(&2u32.to_le_bytes());
        for (t, p) in [(9u64, &b"x"[..]), (8, b"yz")] {
            v.extend_from_slice(&t.to_le_bytes());
            v.extend_from_slice(&(p.len() as u32).to_le_bytes());
            v.extend_from_slice(p);
        }
        let c = crc32c::crc32c(&v);
        v.extend_from_slice(&c.to_le_bytes());
        let p = Batch::parse(Bytes::from(v)).unwrap();
        assert_eq!(p.keys().collect::<Vec<_>>(), vec![None, None]);
        let ev: Vec<_> = p.events().collect();
        assert_eq!((ev[1].seq, &ev[1].payload[..]), (4, &b"yz"[..]));
    }

    #[test]
    fn keyed_batch_corruption_detected() {
        let b = Batch::encode_keyed(&log(), 0, &[(1, b"id", b"hello")]).unwrap();
        let t = Batch::parse(b.clone()).unwrap().without(&[true]).unwrap();
        for x in [b, t] {
            for i in 0..x.len() {
                let mut v = x.to_vec();
                v[i] ^= 0x40;
                assert!(
                    Batch::parse(Bytes::from(v)).is_err(),
                    "flip at {i} undetected"
                );
            }
        }
    }

    #[test]
    fn log_key_roundtrip() {
        let l = log();
        assert_eq!(decode_log_key(&encode_log_key(&l)).unwrap(), l);
        assert!(decode_log_key(&encode_log_key(&l)[..5]).is_err());
    }

    #[test]
    fn ack_roundtrip() {
        let a = Ack {
            status: AckStatus::Unknown,
            copies: 1,
            nodes: 0b101,
            crc: 99,
        };
        assert_eq!(Ack::decode(a.encode()).unwrap(), a);
    }
}

/// Property tests of the codecs a node runs on untrusted bytes: they never panic, a parsed batch
/// can be walked by every accessor (each `expect`s what `parse` validated), and what the encoders
/// write parses back. Mutated batches get their CRC recomputed, so inputs reach the structural
/// checks behind the checksum. (Kani does not finish these in CI time: `Bytes` and anyhow's
/// errors are too much for its solver. See spec/README.md.)
#[cfg(test)]
mod props {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn an_ack_confirms_a_copy_only_as_its_node_s_own_word_for_that_batch() {
        // Why: a copy counts toward an ack, and a node gives a stream up, on a peer's answer.
        // An answer that failed, is for other bytes, or speaks for another node is no copy:
        // counting it would acknowledge, or drop, data that node does not hold.
        let ack = |status, crc, nodes| Ack {
            status,
            copies: 1,
            nodes,
            crc,
        };
        assert!(ack(AckStatus::Ok, 7, 1 << 3).confirms(7, 3));
        assert!(ack(AckStatus::Duplicate, 7, 1 << 3).confirms(7, 3));
        assert!(!ack(AckStatus::Conflict, 7, 1 << 3).confirms(7, 3));
        assert!(!ack(AckStatus::Ok, 8, 1 << 3).confirms(7, 3));
        assert!(!ack(AckStatus::Ok, 7, 1 << 2).confirms(7, 3));
        assert!(!ack(AckStatus::Ok, 7, 0).confirms(7, 3));
    }

    #[test]
    fn only_a_delete_s_answer_has_the_epoch_it_deleted_below() {
        // Why: the SDK takes a delete's epoch from the answer, and any other answer is an error
        // to it, not an epoch.
        assert_eq!(ControlResp::Deleted { epoch: 7 }.deleted(), Some(7));
        assert_eq!(ControlResp::Committed.deleted(), None);
    }

    #[test]
    fn only_a_cursor_s_answer_has_a_cursor() {
        // Why: a node checks a tenant's commit against the writers in the answer of one of the
        // stream's nodes; any other answer names no writer, and the commit is refused.
        let c = vec![("w".to_string(), 1, 2)];
        let answer = ControlResp::Cursor {
            cursor: Some(c.clone()),
        };
        assert_eq!(answer.cursor(), Some(c));
        assert_eq!(ControlResp::Cursor { cursor: None }.cursor(), None);
        assert_eq!(ControlResp::Committed.cursor(), None);
    }

    #[test]
    fn only_an_answer_to_what_a_node_lacks_names_sequences() {
        // Why: a node gives a stream up on these answers. Any other answer (a node of
        // another version) must read as no answer, never as "it lacks nothing".
        assert_eq!(ControlResp::Lacks { seqs: vec![3] }.lacks(), Some(vec![3]));
        assert_eq!(ControlResp::Lacks { seqs: vec![] }.lacks(), Some(vec![]));
        assert_eq!(ControlResp::Committed.lacks(), None);
    }

    fn with_crc(mut body: Vec<u8>) -> Bytes {
        let c = crc32c::crc32c(&body);
        body.extend_from_slice(&c.to_le_bytes());
        Bytes::from(body)
    }

    /// Every accessor of a parsed batch; returns the events it yields.
    fn walk(b: &Batch) -> usize {
        assert!(b.count >= 1 && b.count <= MAX_EVENTS_PER_BATCH);
        assert!(b.end_seq() > b.id.first_seq);
        let keys: Vec<_> = b.keys().collect();
        assert_eq!(keys.len(), b.count as usize);
        assert!(
            keys.iter()
                .flatten()
                .all(|k| !k.is_empty() && k.len() <= MAX_KEY)
        );
        match b.try_events() {
            Ok(evs) => {
                assert!(evs.len() <= b.count as usize);
                assert!(
                    evs.iter()
                        .all(|e| e.seq >= b.id.first_seq && e.seq < b.end_seq())
                );
                assert_eq!(b.keyed().unwrap().len(), evs.len());
                let drop: Vec<bool> = (0..b.count).map(|i| i % 2 == 0).collect();
                let w = Batch::parse(b.without(&drop).unwrap()).unwrap();
                assert_eq!((&w.id, w.count, w.ident), (&b.id, b.count, b.ident));
                evs.len()
            }
            Err(_) => {
                assert!(b.z.is_some(), "only a damaged lz4 block fails to walk");
                0
            }
        }
    }

    type Ev = (u64, Vec<u8>, Vec<u8>);

    fn events() -> impl Strategy<Value = Vec<Ev>> {
        prop::collection::vec(
            (
                any::<u64>(),
                prop::collection::vec(any::<u8>(), 0..4),
                prop::collection::vec(any::<u8>(), 0..6),
            ),
            1..5,
        )
    }

    fn log() -> impl Strategy<Value = LogId> {
        ("[a-z]{1,4}", "[a-z0-9]{1,4}", any::<u64>()).prop_map(|(stream, writer, epoch)| LogId {
            stream,
            writer,
            epoch,
        })
    }

    /// A batch in format `fmt`: 0 = PCB1, 1 = PCB2, 2 = PCZ2.
    fn encode(log: &LogId, seq: u64, evs: &[Ev], fmt: u8) -> Bytes {
        let keyed: Vec<(u64, &[u8], &[u8])> =
            evs.iter().map(|(t, k, p)| (*t, &k[..], &p[..])).collect();
        match fmt {
            0 => {
                let plain: Vec<(u64, &[u8])> = evs.iter().map(|(t, _, p)| (*t, &p[..])).collect();
                Batch::encode(log, seq, &plain).unwrap()
            }
            1 => Batch::encode_keyed(log, seq, &keyed).unwrap(),
            _ => Batch::encode_compressed(log, seq, &keyed).unwrap(),
        }
    }

    #[test]
    fn kept_counts_the_events_stored_with_a_payload_compressed_or_not() {
        // Why: a stream's ingest counter counts what dedup kept; a dropped duplicate keeps its
        // sequence number (and its slot in the batch), so the batch's count is not it.
        let log = LogId {
            stream: "s".into(),
            writer: "w".into(),
            epoch: 1,
        };
        let evs: Vec<(u64, &[u8], &[u8])> = vec![(1, b"a", b"x"), (2, b"b", b"y"), (3, b"c", b"z")];
        for z in [false, true] {
            let bytes = match z {
                false => Batch::encode_keyed(&log, 0, &evs).unwrap(),
                true => Batch::encode_compressed(&log, 0, &evs).unwrap(),
            };
            let b = Batch::parse(bytes).unwrap();
            assert_eq!(b.kept(), 3);
            let less = Batch::parse(b.without(&[false, true, false]).unwrap()).unwrap();
            assert_eq!((less.count, less.kept()), (3, 2), "compressed: {z}");
        }
    }
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]

        #[test]
        fn encoded_batches_parse_back_in_every_format(
            log in log(), seq in 0u64..1 << 40, evs in events(), fmt in 0u8..3,
        ) {
            let b = Batch::parse(encode(&log, seq, &evs, fmt)).unwrap();
            prop_assert_eq!(&b.id, &BatchId { log: log.clone(), first_seq: seq });
            prop_assert_eq!(b.count as usize, evs.len());
            prop_assert_eq!(walk(&b), evs.len());
            let got: Vec<(u64, u64, Vec<u8>)> =
                b.events().map(|e| (e.seq, e.event_time_us, e.payload.to_vec())).collect();
            let want: Vec<(u64, u64, Vec<u8>)> = evs
                .iter()
                .enumerate()
                .map(|(i, (t, _, p))| (seq + i as u64, *t, p.clone()))
                .collect();
            prop_assert_eq!(got, want);
        }

        #[test]
        fn a_mutated_batch_with_a_matching_checksum_never_panics(
            log in log(), evs in events(), fmt in 0u8..3,
            edits in prop::collection::vec((any::<prop::sample::Index>(), any::<u8>()), 1..4),
            cut in any::<prop::sample::Index>(), truncate in any::<bool>(),
        ) {
            let b = encode(&log, 7, &evs, fmt);
            let mut body = b[..b.len() - 4].to_vec();
            for (at, v) in edits {
                let i = at.index(body.len());
                body[i] = v;
            }
            if truncate {
                body.truncate(cut.index(body.len() + 1));
            }
            if let Ok(p) = Batch::parse(with_crc(body)) {
                walk(&p);
            }
        }

        #[test]
        fn arbitrary_bytes_with_a_matching_checksum_never_panic(
            body in prop::collection::vec(any::<u8>(), 0..120), magic in 0usize..4,
        ) {
            let mut body = body;
            let m: [&[u8; 4]; 4] = [BATCH_MAGIC_V1, BATCH_MAGIC, BATCH_MAGIC_Z, b"XXXX"];
            if body.len() >= 4 {
                body[..4].copy_from_slice(m[magic]);
            }
            if let Ok(p) = Batch::parse(with_crc(body)) {
                walk(&p);
            }
        }

        #[test]
        fn acks_round_trip_and_any_body_decodes_or_is_refused(
            status in 0u8..5, copies: u8, nodes: u32, crc: u32,
            junk in prop::collection::vec(any::<u8>(), 0..14),
        ) {
            let mut b = vec![status, copies];
            b.extend_from_slice(&nodes.to_le_bytes());
            b.extend_from_slice(&crc.to_le_bytes());
            let a = Ack::decode(Bytes::from(b.clone())).unwrap();
            prop_assert_eq!(&a.encode()[..], &b[..]);
            if let Ok(a) = Ack::decode(Bytes::from(junk.clone())) {
                prop_assert_eq!(&a.encode()[..], &junk[..]);
            }
        }

        #[test]
        fn append_replicate_and_log_keys_round_trip(
            log in log(), copies: u8, origin: u32,
            batch in prop::collection::vec(any::<u8>(), 0..8),
            junk in prop::collection::vec(any::<u8>(), 0..24),
        ) {
            let batch = Bytes::from(batch);
            prop_assert_eq!(decode_append(encode_append(copies, &batch)).unwrap(), (copies, batch.clone()));
            prop_assert_eq!(decode_replicate(encode_replicate(origin, &batch)).unwrap(), (origin, batch));
            prop_assert_eq!(decode_log_key(&encode_log_key(&log)).unwrap(), log);
            let _ = decode_append(Bytes::from(junk.clone()));
            let _ = decode_replicate(Bytes::from(junk.clone()));
            if let Ok(l) = decode_log_key(&junk) {
                prop_assert_eq!(encode_log_key(&l), junk);
            }
        }
    }
}
