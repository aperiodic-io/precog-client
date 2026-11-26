//! Precog's Rust SDK.
//!
//! - [`Producer`]: batches events of one writer log, each with a dedup key (live-ingestion's
//!   `redpanda-dedup-key`), and appends them durably on 2 nodes. A stream's producers all prefer
//!   the same node, so two producer replicas' copies of an event meet one dedup index. A failed
//!   append is retried with the same bytes on the next node, which is idempotent.
//! - [`Consumer`]: reads a stream from a cursor that is valid on every node, failing over between
//!   nodes; starts at a committed position, the beginning, the end or a time; commits.
//! - [`Client::latest`]: a compacted stream's newest value of every key.
//! - Events larger than a batch: a producer and a consumer that are both `set_chunked` carry an
//!   event of any size up to [`MAX_EVENT_BYTES`] as several events of the stream, each with a
//!   one-byte tag; the broker stores them as any others.
//! - Re-sent context stored once: a producer and a consumer that are both `set_extents` store an
//!   event as what it adds to what its writer log already holds ([`extents`]).

pub mod chunks;
pub mod control;
pub mod cursor;
pub mod extents;
pub mod mesh;
pub mod placement;
pub mod proto;
pub mod rpc;
#[cfg(feature = "testkit")]
pub mod testkit;
pub mod transport;

pub use proto::LogId;
pub use transport::Addr;

use crate::chunks::{Assembly, WHOLE, chunks};
pub use crate::chunks::{CHUNK_BYTES, MAX_EVENT_BYTES};

use crate::cursor::{cursor_from_wire, cursor_to_wire};
use crate::mesh::MeshClient;
use crate::proto::{Batch, Control, ControlResp, Kind};
use anyhow::{Context, Result, bail};
use bytes::{Buf, Bytes};
use std::sync::Arc;

pub use crate::cursor::{Cursor, now_ms};
pub use crate::proto::Event;

/// Batches a producer keeps waiting for their acks (the protocol POCs: pipelining 1.8x).
pub const MAX_IN_FLIGHT: usize = 8;

/// A dedup id as it travels: its 8-byte xxh3 (an empty id stays empty: never deduplicated).
pub fn dedup_key(id: &[u8]) -> Vec<u8> {
    if id.is_empty() {
        vec![]
    } else {
        xxhash_rust::xxh3::xxh3_64(id).to_le_bytes().to_vec()
    }
}

/// Events per batch before `send` asks for a flush (`Producer::is_full`).
pub const MAX_BATCH_EVENTS: usize = 4096;
/// Bytes per batch before `send` asks for a flush.
pub const MAX_BATCH_BYTES: usize = 1 << 20;
const _: () = assert!(
    2 * CHUNK_BYTES == MAX_BATCH_BYTES,
    "two chunks fill a batch"
);

/// A consumer asks again where its stream is this long after it last did: a list of nodes
/// that changed meanwhile moved the stream, and its old nodes pass requests on by then.
const NODES_AGE: std::time::Duration = std::time::Duration::from_secs(10);

/// Where a producer sends: the stream's first node as the cluster has it (index, id), so an
/// append is taken there and not passed on, and a stream's producers still meet one dedup
/// index. Nodes that do not say (an older version, where every node holds every stream):
/// the node a hash of the name gives, of no known id.
fn home(its: Result<Vec<(usize, u32)>>, hashed: usize) -> (usize, u32) {
    (its.ok().and_then(|its| its.first().copied())).unwrap_or((hashed, u32::MAX))
}

/// An append sent to `home` was taken by another node: `home` is down, or no longer one of
/// the stream's nodes (the ack names the nodes that hold the batch, and the node that took
/// it is always one). The producer asks again where the stream is.
fn moved(home: (usize, u32), answered: usize, holders: u32) -> bool {
    // (a node of no known id is in no ack's list: only who answered tells)
    let left = 1u32
        .checked_shl(home.1)
        .is_some_and(|bit| holders & bit == 0);
    answered != home.0 || left
}

/// The nodes a consumer reads from, the one it last read from first: the stream's nodes,
/// or all `n` the client has if the cluster's nodes do not say which they are.
fn readers(its: Result<Vec<(usize, u32)>>, n: usize, last: usize) -> Vec<usize> {
    let its = its.map(|its| its.into_iter().map(|n| n.0).collect());
    let mut its: Vec<usize> = its.unwrap_or_else(|_| (0..n).collect());
    let at = its.iter().position(|&i| i == last).unwrap_or(0);
    its.rotate_left(at);
    its
}

#[derive(Clone)]
pub struct Client {
    mc: Arc<MeshClient>,
}

/// Where a new consumer starts.
#[derive(Clone, Debug)]
pub enum Start {
    Beginning,
    End,
    /// The first batch logged at or after this time (ms since the epoch).
    Time(u64),
    /// The group's committed position, or `or` if it has none.
    Committed {
        or: Box<Start>,
    },
}

/// Trusts the CA certificates in a PEM file for every `tls://` node this process connects to:
/// call it before `Client::connect` to a cluster that serves TLS.
pub use crate::transport::trust as trust_ca;

impl Client {
    /// `nodes`: `tcp://host:port` of every node, or `tls://host:port` (see `trust_ca`).
    pub fn connect(nodes: &[impl AsRef<str>], token: &str) -> Result<Client> {
        let addrs = nodes
            .iter()
            .map(|a| a.as_ref().parse())
            .collect::<Result<Vec<_>>>()?;
        Ok(Client {
            mc: MeshClient::new(addrs, token.into())?,
        })
    }

    /// `connect`, with this long for a node to answer a request, where `connect` gives it
    /// 1.5 s: a node asked for one event of a large arena (`event`) reads the arena first. A
    /// request is given its bytes at 4 MB/s on top (a large append's batches: 4 s more for
    /// 16 MiB), and `Duration::MAX` waits for good.
    pub fn connect_with_timeout(
        nodes: &[impl AsRef<str>],
        token: &str,
        rpc_timeout: std::time::Duration,
    ) -> Result<Client> {
        let mut client = Self::connect(nodes, token)?;
        let mc = Arc::get_mut(&mut client.mc).context("a client just made is shared")?;
        mc.rpc_timeout = rpc_timeout;
        Ok(client)
    }

    /// How long an append is tried on the nodes in turn before it fails with its outcome
    /// unknown, where `connect` gives it 30 s (a gateway answers its own client sooner).
    pub fn set_append_deadline(&mut self, deadline: std::time::Duration) -> Result<()> {
        let mc = Arc::get_mut(&mut self.mc).context("a client in use is shared")?;
        mc.append_deadline = deadline;
        Ok(())
    }

    /// The node a stream's clients try first.
    fn preferred(&self, stream: &str) -> usize {
        (xxhash_rust::xxh3::xxh3_64(stream.as_bytes()) % self.mc.node_count() as u64) as usize
    }

    /// A control request to the first node that answers, from `start` round-robin.
    async fn control(&self, start: usize, c: &Control) -> Result<(usize, ControlResp)> {
        let n = self.mc.node_count();
        let mut last = None;
        for i in 0..n {
            let node = (start + i) % n;
            match self.mc.control(node, c).await {
                Ok(r) => return Ok((node, r)),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap()).context("no node answered")
    }

    /// The cluster as this client reaches it, for a request the SDK has no call for (a
    /// gateway asks the nodes what a tenant's key opens).
    pub fn mesh(&self) -> &Arc<MeshClient> {
        &self.mc
    }

    /// A writer log of `stream` named `writer`, with a new epoch (time-based) per producer.
    pub fn producer(&self, stream: &str, writer: &str) -> Producer {
        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);
        Producer {
            client: self.clone(),
            log: LogId {
                stream: stream.into(),
                writer: writer.into(),
                epoch,
            },
            next_seq: 0,
            events: vec![],
            bytes: 0,
            copies: self.mc.node_count().min(2) as u8,
            home: None,
            moved: Default::default(),
            in_flight: tokio::task::JoinSet::new(),
            flying: Default::default(),
            dedups: false,
            compress: true,
            chunked: false,
            extents: None,
            refused: None,
            failed: None,
        }
    }

    pub async fn consumer(&self, stream: &str, group: &str, start: Start) -> Result<Consumer> {
        let cursor = self.resolve(stream, group, start).await?;
        let mut c = self.consumer_at(stream, cursor);
        c.group = Some(group.into());
        Ok(c)
    }

    /// A consumer from a cursor the caller kept (brrrrr keeps it in its own checkpoints).
    pub fn consumer_at(&self, stream: &str, cursor: Cursor) -> Consumer {
        Consumer {
            client: self.clone(),
            stream: stream.into(),
            group: None,
            resume: cursor.clone(),
            cursor,
            node: self.preferred(stream),
            nodes: None,
            chunked: None,
            extents: None,
            refused: None,
        }
    }

    async fn resolve(&self, stream: &str, group: &str, start: Start) -> Result<Cursor> {
        let at = |time_ms| Control::CursorAt {
            stream: stream.into(),
            time_ms,
        };
        let c = match start {
            Start::Beginning => return Ok(Cursor::new()),
            Start::End => return self.end(stream).await,
            Start::Time(t) => at(t),
            Start::Committed { or } => {
                let q = Control::Position {
                    group: group.into(),
                    stream: stream.into(),
                };
                match self.control(self.preferred(stream), &q).await?.1 {
                    ControlResp::Cursor { cursor: Some(w) } => {
                        return Ok(cursor_from_wire(w, stream));
                    }
                    _ => return Box::pin(self.resolve(stream, group, *or)).await,
                }
            }
        };
        match self.control(self.preferred(stream), &c).await?.1 {
            ControlResp::Cursor { cursor: Some(w) } => Ok(cursor_from_wire(w, stream)),
            r => bail!("unexpected {r:?}"),
        }
    }

    /// Stores `cursor` as `group`'s position on `stream`: a reader that reads ahead of what it
    /// has made durable (the archiver, until its upload is acknowledged) commits the cursor it
    /// kept for the durable part instead of its consumer's.
    pub async fn commit(&self, group: &str, stream: &str, cursor: &Cursor) -> Result<()> {
        let c = Control::Commit {
            group: group.into(),
            stream: stream.into(),
            cursor: cursor_to_wire(cursor, stream),
        };
        match self.control(self.preferred(stream), &c).await?.1 {
            ControlResp::Committed => Ok(()),
            r => bail!("unexpected {r:?}"),
        }
    }

    /// Where `stream` ends: every acknowledged event is before this cursor. An acked batch is on
    /// `copies` (2) nodes, so one node's end can be short (a node just restarted, or repairing);
    /// the ends of any nodes - copies + 1 nodes, merged per writer log, include one of its copies
    /// (2 of 3 nodes, 3 of 4). A node that lost its disk can make the end short until repair:
    /// a reader starting there then reads a few acked events again (spec/mesh, EndCoversAcked).
    pub async fn end(&self, stream: &str) -> Result<Cursor> {
        // the stream's nodes are asked themselves: a node that does not hold the stream passes
        // the question on, and two such answers can be one node's end (spec/mesh/End.tla).
        // Nodes of a version that does not say where a stream is: an error, not a guess
        let its = self.mc.nodes_of(stream, self.preferred(stream)).await?;
        let its: Vec<usize> = its.into_iter().map(|n| n.0).collect();
        let n = its.len();
        let need = n - n.min(2) + 1; // the producers' copies: n.min(2)
        let q = Control::CursorAt {
            stream: stream.into(),
            time_ms: u64::MAX,
        };
        let (mut end, mut answered, mut last) = (Cursor::new(), 0, None);
        for node in its {
            match self.mc.control(node, &q).await {
                Ok(ControlResp::Cursor { cursor: Some(w) }) => {
                    for (log, seq) in cursor_from_wire(w, stream) {
                        let e = end.entry(log).or_insert(0);
                        *e = (*e).max(seq);
                    }
                    answered += 1;
                    if answered == need {
                        return Ok(end);
                    }
                }
                Ok(r) => last = Some(anyhow::anyhow!("unexpected {r:?}")),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| anyhow::anyhow!("no nodes")))
            .with_context(|| format!("the end of {stream} needs {need} nodes, {answered} answered"))
    }

    /// Deletes the stream: every writer log of it there is, on every node, for good. Returns
    /// the epoch it is deleted below (0: there was nothing of it to delete). A producer created
    /// after this (its epoch is the clock, above any before it) writes a new stream of the
    /// name; one created before is refused.
    pub async fn delete(&self, stream: &str) -> Result<u64> {
        let c = Control::Delete {
            stream: stream.into(),
        };
        let (_, r) = self.control(self.preferred(stream), &c).await?;
        r.deleted().with_context(|| format!("unexpected {r:?}"))
    }

    /// The names of the streams with `prefix` that hold something, in order. Not a snapshot:
    /// a stream created or deleted meanwhile may or may not be named.
    pub async fn streams(&self, prefix: &str) -> Result<Vec<String>> {
        self.mc.streams(prefix, 0).await
    }

    /// What the streams with `prefix` hold on each node (`MeshClient::stored`): the usage
    /// meter's sample.
    pub async fn stored(&self, prefix: &str) -> Vec<Option<(u64, u64)>> {
        self.mc.stored(prefix).await
    }

    /// One page of `streams`: at most `limit` names after `after`, and whether there are
    /// more (ask again with the last name as `after`).
    pub async fn streams_page(
        &self,
        prefix: &str,
        after: &str,
        limit: u32,
    ) -> Result<(Vec<String>, bool)> {
        let q = Control::Streams {
            prefix: prefix.into(),
            after: after.into(),
            limit,
            local: false,
        };
        match self.control(0, &q).await?.1 {
            ControlResp::Streams { names, more } => Ok((names, more)),
            r => bail!("unexpected {r:?}"),
        }
    }

    /// A live feed of `stream` from `cursor` (`MeshClient::tail`), through the first node
    /// that takes it: the stored batches as they come, for a reader that waits for the
    /// stream's next event and then polls a `Consumer`.
    pub async fn tail(&self, stream: &str, cursor: &Cursor) -> Result<crate::mesh::Tail> {
        let n = self.mc.node_count();
        let first = self.preferred(stream);
        let mut last = None;
        for i in 0..n {
            let wire = cursor_to_wire(cursor, stream);
            match self.mc.tail((first + i) % n, stream, wire).await {
                Ok(t) => return Ok(t),
                Err(e) => last = Some(e),
            }
        }
        Err(last.expect("at least one node")).context("no node took the feed")
    }

    /// One event of an extents stream, rebuilt by a node: for a reader that does not hold
    /// the stream's arena (a debugger opening one request). `seq` is the event's sequence
    /// number as a consumer yields it. `None`: retention took the event, or the start of the
    /// arena it is stored against. An event over about 16 MiB, or one whose arena is over
    /// `extents::REBUILD_BYTES`, is refused: read it with a consumer.
    pub async fn event(
        &self,
        stream: &str,
        writer: &str,
        epoch: u64,
        seq: u64,
    ) -> Result<Option<Bytes>> {
        let log = LogId {
            stream: stream.into(),
            writer: writer.into(),
            epoch,
        };
        let body = Bytes::from(serde_json::to_vec(&Control::ReadEvent { log, seq })?);
        let (n, start) = (self.mc.node_count(), self.preferred(stream));
        let mut last = anyhow::anyhow!("no node holds all of the event");
        // a node that does not hold all of it may be behind (an acked batch is on 2 of 3)
        for i in 0..n {
            match self
                .mc
                .call((start + i) % n, Kind::Control, body.clone())
                .await
            {
                Ok(f) if f.kind == Kind::Batches => return Ok(Some(f.body)),
                // gone for good; or this node does not hold all of it, and the next may
                Ok(f) => {
                    if let Ok(ControlResp::Event { gone: true }) = serde_json::from_slice(&f.body) {
                        return Ok(None);
                    }
                }
                Err(e) => last = e,
            }
        }
        Err(last).with_context(|| format!("{stream}/{writer} seq {seq}"))
    }

    /// A compacted stream's newest value of every key, by key.
    pub async fn latest(&self, stream: &str) -> Result<Vec<(Vec<u8>, Bytes)>> {
        match self
            .control(
                self.preferred(stream),
                &Control::Latest {
                    stream: stream.into(),
                },
            )
            .await?
            .1
        {
            ControlResp::Latest { entries } => Ok(entries
                .into_iter()
                .map(|(k, v)| (k, Bytes::from(v)))
                .collect()),
            r => bail!("unexpected {r:?}"),
        }
    }
}

/// Events of one writer log, appended in batches.
pub struct Producer {
    client: Client,
    log: LogId,
    next_seq: u64,
    events: Vec<(u64, Vec<u8>, Vec<u8>)>,
    bytes: usize,
    copies: u8,
    /// The node it sends to (`home`), once asked for; `moved`: ask again.
    home: Option<(usize, u32)>,
    moved: Arc<std::sync::atomic::AtomicBool>,
    /// Each batch in flight answers with the dedup ids it carried: `flying`, the ids of all of
    /// them, and `dedups`, whether this producer has sent any (`send` with an id).
    in_flight: tokio::task::JoinSet<(Vec<u64>, Result<()>)>,
    flying: std::collections::HashSet<u64>,
    dedups: bool,
    compress: bool,
    chunked: bool,
    /// An extents producer: the log's encoder, an event it refused (reported by the next
    /// `flush`), and the append that failed (the producer sends nothing after it).
    extents: Option<extents::Encoder>,
    refused: Option<anyhow::Error>,
    failed: Option<String>,
}

impl Producer {
    /// The producer's writer log, and the sequence its next event's first stored event
    /// takes: what names an event to `Client::event`.
    pub fn position(&self) -> (&LogId, u64) {
        (&self.log, self.next_seq + self.events.len() as u64)
    }

    /// Payloads go as one lz4 block per batch (the default: about a third of the bytes to send,
    /// store and replicate, at ~0.2 us/record of client CPU); the broker never decompresses.
    pub fn set_compression(&mut self, on: bool) {
        self.compress = on;
    }

    /// Sends events of any size up to `MAX_EVENT_BYTES`: each event carries a one-byte tag,
    /// and one larger than `CHUNK_BYTES` goes as several events of the stream, which a
    /// consumer that is `set_chunked` too puts back together (one that is not sees the tagged
    /// chunks as events). Every producer and consumer of the stream must agree; set it before
    /// the first `send`. Not for compacted streams: a `put` is tagged and never cut.
    pub fn set_chunked(&mut self, on: bool) {
        self.chunked = on;
    }

    /// Stores each event as what it adds to what this producer's writer log already holds (an
    /// agent's request that repeats the conversation costs what is new in it): see
    /// [`extents`]. Every producer and consumer of the stream must agree; set it before the
    /// first `send`. It includes `set_chunked`. What it changes for the caller:
    ///
    /// - **No dedup ids and no `put`**: an event the broker drops or compacts away takes with
    ///   it bytes that later events name. Both are refused: the next `flush` reports it.
    /// - **After an append failed** (`flush` or `sync` returned its error) the producer sends
    ///   nothing more: what it encoded since names bytes the stream may not hold, and the
    ///   broker serves nothing of a writer log past a missing batch. Events sent since the
    ///   last `sync` that returned `Ok` are unknown, as without this mode; a new producer
    ///   (a new writer log, a new arena) sends them again.
    /// - **Memory**: the producer holds its arena, up to `Extents::arena_bytes` and one
    ///   event's new bytes, about 0.4 times that for its index, and its last event
    ///   (`extent_bytes`). `park` lets go of it.
    /// - **Retention**: a trim that cuts this writer log inside an arena leaves the rest of
    ///   that arena's events unreadable (readers pass over them): up to
    ///   `Extents::reset_events - 1` events past the trim.
    pub fn set_extents(&mut self, on: Option<extents::Extents>) {
        self.extents = on.map(extents::Encoder::new);
        self.chunked |= self.extents.is_some();
    }

    /// The bytes an extents producer holds for its writer log (0: none).
    pub fn extent_bytes(&self) -> usize {
        self.extents.as_ref().map_or(0, |e| e.bytes())
    }

    /// An extents producer lets go of its arena: its next event starts a new one. For a
    /// stream that has gone idle (a producer does not notice by itself).
    pub fn park(&mut self) {
        if let Some(e) = self.extents.as_mut() {
            e.park();
        }
    }

    /// An event this producer cannot send in its mode: the next `flush` says so.
    fn refuse(&mut self, why: &str) {
        let log = &self.log;
        let e = anyhow::anyhow!(
            "{}/{}: {why}; the event was not sent",
            log.stream,
            log.writer
        );
        self.refused = self.refused.take().or(Some(e));
    }

    /// Buffers an event with its dedup id (live-ingestion's `redpanda-dedup-key`; empty: never
    /// deduplicated), sent as its 8-byte hash. `flush` sends it; flush when `is_full`.
    pub fn send(&mut self, time_us: u64, dedup_id: &[u8], payload: &[u8]) {
        let key = dedup_key(dedup_id);
        self.dedups |= !key.is_empty();
        if self.extents.is_some() {
            if self.failed.is_some() {
                return; // the next `flush` says so
            }
            if !key.is_empty() {
                return self.refuse("an extents stream takes no dedup ids");
            }
            // (the event's first chunk gets the next sequence number)
            let seq = self.next_seq + self.events.len() as u64;
            match self.extents.as_mut().unwrap().encode(seq, payload) {
                Ok(stored) => {
                    (chunks(&stored, CHUNK_BYTES)).for_each(|c| self.push(time_us, &[], c))
                }
                Err(e) => self.refuse(&e.to_string()),
            }
            return;
        }
        if !self.chunked {
            return self.push(time_us, &key, payload.to_vec());
        }
        // the id is its first chunk's: the others have none, so the broker's dedup does not
        // take them for copies of the first (a duplicate event loses its first chunk, and a
        // consumer passes over the rest)
        for (i, chunk) in chunks(payload, CHUNK_BYTES).enumerate() {
            self.push(time_us, if i == 0 { &key } else { &[] }, chunk);
        }
    }

    /// Buffers an event of a compacted stream: `key` is its compaction key, kept as is.
    pub fn put(&mut self, time_us: u64, key: &[u8], payload: &[u8]) {
        if self.extents.is_some() {
            return self.refuse("an extents stream is not compacted");
        }
        match self.chunked {
            true => self.push(time_us, key, [&[WHOLE][..], payload].concat()),
            false => self.push(time_us, key, payload.to_vec()),
        }
    }

    fn push(&mut self, time_us: u64, key: &[u8], payload: Vec<u8>) {
        self.bytes += 14 + key.len() + payload.len();
        self.events.push((time_us, key.to_vec(), payload));
    }

    /// Batches sent and not yet acknowledged.
    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }

    /// Waits until every batch sent so far is acknowledged (durable on `copies` nodes).
    pub async fn sync(&mut self) -> Result<()> {
        self.flush().await?;
        while let Some(r) = self.in_flight.join_next().await {
            self.acked(r?)?;
        }
        Ok(())
    }

    /// An append's outcome, with the dedup ids its batch carried: they are in flight no
    /// longer, whatever became of it. An extents producer whose append failed sends nothing
    /// more.
    fn acked(&mut self, (ids, r): (Vec<u64>, Result<()>)) -> Result<()> {
        for id in &ids {
            self.flying.remove(id);
        }
        if r.is_err() && self.extents.is_some() {
            self.failed = r.as_ref().err().map(|e| format!("{e:#}"));
            self.events.clear();
            self.bytes = 0;
        }
        r
    }

    pub fn is_full(&self) -> bool {
        self.events.len() >= MAX_BATCH_EVENTS || self.bytes >= MAX_BATCH_BYTES
    }

    /// Sends the buffered events as one batch, keeping up to `MAX_IN_FLIGHT` batches waiting
    /// for their acks (it waits for the oldest beyond that); `sync` waits for all of them. A
    /// failed append is retried with the same bytes, which is idempotent.
    ///
    /// A chunked producer sends what is buffered as batches of about `MAX_BATCH_BYTES` (one
    /// event's chunks fill several), in order; the others send it as one batch, as before.
    pub async fn flush(&mut self) -> Result<()> {
        if let Some(e) = self.refused.take() {
            return Err(e);
        }
        if let Some(e) = &self.failed {
            let log = &self.log;
            bail!(
                "{}/{}: an append failed ({e}); this producer sends nothing more",
                log.stream,
                log.writer
            );
        }
        while !self.events.is_empty() {
            let mut n = self.events.len();
            if self.chunked {
                let mut bytes = 0;
                let sizes = self.events.iter().map(|e| 14 + e.1.len() + e.2.len());
                let fits = sizes.take_while(|size| {
                    bytes += size;
                    bytes <= MAX_BATCH_BYTES
                });
                n = fits.count().max(1);
            }
            // A node drops the copy of an id that reaches it second, and batches in flight
            // reach it in any order: a batch that repeats an id of one in flight (the
            // application's retry of an event) waits for it, so the first one sent is the one
            // kept and the events stay in the order they were sent.
            let ids: Vec<u64> = match self.dedups {
                true => (self.events[..n].iter())
                    .filter_map(|e| Some(u64::from_le_bytes(e.1[..].try_into().ok()?)))
                    .collect(),
                false => vec![],
            };
            let most = match ids.iter().any(|id| self.flying.contains(id)) {
                true => 1,
                false => MAX_IN_FLIGHT,
            };
            while self.in_flight.len() >= most {
                let r = self.in_flight.join_next().await.unwrap()?;
                self.acked(r)?;
            }
            self.flying.extend(&ids);
            let evs: Vec<(u64, &[u8], &[u8])> = (self.events[..n].iter())
                .map(|(t, k, p)| (*t, &k[..], &p[..]))
                .collect();
            let b = if self.compress {
                Batch::encode_compressed(&self.log, self.next_seq, &evs)?
            } else {
                Batch::encode_keyed(&self.log, self.next_seq, &evs)?
            };
            let (mc, copies) = (self.client.mc.clone(), self.copies);
            let again = self.moved.swap(false, std::sync::atomic::Ordering::AcqRel);
            let to = match self.home.filter(|_| !again) {
                Some(to) => to,
                None => {
                    let hashed = self.client.preferred(&self.log.stream);
                    home(mc.nodes_of(&self.log.stream, hashed).await, hashed)
                }
            };
            self.home = Some(to);
            let gone = self.moved.clone();
            self.in_flight.spawn(async move {
                let r = mc.append(to.0, &b, copies).await.map(|r| {
                    if moved(to, r.node, r.ack.nodes) {
                        gone.store(true, std::sync::atomic::Ordering::Release);
                    }
                });
                (ids, r)
            });
            self.next_seq += n as u64;
            self.events.drain(..n);
        }
        self.bytes = 0;
        Ok(())
    }
}

/// About the most bytes of events an extents consumer rebuilds in one `poll` (and one event
/// whatever its size): `max_bytes` bounds the stored bytes a poll looks at, and an event stored
/// as a few bytes rebuilds to megabytes. Twice the largest read of stored bytes a node gives,
/// so a stream that shares nothing reads as fast as before.
pub const POLL_REBUILT_BYTES: usize = 16 << 20;

/// A reader of one stream.
pub struct Consumer {
    client: Client,
    stream: String,
    group: Option<String>,
    cursor: Cursor,
    node: usize,
    /// The nodes it reads from (`readers`), and until when they are good for (`NODES_AGE`).
    nodes: Option<(Vec<usize>, std::time::Instant)>,
    /// A chunked consumer: each writer log's event in the making, the position a reader may
    /// resume from (no later than the start of an event not yet yielded), and an error to
    /// report at the next poll.
    chunked: Option<std::collections::HashMap<LogId, Assembly>>,
    /// An extents consumer: each writer log's arena and event in the making.
    extents: Option<extents::Readers>,
    resume: Cursor,
    refused: Option<anyhow::Error>,
}

impl Consumer {
    /// Puts back together the events a chunked producer cut (`Producer::set_chunked`): set it
    /// before the first `poll`. `cursor` and `commit` then give a position at the start of an
    /// event, so a consumer that resumes reads the event it was inside from its first chunk.
    /// An event over `MAX_EVENT_BYTES`, or one without a chunk tag (the stream is not chunked),
    /// is passed over and reported as an error by the next `poll`.
    pub fn set_chunked(&mut self, on: bool) {
        self.chunked = on.then(Default::default);
        self.resume = self.cursor.clone();
    }

    /// Reads the events an extents producer stored (`Producer::set_extents`): set it before
    /// the first `poll`. Wherever the consumer starts, it yields whole events: one in the
    /// middle of an arena first reads the log again from the arena's start (at most
    /// `Extents::reset_events` events, or `Extents::arena_bytes`), and `cursor` and `commit`
    /// give the position after the last event yielded, as without this mode. Events whose
    /// arena's start retention has trimmed are passed over (`passed_over` counts them); a
    /// damaged event, or one that is not of this mode, is an error of the next `poll`, and
    /// its log is read on from its next reset. It holds each writer log's arena and last
    /// event, at most about `extents::READER_BYTES` over the stream's logs.
    pub fn set_extents(&mut self, on: bool) {
        self.extents = on.then(Default::default);
        self.resume = self.cursor.clone();
    }

    /// Events an extents consumer passed over.
    pub fn passed_over(&self) -> u64 {
        self.extents.as_ref().map_or(0, |r| r.passed_over)
    }

    /// The next events (about `max_bytes` of batches), per writer in sequence order; empty when
    /// caught up. Fails over to the next node on an error.
    pub async fn poll(&mut self, max_bytes: u32) -> Result<Vec<Event>> {
        // a batch can hold no event for this reader (dedup dropped them all, or the cursor is
        // past them): read on, so an empty poll still means caught up
        if let Some(e) = self.refused.take() {
            return Err(e);
        }
        loop {
            match self.poll_batches(max_bytes).await? {
                Some(evs) if evs.is_empty() => continue,
                evs => return Ok(evs.unwrap_or_default()),
            }
        }
    }

    /// One read: None when no node had a batch past the cursor.
    async fn poll_batches(&mut self, max_bytes: u32) -> Result<Option<Vec<Event>>> {
        let req = Control::ReadCursor {
            stream: self.stream.clone(),
            cursor: cursor_to_wire(&self.cursor, &self.stream),
            max_bytes,
        };
        let body = Bytes::from(serde_json::to_vec(&req)?);
        let mc = self.client.mc.clone();
        // (until when they are good for: no later than that)
        let now = std::time::Instant::now();
        let good = |n: &(Vec<usize>, std::time::Instant)| n.1.checked_duration_since(now).is_some();
        let nodes = match self.nodes.take().filter(good) {
            Some(nodes) => nodes,
            None => {
                let its = mc.nodes_of(&self.stream, self.node).await;
                let its = readers(its, mc.node_count(), self.node);
                (its, now + NODES_AGE)
            }
        };
        let (mut last, mut answered) = (None, false);
        // a node with nothing past the cursor may be behind (an acked batch is on 2 of 3): caught
        // up only when none of the stream's nodes has more. ponytail: an empty poll asks each of
        // them; ask the merged end (`Client::end`) first if tailing readers' empty polls cost
        // too much
        for &node in &nodes.0 {
            match mc.call(node, Kind::Control, body.clone()).await {
                Ok(f) if f.kind == Kind::Batches => {
                    answered = true;
                    if let Some(evs) = self.take(f.body)? {
                        self.node = node;
                        self.nodes = Some(nodes);
                        return Ok(Some(evs));
                    }
                }
                Ok(f) => last = Some(anyhow::anyhow!("unexpected {:?}", f.kind)),
                Err(e) => last = Some(e),
            }
        }
        // (a node that did not answer may be one the stream has left: asked for again)
        self.nodes = last.is_none().then_some(nodes);
        if answered {
            return Ok(None);
        }
        Err(last.unwrap()).context("no node answered")
    }

    fn take(&mut self, mut body: Bytes) -> Result<Option<Vec<Event>>> {
        if body.remaining() < 4 {
            return Ok(None);
        }
        let mut out = vec![];
        // writer logs an extents reader goes back in: the rest of this read is not theirs
        let mut rewound = std::collections::HashSet::new();
        let mut room = POLL_REBUILT_BYTES;
        while body.remaining() >= 4 {
            let len = body.get_u32_le() as usize;
            anyhow::ensure!(body.remaining() >= len, "truncated batches");
            let b = Batch::parse(body.split_to(len))?;
            if rewound.contains(&b.id.log) {
                continue;
            }
            let from = self.cursor.get(&b.id.log).copied().unwrap_or(0);
            let events = b.try_events()?.into_iter().filter(|e| e.seq >= from);
            self.cursor.insert(b.id.log.clone(), b.end_seq());
            if let Some(readers) = self.extents.as_mut() {
                let back = readers.batch(&b.id.log, events, &mut out, &mut room);
                if let Some(to) = back {
                    self.cursor.insert(b.id.log.clone(), to);
                    rewound.insert(b.id.log.clone());
                }
                // (a reader that goes back for an arena resumes where it did before)
                if back.is_none() || room == 0 {
                    let at = readers.resume(&b.id.log).unwrap_or(b.end_seq());
                    // (a consumer that starts past a batch's last event reads none of it)
                    let resume = self.resume.entry(b.id.log.clone()).or_insert(0);
                    *resume = at.max(*resume);
                }
                self.refused = self.refused.take().or(readers.take_error());
                if room == 0 {
                    // rebuilt enough: the batches after this one were not looked at, and the
                    // next poll reads them
                    break;
                }
                continue;
            }
            let Some(logs) = self.chunked.as_mut() else {
                out.extend(events);
                continue;
            };
            let a = logs.entry(b.id.log.clone()).or_default();
            for e in events {
                match a.push(e) {
                    Ok(whole) => out.extend(whole),
                    Err(e) => self.refused = self.refused.take().or(Some(e)),
                }
            }
            let at = a.boundary().unwrap_or(b.end_seq());
            self.resume.insert(b.id.log.clone(), at);
        }
        Ok(Some(out))
    }

    /// Where the consumer is: a position valid on every node.
    pub fn cursor(&self) -> &Cursor {
        match self.chunked.is_some() || self.extents.is_some() {
            true => &self.resume,
            false => &self.cursor,
        }
    }

    /// Where the consumer's reads stand: past the chunks it holds of an event not yet whole,
    /// which `cursor` is not. For waiting on the stream's next batch (`Client::tail` from
    /// here wakes for a batch the consumer has not read); never a position to resume from.
    pub fn read_position(&self) -> &Cursor {
        &self.cursor
    }

    /// Stores the position as the group's (after the caller has made what it read durable).
    pub async fn commit(&self) -> Result<()> {
        let group = self
            .group
            .as_deref()
            .context("a consumer_at has no group to commit to")?;
        self.client.commit(group, &self.stream, self.cursor()).await
    }
}

/// Where a producer sends and a consumer reads, as properties of the answers a cluster gives
/// about a stream's nodes.
#[cfg(test)]
mod props {
    use super::*;
    use proptest::prelude::*;

    /// Why: a failed append says nothing of the batches still in flight. Forgetting their ids
    /// with it let the application's re-send of an event (most likely right after an error)
    /// leave beside the batch carrying its first copy, and reach the node first.
    #[tokio::test]
    async fn a_failed_batch_frees_its_own_ids_and_no_others() {
        let mut p = Client::connect(&["tcp://127.0.0.1:1"], "tok")
            .unwrap()
            .producer("s", "w");
        p.flying.extend([1, 2, 3]);
        assert!(p.acked((vec![2], Err(anyhow::anyhow!("down")))).is_err());
        assert_eq!(p.flying, [1, 3].into());
        assert!(p.acked((vec![1], Ok(()))).is_ok());
        assert_eq!(p.flying, [3].into());
    }

    proptest! {
        /// Why: a producer sends to the stream's first node, so the append costs no hop and a
        /// stream's producers meet one dedup index; it must notice when that is no longer
        /// where its appends are taken (the node is down, or the stream moved off it), and
        /// nodes that do not say where a stream is must be written to as before.
        #[test]
        fn a_producer_s_home_is_the_stream_s_first_node_until_another_takes_its_appends(
            its in prop::collection::vec((0usize..8, 0u32..32), 0..4), hashed in 0usize..8,
            answered in 0usize..8, holders: u32, says: bool,
        ) {
            let to = home(if says { Ok(its.clone()) } else { Err(anyhow::anyhow!("old")) }, hashed);
            match its.first().filter(|_| says) {
                Some(first) => prop_assert_eq!(to, *first),
                None => prop_assert_eq!(to, (hashed, u32::MAX)),
            }
            let elsewhere = answered != to.0;
            let not_a_holder = to.1 < 32 && holders >> to.1 & 1 == 0;
            prop_assert_eq!(moved(to, answered, holders), elsewhere || not_a_holder);
        }

        /// Why: a reader is caught up only when none of the stream's nodes has more, so it
        /// must ask each of them once, and the one it last read from first (it has the next
        /// batch more often than not); nodes that do not say are all asked, as before.
        #[test]
        fn a_reader_asks_each_of_the_stream_s_nodes_once_from_the_one_it_last_read(
            its in prop::collection::btree_set(0usize..8, 0..5), n in 1usize..8, last in 0usize..8, says: bool,
        ) {
            let its: Vec<(usize, u32)> = its.into_iter().map(|i| (i, i as u32)).collect();
            let got = readers(if says { Ok(its.clone()) } else { Err(anyhow::anyhow!("old")) }, n, last);
            let want: Vec<usize> = if says { its.iter().map(|n| n.0).collect() } else { (0..n).collect() };
            let mut sorted = got.clone();
            sorted.sort();
            prop_assert_eq!(&sorted, &want);
            if want.contains(&last) {
                prop_assert_eq!(got[0], last);
            } else {
                prop_assert_eq!(&got, &want);
            }
        }
    }
}
