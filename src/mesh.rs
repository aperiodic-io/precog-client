//! Client: any node accepts any append. A `Writer` owns one writer log
//! `(stream, writer, epoch)` and assigns sequence numbers itself, so retrying
//! the *same bytes* on another node after a timeout is always safe: the mesh
//! either already has them (Duplicate) or stores them now.

use crate::proto::*;
use crate::rpc::RpcClient;
use crate::transport::Addr;
use anyhow::{Result, bail};
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The node a request that starts at node `start` of `n` asks `i`-th: `start`, then each
/// other node once.
fn turn(start: usize, i: usize, n: usize) -> usize {
    (start + i) % n
}

/// A node the client reaches: its address and the connection to it.
struct Link {
    addr: Addr,
    conn: tokio::sync::Mutex<Option<Arc<RpcClient>>>,
}

pub struct MeshClient {
    /// The nodes the client was given, then those it learned of (`nodes_of`): a node keeps
    /// its index.
    links: parking_lot::RwLock<Vec<Arc<Link>>>,
    token: String,
    /// A request is given this, and its bytes at 4 MB/s on top (`rpc::deadline`), as a node
    /// gives its peers.
    pub rpc_timeout: Duration,
    /// Total time an append keeps retrying across nodes.
    pub append_deadline: Duration,
}

#[derive(Debug, Clone)]
pub struct Receipt {
    pub ack: Ack,
    pub node: usize,
    pub attempts: u32,
}

impl MeshClient {
    pub fn new(nodes: Vec<Addr>, token: String) -> Result<Arc<MeshClient>> {
        anyhow::ensure!(!nodes.is_empty(), "no nodes");
        let link = |addr| {
            Arc::new(Link {
                addr,
                conn: Default::default(),
            })
        };
        Ok(Arc::new(MeshClient {
            links: parking_lot::RwLock::new(nodes.into_iter().map(link).collect()),
            token,
            rpc_timeout: Duration::from_millis(1500),
            append_deadline: Duration::from_secs(30),
        }))
    }

    pub fn node_count(&self) -> usize {
        self.links.read().len()
    }

    /// The index of the node at `addr`: one the client has, or a new one.
    fn node_at(&self, addr: Addr) -> usize {
        let mut links = self.links.write();
        let at = links.iter().position(|l| l.addr == addr);
        at.unwrap_or_else(|| {
            links.push(Arc::new(Link {
                addr,
                conn: Default::default(),
            }));
            links.len() - 1
        })
    }

    /// The stream's nodes, in the order a request for it tries them, as the first node that
    /// answers has them (`Control::Nodes`), from `start` round-robin: each one's index here
    /// and its id. A request sent to one of them is answered by it, not passed on: an append
    /// costs no hop, and two answers are two nodes' (`Client::end`). The client learns of
    /// nodes it was not given this way. An error: no node answered, or the nodes are of a
    /// version that does not say.
    pub async fn nodes_of(&self, stream: &str, start: usize) -> Result<Vec<(usize, u32)>> {
        let q = Control::Nodes {
            stream: stream.into(),
        };
        let n = self.node_count();
        let mut last = anyhow::anyhow!("no nodes");
        for i in 0..n {
            let asked = turn(start, i, n);
            match self.control(asked, &q).await {
                Ok(ControlResp::Nodes { nodes }) => {
                    let at = |m: &Member| match m.addr.is_empty() {
                        true => Ok(asked), // (the node itself, under its config's list)
                        false => m.addr.parse().map(|a| self.node_at(a)),
                    };
                    return nodes.iter().map(|m| Ok((at(m)?, m.id))).collect();
                }
                r => last = r.map_or_else(|e| e, |r| anyhow::anyhow!("unexpected {r:?}")),
            }
        }
        Err(last.context(format!("no node says where stream {stream:?} is")))
    }

    /// Every stream with `prefix` that holds something, in order (`Control::Streams`, paged
    /// through), as the first node that answers from `start` has them.
    pub async fn streams(&self, prefix: &str, start: usize) -> Result<Vec<String>> {
        let (mut all, mut after) = (vec![], String::new());
        loop {
            let q = Control::Streams {
                prefix: prefix.into(),
                after: after.clone(),
                limit: 1000,
                local: false,
            };
            let n = self.node_count();
            // (the first node's reason: a node that answered says why there is no list, a
            // node that is down only that it is down)
            let mut why = None;
            let mut page = None;
            for i in 0..n {
                match self.control(turn(start, i, n), &q).await {
                    Ok(ControlResp::Streams { names, more }) => {
                        page = Some((names, more));
                        break;
                    }
                    r => {
                        let e = r.map_or_else(|e| e, |r| anyhow::anyhow!("unexpected {r:?}"));
                        why.get_or_insert(e);
                    }
                }
            }
            let Some((names, more)) = page else {
                let why = why.unwrap_or_else(|| anyhow::anyhow!("no nodes"));
                return Err(why.context(format!("no node lists the streams of {prefix:?}")));
            };
            after = names.last().cloned().unwrap_or(after);
            all.extend(names);
            if !more {
                return Ok(all);
            }
        }
    }

    /// What the streams with `prefix` hold on each node, and how many hold something
    /// (`Control::Stored`), by node; None for a node that did not say (down, or the token does
    /// not read the prefix).
    pub async fn stored(&self, prefix: &str) -> Vec<Option<(u64, u64)>> {
        let q = Control::Stored {
            prefix: prefix.into(),
        };
        let asked = (0..self.node_count()).map(|i| self.control(i, &q));
        let said = futures::future::join_all(asked).await;
        let stored = |r| match r {
            Ok(ControlResp::Stored { streams, bytes }) => Some((streams, bytes)),
            _ => None,
        };
        said.into_iter().map(stored).collect()
    }

    /// A new connection to node `i`, past its hello.
    async fn dial(&self, i: usize) -> Result<Arc<RpcClient>> {
        let addr = self.links.read()[i].addr.clone();
        let c = crate::transport::connect(&addr, self.rpc_timeout).await?;
        let hello = Hello {
            token: self.token.clone(),
            node_id: None,
        };
        c.call(
            Kind::Hello,
            Bytes::from(serde_json::to_vec(&hello)?),
            self.rpc_timeout,
        )
        .await?;
        Ok(Arc::new(c))
    }

    async fn link(&self, i: usize) -> Result<Arc<RpcClient>> {
        let link = self.links.read()[i].clone();
        let mut g = link.conn.lock().await;
        if let Some(c) = g.as_ref()
            && !c.is_closed()
        {
            return Ok(c.clone());
        }
        let c = self.dial(i).await?;
        *g = Some(c.clone());
        Ok(c)
    }

    /// A request to one node and its answer as the node gave it, a refusal (`Kind::Error`)
    /// included: for a caller that passes the answer on (`gateway`). An error is a node that
    /// did not answer.
    pub async fn raw(&self, node: usize, kind: Kind, body: Bytes) -> Result<Frame> {
        let c = self.link(node).await?;
        // (a request's bytes: an answer's size is not known here)
        let t = crate::rpc::deadline(self.rpc_timeout, body.len());
        let r = c.raw(kind, body, t).await;
        if r.is_err() {
            let link = self.links.read()[node].clone();
            let mut g = link.conn.lock().await;
            if g.as_ref().is_some_and(|x| Arc::ptr_eq(x, &c)) {
                *g = None;
            }
        }
        r
    }

    /// Opens a feed on node `node` over a connection of its own (`sub`: an encoded
    /// `Subscribe`), and returns the connection with the node's frames: the feed ends on the
    /// node when the connection is dropped.
    pub async fn feed(
        &self,
        node: usize,
        sub: Bytes,
    ) -> Result<(Arc<RpcClient>, tokio::sync::mpsc::Receiver<Frame>)> {
        let c = self.dial(node).await?;
        let rx = c.subscribe(Kind::Subscribe, sub, 4).await?;
        Ok((c, rx))
    }

    pub async fn call(&self, node: usize, kind: Kind, body: Bytes) -> Result<Frame> {
        let c = self.link(node).await?;
        // (a request's bytes: an answer's size is not known here)
        let t = crate::rpc::deadline(self.rpc_timeout, body.len());
        let r = c.call(kind, body, t).await;
        if r.is_err() {
            // Drop the link on any failure so a hung connection is not reused.
            let link = self.links.read()[node].clone();
            let mut g = link.conn.lock().await;
            if g.as_ref().is_some_and(|x| Arc::ptr_eq(x, &c)) {
                *g = None;
            }
        }
        r
    }

    pub async fn control(&self, node: usize, c: &Control) -> Result<ControlResp> {
        let f = self
            .call(node, Kind::Control, Bytes::from(serde_json::to_vec(c)?))
            .await?;
        anyhow::ensure!(f.kind == Kind::ControlResp, "unexpected {:?}", f.kind);
        Ok(serde_json::from_slice(&f.body)?)
    }

    /// Appends pre-encoded batch bytes, starting at `preferred` node and
    /// failing over round-robin on transport errors, timeouts or
    /// `Unknown` outcomes until `append_deadline`. `Refused` (a conflict, a rejection) says
    /// that nothing of the batch is stored, so it is said only when no earlier attempt of
    /// this call could have stored it: after a node that answered `Unknown`, or one that
    /// was sent the batch and did not answer, a later rejection is an outcome unknown.
    pub async fn append(&self, preferred: usize, batch: &Bytes, copies: u8) -> Result<Receipt> {
        let body = encode_append(copies, batch);
        let start = Instant::now();
        let mut node = preferred % self.node_count();
        let mut attempts = 0;
        let mut last_err = String::new();
        let mut unknown = false;
        loop {
            attempts += 1;
            // (a node that cannot be reached was not sent the batch)
            let sent = match self.link(node).await {
                Ok(_) => Some(self.call(node, Kind::Append, body.clone()).await),
                Err(e) => {
                    last_err = e.to_string();
                    None
                }
            };
            match sent.map(|f| f.and_then(|f| parse_frame_ack(&f))) {
                None => {}
                Some(Ok(ack)) if ack.is_success() => {
                    return Ok(Receipt {
                        ack,
                        node,
                        attempts,
                    });
                }
                Some(Ok(ack))
                    if matches!(ack.status, AckStatus::Conflict | AckStatus::Rejected) =>
                {
                    if unknown {
                        bail!(
                            "append outcome unknown after {attempts} attempts: {last_err}, then {ack:?}"
                        );
                    }
                    return Err(Refused(ack).into());
                }
                Some(Ok(ack)) => (last_err, unknown) = (format!("{ack:?}"), true),
                Some(Err(e)) => (last_err, unknown) = (e.to_string(), true),
            }
            if start.elapsed() > self.append_deadline {
                bail!("append outcome unknown after {attempts} attempts: {last_err}");
            }
            node = (node + 1) % self.node_count();
            if (attempts as usize).is_multiple_of(self.node_count()) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

impl MeshClient {
    /// Opens a live subscription on one node. Delivers every batch that
    /// becomes durable on that node, per writer log in sequence order.
    pub async fn subscribe(
        &self,
        node: usize,
        stream: Option<&str>,
        from_start: bool,
    ) -> Result<Subscription> {
        let c = self.link(node).await?;
        let body = serde_json::to_vec(&Subscribe {
            stream: stream.map(str::to_owned),
            from_start,
            cursor: None,
        })?;
        let mut rx = c
            .subscribe(Kind::Subscribe, Bytes::from(body), 1024)
            .await?;
        // The node's empty push: the feed starts no later than this.
        let start = tokio::time::timeout(self.rpc_timeout, rx.recv()).await;
        if let Ok(Some(f)) = &start
            && f.kind == Kind::Error
        {
            bail!(
                "node {node} refused the subscription: {}",
                String::from_utf8_lossy(&f.body)
            );
        }
        anyhow::ensure!(
            matches!(&start, Ok(Some(f)) if f.body.is_empty()),
            "node {node} did not confirm the subscription"
        );
        Ok(Subscription {
            rx,
            from_start,
            next: Default::default(),
            held: Default::default(),
            ready: Default::default(),
            _conn: c,
        })
    }
}

impl MeshClient {
    /// Sends a subscription as it is and returns the node's frames: the first is its
    /// confirmation (an empty push) or its refusal.
    pub async fn subscribe_raw(
        &self,
        node: usize,
        sub: &Subscribe,
    ) -> Result<tokio::sync::mpsc::Receiver<Frame>> {
        let c = self.link(node).await?;
        let body = Bytes::from(serde_json::to_vec(sub)?);
        c.subscribe(Kind::Subscribe, body, 1024).await
    }

    /// Opens a live feed of `stream` on one node from `cursor` (empty: the stream's start):
    /// every batch a cursor read from there gives, then every batch after, per writer log in
    /// sequence (`Subscribe::cursor`). `Tail::cursor` is the position after the last batch
    /// taken: a reader that lost its feed opens one again from it, on any node. The feed has
    /// a connection of its own, as `feed`'s: a `Tail` dropped ends it on the node (on the
    /// client's shared connection it would stay until the client went).
    pub async fn tail(&self, node: usize, stream: &str, cursor: WireCursor) -> Result<Tail> {
        let c = self.dial(node).await?;
        let body = serde_json::to_vec(&Subscribe {
            stream: Some(stream.into()),
            from_start: false,
            cursor: Some(cursor.clone()),
        })?;
        let mut rx = c
            .subscribe(Kind::Subscribe, Bytes::from(body), 1024)
            .await?;
        let start = tokio::time::timeout(self.rpc_timeout, rx.recv()).await;
        if let Ok(Some(f)) = &start
            && f.kind == Kind::Error
        {
            bail!(
                "node {node} refused the feed: {}",
                String::from_utf8_lossy(&f.body)
            );
        }
        anyhow::ensure!(
            matches!(&start, Ok(Some(f)) if f.body.is_empty()),
            "node {node} did not confirm the feed"
        );
        let at = (cursor.into_iter())
            .map(|(writer, epoch, next)| ((writer, epoch), next))
            .collect();
        Ok(Tail { rx, at, _conn: c })
    }
}

/// An append the cluster refused (`AckStatus::Conflict`, `Rejected`): nothing of it is
/// stored, where any other failed append may be (its outcome is not known).
#[derive(Debug)]
pub struct Refused(pub crate::proto::Ack);

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "append refused: {:?}", self.0)
    }
}

impl std::error::Error for Refused {}

/// Client end of a live feed from a cursor (`MeshClient::tail`).
pub struct Tail {
    rx: tokio::sync::mpsc::Receiver<Frame>,
    /// Per writer log (writer, epoch): the sequence after the last batch taken.
    at: std::collections::BTreeMap<(String, u64), u64>,
    _conn: Arc<RpcClient>,
}

impl Tail {
    /// The next batch; `None` when the feed ended (its connection or its node went: open
    /// another from `cursor`).
    pub async fn next(&mut self) -> Option<Result<Batch>> {
        let f = self.rx.recv().await?;
        if f.kind == Kind::Error {
            return Some(Err(anyhow::anyhow!(
                "feed ended: {}",
                String::from_utf8_lossy(&f.body)
            )));
        }
        Some(Batch::parse(f.body).inspect(|b| {
            let log = (b.id.log.writer.clone(), b.id.log.epoch);
            self.at.insert(log, b.end_seq());
        }))
    }

    /// The position after the last batch `next` gave.
    pub fn cursor(&self) -> WireCursor {
        (self.at.iter())
            .map(|((writer, epoch), next)| (writer.clone(), *epoch, *next))
            .collect()
    }
}

/// Client end of a live feed. Restores per-log order if a node received a
/// log's batches out of order (possible after repair), and drops batches it
/// has already delivered.
pub struct Subscription {
    rx: tokio::sync::mpsc::Receiver<Frame>,
    /// A replay must see every log from sequence 0; a live feed joins each
    /// log at the first batch it sees.
    from_start: bool,
    next: std::collections::HashMap<LogId, u64>,
    held: std::collections::HashMap<LogId, std::collections::BTreeMap<u64, Batch>>,
    ready: std::collections::VecDeque<Batch>,
    _conn: Arc<RpcClient>,
}

impl Subscription {
    /// Next in-order batch; `None` when the connection closed.
    pub async fn next(&mut self) -> Option<Result<Batch>> {
        loop {
            if let Some(b) = self.ready.pop_front() {
                return Some(Ok(b));
            }
            let f = self.rx.recv().await?;
            if f.kind == Kind::Error {
                return Some(Err(anyhow::anyhow!(
                    "subscription refused: {}",
                    String::from_utf8_lossy(&f.body)
                )));
            }
            let b = match Batch::parse(f.body) {
                Ok(b) => b,
                Err(e) => return Some(Err(e)),
            };
            self.accept(b);
        }
    }

    fn accept(&mut self, b: Batch) {
        let log = b.id.log.clone();
        let start = if self.from_start { 0 } else { b.id.first_seq };
        let next = self.next.entry(log.clone()).or_insert(start);
        if b.end_seq() <= *next {
            return; // already delivered
        }
        if b.id.first_seq != *next {
            // A writer's new epoch may start anywhere, but within an epoch
            // a gap means an earlier batch is still on its way.
            self.held.entry(log).or_default().insert(b.id.first_seq, b);
            return;
        }
        *next = b.end_seq();
        self.ready.push_back(b);
        if let Some(h) = self.held.get_mut(&log) {
            while let Some(e) = h.first_entry() {
                let n = self.next[&log];
                if *e.key() < n && e.get().end_seq() <= n {
                    e.remove();
                } else if *e.key() == n {
                    let b = e.remove();
                    self.next.insert(log.clone(), b.end_seq());
                    self.ready.push_back(b);
                } else {
                    break;
                }
            }
        }
    }
}

/// Single-writer handle for one writer log.
pub struct Writer {
    pub log: LogId,
    pub next_seq: u64,
    pub preferred: usize,
    pub copies: u8,
    client: Arc<MeshClient>,
}

impl Writer {
    pub fn new(
        client: Arc<MeshClient>,
        stream: &str,
        writer: &str,
        epoch: u64,
        preferred: usize,
        copies: u8,
    ) -> Writer {
        Writer {
            log: LogId {
                stream: stream.into(),
                writer: writer.into(),
                epoch,
            },
            next_seq: 0,
            preferred,
            copies,
            client,
        }
    }

    pub fn encode(&self, events: &[(u64, &[u8])]) -> Result<Bytes> {
        Batch::encode(&self.log, self.next_seq, events)
    }

    /// Appends one batch; on success advances the sequence and sticks to the
    /// node that answered (so failover is sticky).
    pub async fn append_encoded(&mut self, bytes: &Bytes, count: usize) -> Result<Receipt> {
        let r = self
            .client
            .append(self.preferred, bytes, self.copies)
            .await?;
        self.preferred = r.node;
        self.next_seq += count as u64;
        Ok(r)
    }

    pub async fn append(&mut self, events: &[(u64, &[u8])]) -> Result<Receipt> {
        let b = self.encode(events)?;
        self.append_encoded(&b, events.len()).await
    }
}

/// A node's answer to an append: an ack frame, decoded.
fn parse_frame_ack(f: &Frame) -> Result<Ack> {
    if f.kind != Kind::Ack {
        bail!("expected ack, got {:?}", f.kind);
    }
    Ack::decode(f.body.clone())
}

#[cfg(test)]
mod props {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// Why: a request that names a node asks that node first (its caller chose it: the
        /// stream's own node, or the one that answered last), and must reach every other node
        /// before it gives up, each once: a node asked twice is a node not asked.
        #[test]
        fn a_request_asks_its_node_first_then_every_other_once(n in 1usize..33, start in 0usize..64) {
            let start = start % n;
            let asked: Vec<usize> = (0..n).map(|i| turn(start, i, n)).collect();
            prop_assert_eq!(asked[0], start);
            let mut each = asked.clone();
            each.sort_unstable();
            prop_assert_eq!(each, (0..n).collect::<Vec<_>>());
        }
    }
}
