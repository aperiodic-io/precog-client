//! Multiplexed request/response over one ordered byte stream (a TCP socket).
//! Many requests can be in flight;
//! responses are matched by `req_id`. The writer task coalesces queued frames
//! into one write, so pipelined small requests cost few syscalls.

use crate::proto::{Frame, Kind, MAX_FRAME_BYTES};
use anyhow::{Result, anyhow, bail, ensure};
use bytes::{Buf, BytesMut};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

/// What a request is given to be answered: `base`, and on top the request's bytes at 4 MB/s: a
/// 16 MiB batch, read, checked and synced, gets 4 s more. Not its answer's, whose size is not
/// known before it comes (`Node::peer_raw`). Nodes, clients and the gateway all use it, so a
/// caller does not give up on a large append the node is still within its time for; a
/// `Duration::MAX` base stays `MAX`. ponytail: one floor rate for every link; measured per link
/// if a slower one shows.
pub fn deadline(base: Duration, bytes: usize) -> Duration {
    base.saturating_add(Duration::from_micros(bytes as u64 / 4))
}

pub type BoxRead = Box<dyn AsyncRead + Unpin + Send>;
pub type BoxWrite = Box<dyn AsyncWrite + Unpin + Send>;

/// Reads one frame. `Ok(None)` on clean EOF at a frame boundary.
pub async fn read_frame<R: AsyncRead + Unpin>(
    r: &mut R,
    buf: &mut BytesMut,
) -> Result<Option<Frame>> {
    read_frame_max(r, buf, MAX_FRAME_BYTES).await
}

/// Whether a frame may state the length `len` where frames are at most `max` bytes: checked
/// before any of it is reserved, so a length costs its sender's connection, not memory.
pub fn frame_len_ok(len: usize, max: usize) -> bool {
    (9..=max.min(MAX_FRAME_BYTES)).contains(&len)
}

/// `read_frame` where a frame is at most `max` bytes (a connection's hello: nothing says yet
/// who sends it).
pub async fn read_frame_max<R: AsyncRead + Unpin>(
    r: &mut R,
    buf: &mut BytesMut,
    max: usize,
) -> Result<Option<Frame>> {
    loop {
        if buf.len() >= 4 {
            let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
            ensure!(frame_len_ok(len, max), "bad frame length {len}");
            if buf.len() >= 4 + len {
                buf.advance(4);
                let mut f = buf.split_to(len);
                let kind = Kind::from_u8(f[0])?;
                let req_id = u64::from_le_bytes(f[1..9].try_into().unwrap());
                f.advance(9);
                return Ok(Some(Frame {
                    kind,
                    req_id,
                    body: f.freeze(),
                }));
            }
            buf.reserve(4 + len - buf.len());
        } else {
            buf.reserve(64 * 1024);
        }
        if r.read_buf(buf).await? == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            bail!("connection closed mid-frame");
        }
    }
}

/// A budget of bytes held in memory for others (requests being served, answers queued for a
/// socket): `admit` waits until its bytes fit, so whoever sends them is held back instead of the
/// node growing. More bytes than the whole budget cost all of it: they pass alone.
#[derive(Clone)]
pub struct Meter {
    room: Arc<Semaphore>,
    limit: u32,
}

impl Meter {
    pub fn new(limit: usize) -> Meter {
        // ponytail: a permit a byte, so at most 4 GiB a meter; a coarser unit if one needs more
        let limit = limit.clamp(1, u32::MAX as usize) as u32;
        Meter {
            room: Arc::new(Semaphore::new(limit as usize)),
            limit,
        }
    }

    fn cost(&self, bytes: usize) -> u32 {
        bytes.min(self.limit as usize) as u32
    }

    /// Room for `bytes`, once there is: held until the permit is dropped (or `release`d, for
    /// one `forget`ed on its way to the socket). None: the meter is closed (its connection's
    /// socket is gone), and whoever waited for room there is let go.
    pub async fn admit(&self, bytes: usize) -> Option<OwnedSemaphorePermit> {
        let room = self.room.clone().acquire_many_owned(self.cost(bytes));
        room.await.ok()
    }

    fn release(&self, bytes: usize) {
        self.room.add_permits(self.cost(bytes) as usize);
    }

    /// The bytes admitted and not yet released.
    pub fn held(&self) -> usize {
        self.limit as usize - self.room.available_permits()
    }
}

/// A connection's way out: frames queued for its socket, counted against its meter from before
/// they are made (`room`) until they are written (`write_loop`). None: not counted (a client's
/// own connection, tests).
#[derive(Clone)]
pub struct Out {
    tx: mpsc::Sender<Frame>,
    meter: Option<Meter>,
}

impl From<mpsc::Sender<Frame>> for Out {
    fn from(tx: mpsc::Sender<Frame>) -> Out {
        Out { tx, meter: None }
    }
}

impl Out {
    pub fn metered(tx: mpsc::Sender<Frame>, meter: Meter) -> Out {
        Out {
            tx,
            meter: Some(meter),
        }
    }

    /// Room for an answer of at most `bytes`, taken before the answer is read or built: while
    /// the socket takes nothing, whoever would answer waits here holding nothing.
    pub async fn room(&self, bytes: usize) -> Option<OwnedSemaphorePermit> {
        self.meter.as_ref()?.admit(bytes).await
    }

    /// Queues the frame: within `room` if it fits there (the rest of it is given back), else
    /// once the meter has room. False: the connection is gone.
    pub async fn send(&self, f: Frame, room: &mut Option<OwnedSemaphorePermit>) -> bool {
        if let Some(m) = &self.meter {
            let cost = m.cost(f.body.len()) as usize;
            match room.as_mut().and_then(|r| r.split(cost)) {
                Some(part) => part.forget(),
                None => {
                    *room = None; // (given back first: the frame may need all of the meter)
                    match m.admit(cost).await {
                        Some(part) => part.forget(),
                        None => return false,
                    }
                }
            }
        }
        self.tx.send(f).await.is_ok()
    }

    pub async fn closed(&self) {
        self.tx.closed().await
    }
}

/// Writer task: drains `rx`, coalescing all immediately available frames.
pub async fn write_loop<W: AsyncWrite + Unpin>(w: W, rx: mpsc::Receiver<Frame>) -> Result<()> {
    write_loop_metered(w, rx, None).await
}

/// `write_loop` for a connection whose frames were counted by `Out::send`: each is released
/// once written.
pub async fn write_loop_metered<W: AsyncWrite + Unpin>(
    w: W,
    rx: mpsc::Receiver<Frame>,
    meter: Option<Meter>,
) -> Result<()> {
    let r = write_frames(w, rx, &meter).await;
    // the socket is gone: whoever waits for room to queue more for it is let go
    if let Some(m) = &meter {
        m.room.close();
    }
    r
}

async fn write_frames<W: AsyncWrite + Unpin>(
    mut w: W,
    mut rx: mpsc::Receiver<Frame>,
    meter: &Option<Meter>,
) -> Result<()> {
    let mut out = BytesMut::with_capacity(256 * 1024);
    let mut sizes = vec![];
    while let Some(f) = rx.recv().await {
        sizes.push(f.body.len());
        f.encode_into(&mut out);
        while out.len() < 4 * 1024 * 1024 {
            match rx.try_recv() {
                Ok(f) => {
                    sizes.push(f.body.len());
                    f.encode_into(&mut out)
                }
                Err(_) => break,
            }
        }
        w.write_all(&out).await?;
        w.flush().await?;
        out.clear();
        for n in sizes.drain(..) {
            if let Some(m) = meter {
                m.release(n);
            }
        }
    }
    let _ = w.shutdown().await;
    Ok(())
}

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Frame>>>>;
type Subs = Arc<Mutex<HashMap<u64, mpsc::Sender<Frame>>>>;

/// Client side of a multiplexed connection.
pub struct RpcClient {
    tx: mpsc::Sender<Frame>,
    pending: Pending,
    subs: Subs,
    next_id: AtomicU64,
    closed: Arc<AtomicBool>,
    /// The connection's writer and reader: a client dropped closes its connection both ways
    /// (a reader left running would keep a feed, and the node's task that feeds it).
    tasks: [tokio::task::AbortHandle; 2],
}

impl Drop for RpcClient {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

impl RpcClient {
    pub fn start(r: BoxRead, w: BoxWrite) -> RpcClient {
        let (tx, rx) = mpsc::channel::<Frame>(4096);
        let pending: Pending = Default::default();
        let subs: Subs = Default::default();
        let closed = Arc::new(AtomicBool::new(false));
        let writer = {
            let closed = closed.clone();
            tokio::spawn(async move {
                if let Err(e) = write_loop(w, rx).await {
                    tracing::debug!("rpc writer ended: {e}");
                }
                closed.store(true, Ordering::Release);
            })
        };
        let reader = {
            let pending = pending.clone();
            let subs = subs.clone();
            let closed = closed.clone();
            tokio::spawn(async move {
                let mut r = r;
                let mut buf = BytesMut::new();
                loop {
                    match read_frame(&mut r, &mut buf).await {
                        Ok(Some(f)) if f.kind == Kind::Push => {
                            let id = f.req_id;
                            let tx = subs.lock().get(&id).cloned();
                            if let Some(tx) = tx {
                                // Backpressure: a slow subscriber stalls this
                                // connection's reader, and TCP flow
                                // control then stalls the server's push.
                                if tx.send(f).await.is_err() {
                                    subs.lock().remove(&id);
                                }
                            }
                        }
                        Ok(Some(f)) => {
                            let call = pending.lock().remove(&f.req_id);
                            if let Some(tx) = call {
                                let _ = tx.send(f);
                            } else if f.kind == Kind::Error {
                                // a feed the node refused: its answer, then its end (the
                                // subscriber is told at once, not by a timeout)
                                let sub = subs.lock().remove(&f.req_id);
                                if let Some(tx) = sub {
                                    let _ = tx.send(f).await;
                                }
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            tracing::debug!("rpc reader ended: {e}");
                            break;
                        }
                    }
                }
                closed.store(true, Ordering::Release);
                pending.lock().clear(); // wakes all waiters with an error
                subs.lock().clear(); // ends all subscriptions
            })
        };
        RpcClient {
            tx,
            pending,
            subs,
            next_id: AtomicU64::new(1),
            closed,
            tasks: [writer.abort_handle(), reader.abort_handle()],
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Opens a server-push stream: sends one request and returns the
    /// receiver of every `Push` frame carrying its req_id. The stream ends
    /// when the connection closes.
    pub async fn subscribe(
        &self,
        kind: Kind,
        body: bytes::Bytes,
        buffer: usize,
    ) -> Result<mpsc::Receiver<Frame>> {
        if self.is_closed() {
            bail!("connection closed");
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(buffer);
        self.subs.lock().insert(id, tx);
        self.tx
            .send(Frame::new(kind, id, body))
            .await
            .map_err(|_| anyhow!("connection closed"))?;
        Ok(rx)
    }

    /// A request and its answer; the node's `Kind::Error` answer is an error here.
    pub async fn call(&self, kind: Kind, body: bytes::Bytes, timeout: Duration) -> Result<Frame> {
        Self::answer(self.raw(kind, body, timeout).await?)
    }

    /// A node's answer, or the error it answered with.
    pub fn answer(f: Frame) -> Result<Frame> {
        if f.kind == Kind::Error {
            bail!("remote error: {}", String::from_utf8_lossy(&f.body));
        }
        Ok(f)
    }

    /// A request and its answer as the node sent it, `Kind::Error` included (a node that
    /// passes the answer on does not read it): an error here is the connection's.
    pub async fn raw(&self, kind: Kind, body: bytes::Bytes, timeout: Duration) -> Result<Frame> {
        if self.is_closed() {
            bail!("connection closed");
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().insert(id, tx);
        let res = tokio::time::timeout(timeout, async {
            self.tx
                .send(Frame::new(kind, id, body))
                .await
                .map_err(|_| anyhow!("connection closed"))?;
            rx.await.map_err(|_| anyhow!("connection closed"))
        })
        .await;
        match res {
            Ok(r) => r,
            Err(_) => {
                self.pending.lock().remove(&id);
                bail!("rpc timeout after {timeout:?}")
            }
        }
    }
}

/// The frame reader faces the network: any byte stream, cut into any reads, yields the frames
/// written to it in order and then a clean end, or an error, never a panic.
#[cfg(test)]
mod props {
    use super::*;
    use proptest::prelude::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// Hands out the stream in the given read sizes.
    struct Chunks(Vec<u8>, Vec<usize>, usize);

    impl AsyncRead for Chunks {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            out: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let Chunks(b, sizes, at) = &mut *self;
            let n = sizes.get(*at).copied().unwrap_or(usize::MAX);
            *at += 1;
            let n = n.max(1).min(out.remaining()).min(b.len());
            out.put_slice(&b[..n]);
            b.drain(..n);
            Poll::Ready(Ok(()))
        }
    }

    /// Why: a live feed ends with its connection, and a node that carries another node's feed
    /// for a client ends it by dropping its connection to that node. A dropped client that
    /// went on reading would keep the feed, and its node's task, for good.
    #[tokio::test]
    async fn a_client_dropped_closes_its_connection_both_ways() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (ours, theirs) = tokio::io::duplex(1 << 16);
        let (r, w) = tokio::io::split(ours);
        let client = RpcClient::start(Box::new(r), Box::new(w));
        let (mut r, mut w) = tokio::io::split(theirs);
        let mut frame = BytesMut::new();
        Frame::new(Kind::Push, 7, bytes::Bytes::from_static(b"x")).encode_into(&mut frame);
        w.write_all(&frame).await.unwrap(); // (read while the client lives)
        drop(client);
        let closed = async {
            let mut buf = [0u8; 16];
            assert_eq!(r.read(&mut buf).await.unwrap(), 0, "it writes no more");
            // and it reads no more: the far end's writes fail, which ends a feed's task there
            while w.write_all(&frame).await.is_ok() {
                tokio::task::yield_now().await;
            }
        };
        let closed = tokio::time::timeout(Duration::from_secs(5), closed).await;
        closed.expect("the connection outlives its client");
    }

    fn read_all(bytes: Vec<u8>, sizes: Vec<usize>) -> (Vec<Frame>, Result<()>) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut r = Chunks(bytes, sizes, 0);
            let mut buf = BytesMut::new();
            let mut got = vec![];
            loop {
                match read_frame(&mut r, &mut buf).await {
                    Ok(Some(f)) => got.push(f),
                    Ok(None) => return (got, Ok(())),
                    Err(e) => return (got, Err(e)),
                }
            }
        })
    }

    proptest! {
        /// Why: a fixed deadline gave a large batch the time of a small one, and a busy peer
        /// answered it `Unknown` (`props_deadline` in tests/cluster.rs). The deadline grows with
        /// the bytes, never shrinks, and a small request keeps the base within a millisecond.
        #[test]
        fn a_deadline_grows_with_the_bytes_and_a_small_request_keeps_the_base(
            base_ms in 0u64..10_000,
            a in 0usize..(64 << 20),
            b in 0usize..(64 << 20),
        ) {
            let base = Duration::from_millis(base_ms);
            let (lo, hi) = (a.min(b), a.max(b));
            prop_assert!(deadline(base, lo) <= deadline(base, hi));
            prop_assert_eq!(deadline(base, 0), base);
            prop_assert!(deadline(base, lo % 4096) < base + Duration::from_millis(2));
            // 4 MB/s: 16 MiB gets about 4 s more
            prop_assert_eq!(deadline(base, 16 << 20) - base, Duration::from_micros(4 << 20));
            // (a caller that waits for good: `Client::connect_with_timeout(Duration::MAX)`)
            prop_assert_eq!(deadline(Duration::MAX, hi), Duration::MAX);
        }

        #[test]
        fn frames_read_back_in_order_however_the_stream_is_cut(
            frames in prop::collection::vec(
                (1u8..=10, any::<u64>(), prop::collection::vec(any::<u8>(), 0..64)), 0..8),
            sizes in prop::collection::vec(1usize..40, 0..64),
            cut in any::<prop::sample::Index>(),
        ) {
            let mut b = BytesMut::new();
            for (k, id, body) in &frames {
                Frame::new(Kind::from_u8(*k).unwrap(), *id, body.clone().into()).encode_into(&mut b);
            }
            let (got, end) = read_all(b.to_vec(), sizes.clone());
            prop_assert!(end.is_ok());
            prop_assert_eq!(got.len(), frames.len());
            for (f, (k, id, body)) in got.iter().zip(&frames) {
                prop_assert_eq!((f.kind as u8, f.req_id, &f.body[..]), (*k, *id, &body[..]));
            }
            // a stream cut inside a frame is an error after the frames before the cut
            let c = cut.index(b.len() + 1);
            let whole = frames.iter().scan(0, |at, f| { *at += 13 + f.2.len(); Some(*at) })
                .filter(|&e| e <= c).count();
            let (got, end) = read_all(b[..c].to_vec(), sizes);
            prop_assert_eq!(got.len(), whole);
            let boundary = c == 0 || frames.iter().scan(0, |at, f| { *at += 13 + f.2.len(); Some(*at) }).any(|e| e == c);
            prop_assert_eq!(end.is_ok(), boundary);
        }

        #[test]
        fn any_bytes_read_as_frames_or_an_error(
            bytes in prop::collection::vec(any::<u8>(), 0..256),
            sizes in prop::collection::vec(1usize..40, 0..16),
        ) {
            let _ = read_all(bytes, sizes);
        }

        /// Why: a frame's length is its sender's claim. One over the limit must be refused
        /// on the claim alone, before the node reserves or waits for a byte of it; under a
        /// lower limit (a hello, from nobody yet) the same.
        #[test]
        fn a_frame_over_the_limit_is_refused_on_its_length_alone(
            len in prop_oneof![0usize..32, any::<u32>().prop_map(|x| x as usize),
                (0usize..64).prop_map(|d| MAX_FRAME_BYTES - 32 + d)],
            max in prop_oneof![9usize..4096, Just(MAX_FRAME_BYTES), Just(usize::MAX)],
            sent in 0usize..16,
        ) {
            let ok = len >= 9 && len <= max && len <= MAX_FRAME_BYTES;
            prop_assert_eq!(frame_len_ok(len, max), ok);
            // the length and a few bytes of the frame, then nothing: refused at once, or the
            // reader waits for the rest having reserved no more than the frame it was promised
            let mut b = (len as u32).to_le_bytes().to_vec();
            b.extend(std::iter::repeat_n(1u8, sent.min(len)));
            let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
            let (r, cap) = rt.block_on(async {
                let mut r = Chunks(b, vec![], 0);
                let mut buf = BytesMut::new();
                let r = read_frame_max(&mut r, &mut buf, max).await;
                (r, buf.capacity())
            });
            if !ok {
                prop_assert!(r.unwrap_err().to_string().contains("bad frame length"));
                prop_assert!(cap <= 64 * 1024 + 16, "{} bytes reserved for a refused frame", cap);
            } else if sent >= len {
                prop_assert!(r.unwrap().is_some());
            } else {
                prop_assert!(r.is_err(), "closed mid-frame");
                prop_assert!(cap <= 2 * (len + 4) + 64 * 1024);
            }
        }

        /// Why: a meter is what stands between a sender and the node's memory. If it ever
        /// admits more than its limit the node grows; if it admits less than fits, or loses
        /// room when a permit is given back, senders stall for good.
        #[test]
        fn a_meter_never_admits_more_than_its_limit_and_gives_all_of_it_back(
            limit in 1usize..64,
            ops in prop::collection::vec((0usize..96, any::<prop::sample::Index>(), any::<bool>()), 1..40),
        ) {
            use futures::FutureExt;
            let m = Meter::new(limit);
            let mut held: Vec<(OwnedSemaphorePermit, usize)> = vec![];
            for (bytes, which, give_back) in ops {
                if give_back && !held.is_empty() {
                    drop(held.remove(which.index(held.len())));
                } else {
                    let cost = bytes.min(limit);
                    let fits = held.iter().map(|h| h.1).sum::<usize>() + cost <= limit;
                    match m.admit(bytes).now_or_never().map(|p| p.expect("open")) {
                        Some(p) => {
                            prop_assert!(fits, "{} bytes admitted over the limit", bytes);
                            held.push((p, cost));
                        }
                        None => prop_assert!(!fits, "{} bytes that fit were not admitted", bytes),
                    }
                }
                prop_assert_eq!(m.held(), held.iter().map(|h| h.1).sum::<usize>());
                prop_assert!(m.held() <= limit);
            }
            held.clear();
            prop_assert_eq!(m.held(), 0);
            // more than the whole limit passes alone
            prop_assert!(m.admit(limit + 1).now_or_never().is_some());
        }

        /// Why: what a connection has queued for its socket is counted from the moment its
        /// room is taken until it is written. A frame queued and not counted is memory a
        /// reader that stops reading gets for free; a frame written and still counted is a
        /// connection that stalls with an idle socket.
        #[test]
        fn a_connections_queue_holds_no_more_than_its_meter_until_the_socket_takes_it(
            limit in 8usize..64,
            frames in prop::collection::vec((0usize..80, prop::option::of(0usize..80)), 1..24),
        ) {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                let meter = Meter::new(limit);
                let (tx, rx) = mpsc::channel(64);
                let out = Out::metered(tx, meter.clone());
                // a socket that takes nothing until told to
                let (w, mut r) = tokio::io::duplex(1);
                let (m, n) = (meter.clone(), frames.len());
                let sender = tokio::spawn(async move {
                    for (len, room) in frames {
                        let mut room = match room {
                            Some(r) => {
                                // room is taken before the answer is made: the meter holds it
                                let got = out.room(r).await.expect("room, on a metered connection");
                                assert_eq!(got.num_permits(), r.min(limit));
                                assert!(m.held() >= got.num_permits());
                                Some(got)
                            }
                            None => None,
                        };
                        assert!(m.held() <= limit);
                        assert!(out.send(Frame::new(Kind::Push, 1, vec![0u8; len].into()), &mut room).await);
                        assert!(m.held() <= limit);
                    }
                });
                let writer = tokio::spawn(write_loop_metered(w, rx, Some(meter.clone())));
                // stalled: the sender stops once the meter is full, and holds no more
                tokio::time::sleep(Duration::from_millis(5)).await;
                assert!(meter.held() <= limit);
                // read: every frame arrives, and everything is given back
                let mut buf = BytesMut::new();
                for _ in 0..n {
                    assert!(read_frame(&mut r, &mut buf).await.unwrap().is_some());
                }
                sender.await.unwrap();
                drop(r);
                let _ = writer.await;
                assert_eq!(meter.held(), 0);
            });
        }

        /// Why: a handler or a feed that waits for room on a connection whose socket has died
        /// would wait for good, with whatever it holds: when the socket goes, the wait ends.
        #[test]
        fn a_sender_waiting_for_room_is_let_go_when_the_socket_is_gone(
            limit in 8usize..32,
            len in 8usize..64,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                let meter = Meter::new(limit);
                let (tx, rx) = mpsc::channel(64);
                let out = Out::metered(tx, meter.clone());
                let (w, r) = tokio::io::duplex(1);
                let writer = tokio::spawn(write_loop_metered(w, rx, Some(meter)));
                let sender = tokio::spawn(async move {
                    // more than the meter and the socket take: the last ones wait
                    let mut sent = 0;
                    while out.send(Frame::new(Kind::Push, 1, vec![0u8; len].into()), &mut None).await {
                        sent += 1;
                        assert!(sent < 1000, "the socket took nothing, yet nothing waited");
                    }
                    assert!(out.room(len).await.is_none());
                });
                tokio::time::sleep(Duration::from_millis(5)).await;
                assert!(!sender.is_finished(), "it waits for room");
                drop(r);
                assert!(writer.await.unwrap().is_err());
                tokio::time::timeout(Duration::from_secs(5), sender).await.expect("let go").unwrap();
            });
        }
    }
}
