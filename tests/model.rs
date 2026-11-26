//! Model-based test of the whole path (SDK, mesh client, RPC, node, store, WAL) on a 3-node
//! cluster of the published node image (`testkit`). Ported from precog's
//! `crates/client/tests/model.rs`, the same models, with one difference: a node that joins
//! repairs on its own (every 200 ms, waited for), where precog's in-process test ran its
//! repair pass and checked it. The repair itself is precog's to test, not the client's: two producer replicas send the same events with dedup ids,
//! a compacted stream takes puts, and readers read from the beginning, resume from a committed
//! position, start at the end and ask for the latest values, against a model of what was acked,
//! with one node down from the start until it joins and repairs.

mod model {
    use precog_client::testkit;
    use precog_client::{Client, Event, Start};
    use proptest::prelude::*;
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};

    #[derive(Clone, Debug)]
    enum Op {
        /// producer replica 0 or 1 sends these dedup ids (flushed, not awaited)
        Send(u8, Vec<u8>),
        /// a compacted stream's key gets a value
        Put(u8, u8),
        ReadAll,
        /// read part, commit, and resume from the committed position on a new consumer
        Resume(u32),
        End,
        Latest,
        /// the node that was down starts and repairs from its peers
        Join,
    }

    fn ops() -> impl Strategy<Value = Vec<Op>> {
        prop::collection::vec(
            prop_oneof![
                6 => (0u8..2, prop::collection::vec(0u8..24, 1..6)).prop_map(|(p, ids)| Op::Send(p, ids)),
                2 => (0u8..4, any::<u8>()).prop_map(|(k, v)| Op::Put(k, v)),
                1 => Just(Op::ReadAll),
                1 => (64u32..512).prop_map(Op::Resume),
                1 => Just(Op::End),
                1 => Just(Op::Latest),
                1 => Just(Op::Join),
            ],
            1..24,
        )
    }

    /// Three nodes; `down` is not started (it refuses connections) until `Op::Join`, and then
    /// repairs from its peers every 200 ms.
    struct Cluster {
        addrs: Vec<String>,
        c: testkit::Cluster,
    }

    impl Cluster {
        async fn new(_dir: &std::path::Path, down: Option<usize>) -> Cluster {
            let mut o = testkit::Options::nodes(3)
                .up(&(0..3).filter(|&i| Some(i) != down).collect::<Vec<_>>())
                .set("rpc_timeout_ms", json!(300))
                .set("repair_interval_ms", json!(1_000_000))
                .set("segment_size", json!(1 << 20))
                // a class whose streams keep 150 KB (the extents tests' retention)
                .stream("short/*", json!({"retention_bytes": 150_000}))
                .stream("state", json!({"compacted": true}));
            o.token = "tok".into();
            if let Some(i) = down {
                o = o.set_on(i, "repair_interval_ms", json!(200));
            }
            let c = tokio::task::spawn_blocking(move || testkit::Cluster::new(o))
                .await
                .unwrap()
                .unwrap();
            Cluster {
                addrs: c.nodes.clone(),
                c,
            }
        }

        /// Starts node `i` and gives it a few of its repair passes.
        async fn start(&mut self, i: usize) {
            tokio::task::block_in_place(|| self.c.start(i)).unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        }
    }

    /// A commit, asked again until it is acknowledged, as a caller does: the test's peers
    /// time out after 300 ms, and on a busy machine a replicate of the position can take
    /// longer (the commit is then not acknowledged; committing the same position again is).
    async fn commit(c: &precog_client::Consumer) {
        let mut r = c.commit().await;
        for _ in 0..50 {
            if r.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            r = c.commit().await;
        }
        r.unwrap();
    }

    async fn drain(c: &mut precog_client::Consumer) -> Vec<Event> {
        let mut out = vec![];
        loop {
            let evs = c.poll(4096).await.unwrap();
            if evs.is_empty() {
                return out;
            }
            out.extend(evs);
        }
    }

    fn ids(evs: &[Event]) -> Vec<String> {
        evs.iter()
            .map(|e| String::from_utf8(e.payload.to_vec()).unwrap())
            .collect()
    }

    /// Each acked id once, nothing that was never sent, each writer's events in sequence.
    fn exactly_once(evs: &[Event], acked: &BTreeSet<String>) -> Result<(), TestCaseError> {
        let got = ids(evs);
        let set: BTreeSet<String> = got.iter().cloned().collect();
        prop_assert_eq!(got.len(), set.len(), "an id read twice: {:?}", got);
        prop_assert_eq!(&set, acked);
        for w in ["a", "b"] {
            let seqs: Vec<u64> = evs
                .iter()
                .filter(|e| e.writer == w)
                .map(|e| e.seq)
                .collect();
            prop_assert!(
                seqs.windows(2).all(|p| p[0] < p[1]),
                "writer {} out of order: {:?}",
                w,
                seqs
            );
        }
        Ok(())
    }

    async fn run(mut down: Option<usize>, ops: Vec<Op>) -> Result<(), TestCaseError> {
        let dir = tempfile::tempdir().unwrap();
        let mut cluster = Cluster::new(dir.path(), down).await;
        let client = Client::connect(&cluster.addrs, "tok").unwrap();
        let mut ps = [client.producer("raw", "a"), client.producer("raw", "b")];
        let mut state = client.producer("state", "s");
        let mut acked: BTreeSet<String> = BTreeSet::new();
        let mut latest: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut t = 1u64;
        for op in ops {
            match op {
                Op::Send(p, evs) => {
                    for id in evs {
                        t += 1;
                        let id = format!("e{id}");
                        ps[p as usize].send(t, id.as_bytes(), id.as_bytes());
                        acked.insert(id);
                    }
                    ps[p as usize].flush().await.unwrap();
                    continue;
                }
                Op::Put(k, v) => {
                    t += 1;
                    state.put(t, &[k], &[v]);
                    state.sync().await.unwrap();
                    latest.insert(vec![k], vec![v]);
                    continue;
                }
                _ => {}
            }
            for p in &mut ps {
                p.sync().await.unwrap();
            }
            match op {
                Op::ReadAll => {
                    let mut c = client
                        .consumer("raw", "all", Start::Beginning)
                        .await
                        .unwrap();
                    exactly_once(&drain(&mut c).await, &acked)?;
                }
                Op::Resume(max) => {
                    let g = format!("g{t}");
                    let mut c = client.consumer("raw", &g, Start::Beginning).await.unwrap();
                    let mut got = c.poll(max).await.unwrap();
                    commit(&c).await;
                    let mut r = client
                        .consumer(
                            "raw",
                            &g,
                            Start::Committed {
                                or: Box::new(Start::End),
                            },
                        )
                        .await
                        .unwrap();
                    got.extend(drain(&mut r).await);
                    exactly_once(&got, &acked)?;
                }
                Op::End => {
                    let mut c = client.consumer("raw", "end", Start::End).await.unwrap();
                    prop_assert!(drain(&mut c).await.is_empty(), "events past the end");
                }
                Op::Join => {
                    if let Some(i) = down.take() {
                        cluster.start(i).await;
                    }
                }
                Op::Latest => {
                    let got: BTreeMap<Vec<u8>, Vec<u8>> = client
                        .latest("state")
                        .await
                        .unwrap()
                        .into_iter()
                        .map(|(k, v)| (k, v.to_vec()))
                        .collect();
                    prop_assert_eq!(&got, &latest);
                }
                Op::Send(..) | Op::Put(..) => unreachable!(),
            }
        }
        Ok(())
    }

    /// Why: a deleted stream must read as empty through the SDK on a cluster, with a node down
    /// or not, while the name stays usable: what a producer created after the delete sends is
    /// read, what one that wrote before it sends is refused.
    async fn delete(down: Option<usize>, before: u8, after: u8) -> Result<(), TestCaseError> {
        let dir = tempfile::tempdir().unwrap();
        let c = Cluster::new(dir.path(), down).await;
        let client = Client::connect(&c.addrs, "tok").unwrap();
        let mut old = client.producer("sessions/1", "w");
        let mut kept = client.producer("sessions/2", "w");
        for i in 0..before {
            old.send(i as u64, &[], &[i]);
            kept.send(i as u64, &[], &[i]);
        }
        old.sync().await.unwrap();
        kept.sync().await.unwrap();
        // (asked again until acknowledged: a replica can be slower than the RPC timeout)
        let mut floor = client.delete("sessions/1").await;
        for _ in 0..50 {
            if floor.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            floor = client.delete("sessions/1").await;
        }
        let floor = floor.unwrap();
        let all = |stream: &'static str| {
            let client = client.clone();
            async move { drain(&mut client.consumer_at(stream, Default::default())).await }
        };
        prop_assert_eq!(all("sessions/1").await.len(), 0);
        prop_assert_eq!(all("sessions/2").await.len(), before as usize);
        // the list names what holds something: not the deleted stream
        let kept: Vec<String> = (before > 0)
            .then(|| "sessions/2".into())
            .into_iter()
            .collect();
        prop_assert_eq!(client.streams("sessions/").await.unwrap(), kept.clone());
        // the producer from before the delete is refused, if the stream held anything of it
        // (a writer no node had seen yet writes the new stream); one created now is not
        old.send(99, &[], b"late");
        prop_assert_eq!(old.sync().await.is_err(), before > 0);
        prop_assert_eq!(floor > 0, before > 0);
        let mut new = client.producer("sessions/1", "w");
        for i in 0..after {
            new.send(i as u64, &[], &[i]);
        }
        new.sync().await.unwrap();
        let got = all("sessions/1").await;
        prop_assert_eq!(got.len(), after as usize + (before == 0) as usize);
        prop_assert!(got.iter().all(|e| e.epoch >= floor));
        let mut listed: Vec<String> = (!got.is_empty())
            .then(|| "sessions/1".into())
            .into_iter()
            .collect();
        listed.extend(kept);
        prop_assert_eq!(client.streams("sessions/").await.unwrap(), listed);
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(8))]

        #[test]
        fn a_deleted_stream_reads_as_empty_and_its_name_works_for_new_producers(
            down in prop::option::of(0usize..3), before in 0u8..6, after in 0u8..6,
        ) {
            let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
            rt.block_on(delete(down, before, after))?;
        }
    }

    proptest! {

        #[test]
        fn a_cluster_delivers_every_acked_event_once_and_the_newest_values(
            down in prop::option::of(0usize..3), ops in ops(),
        ) {
            let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
            rt.block_on(run(down, ops))?;
        }
    }
    /// An event's bytes: its own, so that no mix of two events equals a third.
    fn large_payload(i: usize, n: usize) -> Vec<u8> {
        (0..n).map(|j| (i * 31 + j / 7) as u8).collect()
    }

    /// Why: an agent session's events run to megabytes, and the broker takes a batch of at
    /// most 8 MiB. Through the SDK on a cluster, with a node down or not, events of any size
    /// come back whole, once and in order: across batches, when the same event is sent twice
    /// (its dedup id), and for a reader that stops anywhere, commits and resumes (it restarts
    /// the event it was inside from its first chunk). A reader that is not chunked sees the
    /// chunks as they are stored.
    async fn large(
        down: Option<usize>,
        sizes: Vec<usize>,
        polls: usize,
    ) -> Result<(), TestCaseError> {
        let dir = tempfile::tempdir().unwrap();
        let c = Cluster::new(dir.path(), down).await;
        let client = Client::connect(&c.addrs, "tok").unwrap();
        let mut p = client.producer("big", "w");
        p.set_chunked(true);
        for (i, &n) in sizes.iter().enumerate() {
            p.send(i as u64, format!("id{i}").as_bytes(), &large_payload(i, n));
        }
        // the first event again, as a retry of the application sends it
        p.send(0, b"id0", &large_payload(0, sizes[0]));
        p.sync().await.unwrap();
        let want: Vec<Vec<u8>> = (sizes.iter().enumerate())
            .map(|(i, &n)| large_payload(i, n))
            .collect();
        let payloads = |evs: Vec<Event>| {
            evs.into_iter()
                .map(|e| e.payload.to_vec())
                .collect::<Vec<_>>()
        };
        let mut all = client.consumer("big", "g", Start::Beginning).await.unwrap();
        all.set_chunked(true);
        let got = payloads(drain(&mut all).await);
        // caught up, the reads stand where the reader would resume: at the stream's end
        prop_assert!(!all.cursor().is_empty() && all.read_position() == all.cursor());
        prop_assert_eq!(got.len(), want.len());
        prop_assert!(got == want, "not the events sent");
        // a reader that stops after a few small polls, commits, and another that resumes
        let mut a = client.consumer("big", "h", Start::Beginning).await.unwrap();
        a.set_chunked(true);
        let mut first = vec![];
        for _ in 0..polls {
            first.extend(a.poll(1).await.unwrap());
        }
        // the reads stand past the chunks held of an event not yet whole, never behind it
        let read = a.read_position();
        prop_assert!(
            a.cursor()
                .iter()
                .all(|(l, at)| read.get(l).is_some_and(|r| r >= at))
        );
        commit(&a).await;
        let or = Box::new(Start::End);
        let mut b = client
            .consumer("big", "h", Start::Committed { or })
            .await
            .unwrap();
        b.set_chunked(true);
        let both = payloads([first, drain(&mut b).await].concat());
        prop_assert!(
            both == want,
            "{} events over the two readers, {} sent",
            both.len(),
            want.len()
        );
        // not chunked: the stored events, each with its tag
        let raw = drain(&mut client.consumer_at("big", Default::default())).await;
        let cut = sizes.iter().any(|&n| n > precog_client::CHUNK_BYTES);
        prop_assert_eq!(raw.len() > sizes.len(), cut);
        prop_assert!(raw.iter().all(|e| e.payload[0] <= 3));
        Ok(())
    }

    /// Why: a node keeps the copy of a dedup id that reaches it first, and batches in flight
    /// reach it in any order, so a retry sent beside its first copy could be the one kept and
    /// the event read out of order (spec/client/Retry.tla). A batch that repeats an id of one
    /// in flight is sent only once nothing is; one that repeats none, or carries a
    /// compaction key (`put`), goes beside the others as before.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_retry_of_an_event_in_flight_waits_for_its_first_copy() {
        let dir = tempfile::tempdir().unwrap();
        let c = Cluster::new(dir.path(), None).await;
        let client = Client::connect(&c.addrs, "tok").unwrap();
        let mut p = client.producer("retry", "w");
        p.send(0, b"a", b"first");
        p.flush().await.unwrap();
        p.send(1, b"b", b"another");
        p.flush().await.unwrap();
        assert_eq!(p.in_flight(), 2, "no id repeated: both in flight");
        p.send(2, b"a", b"again");
        p.flush().await.unwrap();
        assert_eq!(
            p.in_flight(),
            1,
            "the retry went once nothing was in flight"
        );
        p.send(3, b"c", b"new");
        p.flush().await.unwrap();
        assert_eq!(p.in_flight(), 2, "what was acknowledged holds nothing back");
        p.sync().await.unwrap();
        let mut k = client.producer("keys", "w");
        for _ in 0..2 {
            k.put(0, b"8 bytes.", b"v");
            k.flush().await.unwrap();
        }
        assert_eq!(k.in_flight(), 2, "a compaction key is not a dedup id");
        k.sync().await.unwrap();
    }

    /// Why: a chunked reader pointed at a stream that is not chunked, or at an event it cannot
    /// hold, must say so and go on: the events it can read still come, the error is the next
    /// poll's (the events of this one are not lost to it), and it is reported once. A `put`
    /// on a chunked producer is a whole event, tagged like any other.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_chunked_reader_reports_what_it_passes_over_once_and_reads_on() {
        let dir = tempfile::tempdir().unwrap();
        let c = Cluster::new(dir.path(), None).await;
        let client = Client::connect(&c.addrs, "tok").unwrap();
        // an event with no tag at all, from a producer that is not chunked
        let mut plain = client.producer("mixed", "plain");
        plain.send(0, &[], &[]);
        plain.sync().await.unwrap();
        let mut chunked = client.producer("mixed", "chunked");
        chunked.set_chunked(true);
        chunked.set_compression(false); // (as sent: the chunks are the same events either way)
        chunked.put(1, b"key", b"a value");
        chunked.send(2, &[], b"an event");
        chunked.sync().await.unwrap();
        let mut r = client.consumer_at("mixed", Default::default());
        r.set_chunked(true);
        let mut got: Vec<Vec<u8>> = vec![];
        let mut errors = 0;
        for _ in 0..6 {
            match r.poll(1 << 20).await {
                Ok(evs) => got.extend(evs.into_iter().map(|e| e.payload.to_vec())),
                Err(e) => {
                    assert!(e.to_string().contains("without a chunk tag"), "{e}");
                    errors += 1;
                }
            }
        }
        got.sort();
        assert_eq!(got, [b"a value".to_vec(), b"an event".to_vec()]);
        assert_eq!(errors, 1, "reported once");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(6))]

        #[test]
        fn events_larger_than_a_batch_come_back_whole_once_and_in_order(
            down in prop::option::of(0usize..3),
            sizes in prop::collection::vec(prop_oneof![0usize..2_000, 400_000usize..3_500_000], 1..5),
            polls in 0usize..4,
        ) {
            let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
            rt.block_on(large(down, sizes, polls))?;
        }
    }

    /// The requests of an agent's session, from `steps` of (kind, new bytes): each request is
    /// the conversation so far and a new message; now and then the oldest message is dropped,
    /// or the request is another one altogether.
    fn session(steps: &[(u8, usize)]) -> Vec<Vec<u8>> {
        let mut x = 0x2545_F491_4F6C_DD1Du64;
        let mut msgs: Vec<Vec<u8>> = vec![];
        (steps.iter().enumerate())
            .map(|(i, &(kind, n))| {
                let mut msg = vec![0u8; n];
                for b in &mut msg {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    *b = x as u8;
                }
                match kind {
                    0 => return msg,
                    1 if !msgs.is_empty() => drop(msgs.remove(0)),
                    _ => {}
                }
                msgs.push(msg);
                let head = format!("{{\"call\":{i},\"system\":\"you are an agent\",\"messages\":[");
                [head.as_bytes(), &msgs.concat(), b"]}"].concat()
            })
            .collect()
    }

    fn payloads(evs: Vec<Event>) -> Vec<Vec<u8>> {
        evs.into_iter().map(|e| e.payload.to_vec()).collect()
    }

    /// Why: an agent's requests repeat the conversation so far, and with extents on the stream
    /// holds what each adds. Through the SDK on a cluster, with a node down or not, whatever
    /// the session does and wherever the producer's limits start a new arena, a reader gets
    /// every request back whole, once and in order: from the beginning, from a position it
    /// committed anywhere (it goes back to its arena's start by itself), and from the end
    /// while the session goes on. A reader without the mode sees what is stored.
    async fn extents(
        down: Option<usize>,
        steps: Vec<(u8, usize)>,
        limits: (u32, usize),
        polls: usize,
        later: usize,
    ) -> Result<(), TestCaseError> {
        let dir = tempfile::tempdir().unwrap();
        let c = Cluster::new(dir.path(), down).await;
        let client = Client::connect(&c.addrs, "tok").unwrap();
        let want = session(&steps);
        let later = later.min(want.len());
        let mut p = client.producer("acme/session", "agent");
        p.set_extents(Some(precog_client::extents::Extents {
            reset_events: limits.0,
            arena_bytes: limits.1,
        }));
        for (i, e) in want[..want.len() - later].iter().enumerate() {
            p.send(i as u64, &[], e);
            if i % 3 == 2 {
                p.flush().await.unwrap(); // several batches in flight
            }
        }
        p.sync().await.unwrap();
        prop_assert!(
            p.extent_bytes()
                <= 3 * (limits.1 + want.iter().map(|e| e.len()).max().unwrap_or(0)) + 4096
        );
        let mut tail = client
            .consumer("acme/session", "tail", Start::End)
            .await
            .unwrap();
        tail.set_extents(true);
        prop_assert!(drain(&mut tail).await.is_empty(), "events past the end");
        for (i, e) in want.iter().enumerate().skip(want.len() - later) {
            p.send(i as u64, &[], e);
        }
        p.sync().await.unwrap();
        let got = payloads(drain(&mut tail).await);
        prop_assert!(
            got == want[want.len() - later..],
            "{} events after the end",
            got.len()
        );

        let mut all = client
            .consumer("acme/session", "g", Start::Beginning)
            .await
            .unwrap();
        all.set_extents(true);
        let events = drain(&mut all).await;
        // one event alone, rebuilt by a node, for a reader that holds nothing
        for e in events.iter().rev().step_by(4) {
            let one = client
                .event("acme/session", &e.writer, e.epoch, e.seq)
                .await
                .unwrap();
            prop_assert!(one.as_ref() == Some(&e.payload), "event {} alone", e.seq);
        }
        let got = payloads(events);
        prop_assert!(got == want, "{} events of {}", got.len(), want.len());
        prop_assert_eq!(all.passed_over(), 0);
        // a reader that stops after a few batches, commits, and another that resumes
        let mut a = client
            .consumer("acme/session", "h", Start::Beginning)
            .await
            .unwrap();
        a.set_extents(true);
        let mut first = vec![];
        for _ in 0..polls {
            first.extend(a.poll(1).await.unwrap());
        }
        commit(&a).await;
        let or = Box::new(Start::End);
        let mut b = client
            .consumer("acme/session", "h", Start::Committed { or })
            .await
            .unwrap();
        b.set_extents(true);
        let both = payloads([first, drain(&mut b).await].concat());
        prop_assert!(
            both == want,
            "{} events over the two readers, {} sent",
            both.len(),
            want.len()
        );
        // without the mode: the stored events, each a chunk tag and a version
        let raw = drain(&mut client.consumer_at("acme/session", Default::default())).await;
        prop_assert!(raw.len() >= want.len());
        prop_assert!(raw.iter().all(|e| e.payload[0] <= 3));
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(12))]

        #[test]
        fn an_extents_stream_reads_back_every_request_whole_from_anywhere(
            down in prop::option::of(0usize..3),
            steps in prop::collection::vec((prop_oneof![1 => Just(0u8), 2 => Just(1u8), 7 => Just(2u8)], 0usize..3_000), 1..30),
            limits in (1u32..9, prop_oneof![0usize..40_000, Just(32 << 20)]),
            polls in 0usize..6, later in 0usize..8,
        ) {
            let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
            rt.block_on(extents(down, steps, limits, polls, later))?;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_extents_stream_stores_a_session_s_context_once() {
        // Why: this is what the mode is for. 60 requests of a conversation that grows by 2 KB
        // hold 3.6 MB; the stream holds what each adds, and the conversation again where the
        // producer let go of its arena (`park`, for a session gone idle).
        let dir = tempfile::tempdir().unwrap();
        let c = Cluster::new(dir.path(), None).await;
        let client = Client::connect(&c.addrs, "tok").unwrap();
        let sent = session(&[(2, 2000); 60]);
        let logical: usize = sent.iter().map(|e| e.len()).sum();
        let mut p = client.producer("acme/session-1", "agent");
        p.set_extents(Some(Default::default()));
        for (i, e) in sent.iter().enumerate() {
            if i == 40 {
                assert!(p.extent_bytes() > 80_000, "the arena and the last request");
                p.park();
                assert_eq!(p.extent_bytes(), 0);
            }
            p.send(i as u64, b"", e);
            p.sync().await.unwrap();
        }
        let mut r = client.consumer_at("acme/session-1", Default::default());
        r.set_extents(true);
        let got = drain(&mut r).await;
        assert!(got.windows(2).all(|w| w[0].seq < w[1].seq));
        assert_eq!(
            got.last().unwrap().event_time_us,
            59,
            "an event keeps its time"
        );
        assert!(payloads(got) == sent);
        // a reader that starts inside an arena, with the rest of the session in its first
        // read: it goes back for the arena, and what else that read held of the log is read
        // again after it
        let mut some = client.consumer_at("acme/session-1", Default::default());
        some.set_extents(true);
        let n = some.poll(1).await.unwrap().len();
        assert!((1..39).contains(&n), "{n}");
        let mut late = client.consumer_at("acme/session-1", some.cursor().clone());
        late.set_extents(true);
        assert!(payloads(late.poll(4 << 20).await.unwrap()) == sent[n..]);
        let mut plain = client.consumer_at("acme/session-1", Default::default());
        let stored: usize = drain(&mut plain)
            .await
            .iter()
            .map(|e| e.payload.len())
            .sum();
        assert!(
            logical > 3_600_000 && stored * 15 < logical,
            "{stored} of {logical}"
        );
        // a producer that is not of the mode holds nothing, and its `park` is nothing
        let mut plain = client.producer("acme/plain", "agent");
        plain.park();
        assert_eq!(plain.extent_bytes(), 0);
        assert_eq!(
            r.passed_over() + client.consumer_at("x", Default::default()).passed_over(),
            0
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_read_rebuilds_about_a_budget_of_bytes_and_the_next_goes_on_from_its_cursor() {
        // Why: `max_bytes` bounds the stored bytes a read looks at, and an event stored as a
        // recipe of a few bytes rebuilds to megabytes: twelve sends of one 3 MB body are a few
        // KB stored after the first, and a read that rebuilt them all would hold 36 MB (a
        // tenant can make that gigabytes in a gateway). A read stops once it has rebuilt
        // `POLL_REBUILT_BYTES`, and its cursor is right there: a reader that resumes from it
        // gets the next event, none again and none passed over.
        let dir = tempfile::tempdir().unwrap();
        let c = Cluster::new(dir.path(), None).await;
        let client = Client::connect(&c.addrs, "tok").unwrap();
        let body = large_payload(1, 3 << 20);
        let mut p = client.producer("acme/big", "agent");
        p.set_extents(Some(Default::default()));
        for i in 0..12 {
            p.send(i, b"", &body);
            p.sync().await.unwrap();
        }
        let (mut times, mut pages) = (vec![], 0);
        let mut r = client.consumer_at("acme/big", Default::default());
        loop {
            r.set_extents(true);
            let page = r.poll(8 << 20).await.unwrap();
            if page.is_empty() {
                break;
            }
            let rebuilt: usize = page.iter().map(|e| e.payload.len()).sum();
            assert!(
                rebuilt < precog_client::POLL_REBUILT_BYTES + body.len(),
                "a read rebuilt {rebuilt} bytes"
            );
            assert!(page.iter().all(|e| e.payload[..] == body[..]));
            times.extend(page.iter().map(|e| e.event_time_us));
            pages += 1;
            // (a new reader at the cut: it goes back for the arena, and yields from the cut)
            r = client.consumer_at("acme/big", r.cursor().clone());
        }
        assert_eq!(times, (0..12).collect::<Vec<u64>>());
        assert!(pages >= 2, "{pages}");
        // and a reader that keeps going sees the same, page after page
        let mut r = client.consumer_at("acme/big", Default::default());
        r.set_extents(true);
        let all = drain(&mut r).await;
        assert_eq!(all.len(), 12);
        assert!(all.windows(2).all(|w| w[0].seq < w[1].seq));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_extents_producer_refuses_dedup_ids_and_puts_and_stops_after_a_failed_append() {
        // Why: an event the broker drops (a dedup id seen before, a compacted key written again)
        // takes with it bytes that later events name, so both are refused where they are sent.
        // And after an append failed the producer has encoded events against bytes the stream
        // may not hold, and the broker serves nothing of a writer log past a missing batch: the
        // producer must stop, loudly (spec/client/Extents.tla), and a new producer's events
        // must be readable.
        let dir = tempfile::tempdir().unwrap();
        let c = Cluster::new(dir.path(), None).await;
        let client = Client::connect(&c.addrs, "tok").unwrap();
        let sent = session(&[(2, 2000); 12]);
        let mut p = client.producer("acme/session-2", "agent");
        p.set_extents(Some(Default::default()));
        p.send(0, b"", &sent[0]);
        p.send(1, b"an-id", &sent[1]);
        let e = p.flush().await.unwrap_err().to_string();
        assert!(
            e.contains("acme/session-2/agent") && e.contains("no dedup ids"),
            "{e}"
        );
        p.put(1, b"key", &sent[1]);
        let e = p.sync().await.unwrap_err().to_string();
        assert!(e.contains("not compacted"), "{e}");
        p.send(1, b"", &vec![0; precog_client::MAX_EVENT_BYTES + 1]);
        let e = p.sync().await.unwrap_err().to_string();
        assert!(e.contains("is over"), "{e}");
        for e in &sent[1..4] {
            p.send(1, b"", e);
        }
        p.sync().await.unwrap(); // the refused events were not sent; the others are

        // the stream is deleted under the producer: its next append is refused
        assert!(client.delete("acme/session-2").await.unwrap() > 0);
        p.send(4, b"", &sent[4]);
        p.send(4, b"", &sent[5]);
        let e = p.sync().await.unwrap_err().to_string();
        assert!(
            !e.contains("sends nothing more"),
            "the append's own error: {e}"
        );
        p.send(5, b"", &sent[5]);
        let e = p.sync().await.unwrap_err().to_string();
        assert!(
            e.contains("an append failed") && e.contains("sends nothing more"),
            "{e}"
        );
        assert!(p.flush().await.is_err(), "and again");
        assert!(!p.is_full(), "what it had buffered is gone");

        // a new producer is a new writer log with its own arena
        let mut p = client.producer("acme/session-2", "agent");
        p.set_extents(Some(Default::default()));
        for e in &sent[4..] {
            p.send(9, b"", e);
        }
        p.sync().await.unwrap();
        let mut r = client.consumer_at("acme/session-2", Default::default());
        r.set_extents(true);
        assert!(payloads(drain(&mut r).await) == sent[4..]);

        // a reader of the mode on a stream that is not of it says so, once, and reads on
        let mut plain = client.producer("acme/plain", "agent");
        plain.send(0, b"", b"\x09not an extents event");
        plain.sync().await.unwrap();
        let mut r = client.consumer_at("acme/plain", Default::default());
        r.set_extents(true);
        assert!(r.poll(1 << 20).await.unwrap().is_empty());
        let e = r.poll(1 << 20).await.unwrap_err().to_string();
        assert!(e.contains("chunk tag 9"), "{e}");
        assert!(r.poll(1 << 20).await.unwrap().is_empty(), "reported once");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_node_rebuilds_one_event_and_says_what_retention_took() {
        // Why: a reader without the stream's arena (a debugger opening one request) asks a
        // node for one event. It gets the event's bytes; `None` for an event retention took
        // or took the arena's start of, for good, on a stream whose class keeps 150 KB; and an
        // error, not an event, for what is not one: a sequence number inside an event, an
        // event of another mode, an event larger than a frame. A node that is down is passed
        // over (spec/extents/ExtentsRead.tla).
        for down in [None, Some(0), Some(1), Some(2)] {
            let dir = tempfile::tempdir().unwrap();
            let c = Cluster::new(dir.path(), down).await;
            let client = Client::connect(&c.addrs, "tok").unwrap();
            // 40 requests of a conversation's last 5 messages of 8 KB, a new arena every 8:
            // an arena is about 100 KB, the stream keeps 150 KB (a stream that keeps less than
            // an arena keeps nothing a reader can rebuild)
            let steps: Vec<(u8, usize)> = (0..40).map(|i| (1 + (i < 5) as u8, 8000)).collect();
            let sent = session(&steps);
            let mut p = client.producer("short/session", "agent");
            p.set_extents(Some(precog_client::extents::Extents {
                reset_events: 8,
                ..Default::default()
            }));
            for (i, e) in sent.iter().enumerate() {
                p.send(i as u64, b"", e);
                p.sync().await.unwrap();
            }
            let mut r = client.consumer_at("short/session", Default::default());
            r.set_extents(true);
            let kept = drain(&mut r).await;
            let first = sent.len() - kept.len();
            assert!(
                first >= 8 && kept.len() >= 8,
                "{first} trimmed, {} kept",
                kept.len()
            );
            assert_eq!(first % 8, 0, "a reader starts at a reset");
            assert!(payloads(kept.clone()) == sent[first..]);
            let (writer, epoch) = (kept[0].writer.clone(), kept[0].epoch);
            let event = |seq: u64| client.event("short/session", &writer, epoch, seq);
            // what a consumer yields, a node rebuilds; what retention took is gone
            assert_eq!(
                event(kept[5].seq).await.unwrap(),
                Some(kept[5].payload.clone())
            );
            assert_eq!(event(0).await.unwrap(), None, "trimmed");
            // an event the stream still holds whose arena's start is trimmed: gone as well
            // (the consumer passed these over)
            if r.passed_over() > 0 {
                assert_eq!(event(kept[0].seq - 1).await.unwrap(), None, "an orphan");
            }
            // whichever node a stream's readers ask first, and whichever is down: every node
            // is asked before the reader gives up (eight streams: every first node)
            for i in 0..8 {
                let name = format!("acme/one-{i}");
                let mut p = client.producer(&name, "agent");
                p.set_extents(Some(Default::default()));
                p.send(0, b"", name.as_bytes());
                p.sync().await.unwrap();
                let e = drain(&mut client.consumer_at(&name, Default::default())).await;
                let one = client.event(&name, "agent", e[0].epoch, 0).await.unwrap();
                assert_eq!(
                    one.as_deref(),
                    Some(name.as_bytes()),
                    "{name}, down: {down:?}"
                );
            }
            // past the log's end: no node holds it
            let e = event(10_000).await.unwrap_err();
            assert!(
                format!("{e:#}").contains("short/session/agent seq 10000"),
                "{e:#}"
            );
        }
        let dir = tempfile::tempdir().unwrap();
        let c = Cluster::new(dir.path(), None).await;
        // (a node reads the 17 MiB before it refuses them: in a test build on a busy machine
        // that took the 1.5 s a client gives a request by default)
        let patient = std::time::Duration::from_secs(20);
        let client = Client::connect_with_timeout(&c.addrs, "tok", patient).unwrap();
        // a 17 MiB event: several stored events, and more than a frame holds
        let large: Vec<u8> = (0..17usize << 20).map(|i| (i / 4093) as u8).collect();
        let mut p = client.producer("acme/large", "agent");
        p.set_extents(Some(Default::default()));
        p.send(0, b"", &large);
        p.send(1, b"", b"a small one after it");
        p.sync().await.unwrap();
        let mut r = client.consumer_at("acme/large", Default::default());
        r.set_extents(true);
        let got = drain(&mut r).await;
        assert!(got.len() == 2 && got[0].payload[..] == large[..]);
        let event = |seq: u64| client.event("acme/large", "agent", got[0].epoch, seq);
        let e = format!("{:#}", event(0).await.unwrap_err());
        assert!(e.contains("larger than a frame"), "{e}");
        let e = format!("{:#}", event(1).await.unwrap_err());
        assert!(e.contains("not the start of an event"), "{e}");
        assert_eq!(
            event(got[1].seq).await.unwrap().unwrap(),
            "a small one after it"
        );
        // an event of a stream that is not of the mode
        let mut plain = client.producer("acme/plain", "agent");
        plain.set_chunked(true);
        plain.send(0, b"", b"\x09a chunked event, no extents");
        plain.sync().await.unwrap();
        let e = client.event("acme/plain", "agent", 0, 0).await.unwrap_err();
        assert!(format!("{e:#}").contains("acme/plain/agent seq 0"), "{e:#}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn retention_inside_an_arena_is_passed_over_and_a_large_request_takes_several_batches() {
        // Why: the broker trims an extents stream as any stream. A trim inside an arena
        // leaves events whose arena's start is gone: a consumer must pass those over, count
        // them, and yield the rest whole from the next reset (here a class that keeps
        // 150 KB, arenas of about 100 KB). And a request over a batch must go as several
        // batches, as a chunked producer's does: the mode includes chunking.
        let dir = tempfile::tempdir().unwrap();
        let c = Cluster::new(dir.path(), None).await;
        let client = Client::connect(&c.addrs, "tok").unwrap();
        let steps: Vec<(u8, usize)> = (0..40).map(|i| (1 + (i < 5) as u8, 8000)).collect();
        let sent = session(&steps);
        let mut p = client.producer("short/session", "agent");
        p.set_extents(Some(precog_client::extents::Extents {
            reset_events: 8,
            ..Default::default()
        }));
        for (i, e) in sent.iter().enumerate() {
            p.send(i as u64, b"", e);
            p.sync().await.unwrap();
        }
        let mut r = client.consumer_at("short/session", Default::default());
        r.set_extents(true);
        let kept = payloads(drain(&mut r).await);
        let first = sent.len() - kept.len();
        assert!(
            first >= 8 && kept.len() >= 8,
            "{first} trimmed, {} kept",
            kept.len()
        );
        assert_eq!(first % 8, 0, "a reader starts at a reset");
        assert!(kept == sent[first..]);
        assert!(
            (1..8).contains(&r.passed_over()),
            "{} passed over",
            r.passed_over()
        );

        // 9 MiB that lz4 cannot shrink: more than the broker takes in one batch
        let mut x = 7u64;
        let large: Vec<u8> = (0..9usize << 20)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        let mut p = client.producer("acme/large", "agent");
        p.set_extents(Some(Default::default()));
        p.send(0, b"", &large);
        p.sync().await.unwrap();
        let mut r = client.consumer_at("acme/large", Default::default());
        r.set_extents(true);
        let got = drain(&mut r).await;
        assert!(got.len() == 1 && got[0].payload[..] == large[..]);

        // a reader that stops with chunks of a request in hand: its position is the
        // request's start, not how far it has read, so the reader that resumes gets it whole
        let mut p = client.producer("acme/resume", "agent");
        p.set_extents(Some(Default::default()));
        p.send(0, b"", b"a small request");
        p.send(1, b"", &large[..3 << 20]);
        p.sync().await.unwrap();
        let mut a = client.consumer_at("acme/resume", Default::default());
        a.set_extents(true);
        let first = a.poll(1).await.unwrap();
        assert_eq!(
            first.len(),
            1,
            "the small one; the large one's first chunk came with it"
        );
        let at = a.cursor().clone();
        assert_eq!(
            at.values().copied().collect::<Vec<_>>(),
            [1],
            "at the large one's start"
        );
        let mut b = client.consumer_at("acme/resume", at);
        b.set_extents(true);
        let rest = drain(&mut b).await;
        assert!(rest.len() == 1 && rest[0].payload[..] == large[..3 << 20]);
    }

    /// Why: the SDK talks to nodes it does not run: one of another version, one that hangs.
    /// A node that answers a list with something else, or takes no feed, must be an error
    /// the caller reads, within the client's timeout: not a panic, and not a wait for ever.
    #[tokio::test]
    async fn a_node_that_answers_a_list_with_something_else_or_takes_no_feed_is_an_error() {
        use precog_client::proto::{ControlResp, Frame, Kind};
        use tokio::io::AsyncWriteExt;
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let at = format!("tcp://{}", l.local_addr().unwrap());
        // it takes a hello, answers every control request as it would a delete, and says
        // nothing to a subscription
        tokio::spawn(async move {
            while let Ok((s, _)) = l.accept().await {
                tokio::spawn(async move {
                    let (mut r, mut w) = s.into_split();
                    let mut buf = bytes::BytesMut::new();
                    while let Ok(Some(f)) = precog_client::rpc::read_frame(&mut r, &mut buf).await {
                        let deleted = ControlResp::Deleted { epoch: 7 };
                        let (kind, body) = match f.kind {
                            Kind::Hello => (Kind::Ack, vec![]),
                            Kind::Control => {
                                (Kind::ControlResp, serde_json::to_vec(&deleted).unwrap())
                            }
                            _ => continue,
                        };
                        let mut out = bytes::BytesMut::new();
                        Frame::new(kind, f.req_id, body.into()).encode_into(&mut out);
                        w.write_all(&out).await.unwrap();
                    }
                });
            }
        });
        let timeout = std::time::Duration::from_millis(200);
        let client = Client::connect_with_timeout(&[at], "tok", timeout).unwrap();
        let e = client.streams_page("acme/", "", 10).await.unwrap_err();
        assert!(format!("{e:#}").contains("unexpected Deleted"), "{e:#}");
        let Err(e) = client.tail("acme/s", &Default::default()).await else {
            panic!("a feed no node confirmed")
        };
        let e = format!("{e:#}");
        assert!(
            e.starts_with("no node took the feed: ") && e.contains("did not confirm"),
            "{e}"
        );
    }
}
