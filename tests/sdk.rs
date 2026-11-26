//! The SDK against real nodes: clusters of the published node image (`testkit`), 3 to 5 nodes,
//! some started late. Ported from precog's `crates/client/tests/sdk.rs`, the same tests, the
//! nodes in containers instead of in the test's process.
use precog_client::testkit::{Cluster, Options};
use precog_client::{Client, Event, Start};
use serde_json::json;
use std::collections::BTreeSet;

/// Nodes `up` of three are started; the others start later (`start`).
async fn cluster(up: &[u32]) -> Cluster {
    cluster_of(3, up).await
}

/// Nodes `up` of `n`, each in its own failure domain. A node started later (`start`) is empty
/// and does no repair within the test, as precog's own tests start one.
async fn cluster_of(n: usize, up: &[u32]) -> Cluster {
    let mut o = Options::nodes(n)
        .up(&up.iter().map(|&i| i as usize).collect::<Vec<_>>())
        .set("rpc_timeout_ms", json!(300))
        .set("segment_size", json!(1 << 20))
        .stream("state", json!({"compacted": true}));
    o.token = "tok".into();
    for i in (0..n as u32).filter(|i| !up.contains(i)) {
        o = o.set_on(i as usize, "repair_interval_ms", json!(3_600_000));
    }
    tokio::task::spawn_blocking(move || Cluster::new(o))
        .await
        .unwrap()
        .unwrap()
}

/// Starts node `i`, which was not started with the cluster.
async fn start(c: &mut Cluster, i: u32, _repair_ms: u64) {
    tokio::task::block_in_place(|| c.start(i as usize)).unwrap();
}

/// The sum of a metric over every node.
fn metric(c: &Cluster, series: &str) -> u64 {
    (0..c.nodes.len())
        .map(|i| c.metric(i, series).unwrap() as u64)
        .sum()
}

fn names(evs: &[Event]) -> Vec<String> {
    evs.iter()
        .map(|e| String::from_utf8(e.payload.to_vec()).unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicas_publish_the_same_events_and_a_reader_sees_each_once_in_writer_order() {
    let c = cluster(&[0, 1, 2]).await;
    let client = Client::connect(&c.nodes, "tok").unwrap();
    let mut a = client.producer("raw.trades.x", "producer-a");
    let mut b = client.producer("raw.trades.x", "producer-b");
    for i in 0..50u64 {
        let id = format!("trade-{i}");
        a.send(i, id.as_bytes(), format!("t{i}").as_bytes());
        b.send(i, id.as_bytes(), format!("t{i}").as_bytes());
        if i % 10 == 9 {
            a.sync().await.unwrap();
            b.sync().await.unwrap();
        }
    }
    let mut r = client
        .consumer("raw.trades.x", "archiver", Start::Beginning)
        .await
        .unwrap();
    let mut got = vec![];
    loop {
        let evs = r.poll(4096).await.unwrap();
        if evs.is_empty() {
            break;
        }
        got.extend(evs);
    }
    let set: BTreeSet<String> = names(&got).into_iter().collect();
    assert_eq!(set.len(), 50, "every event once");
    assert_eq!(got.len(), 50, "no duplicate of a replica's event");
    for w in ["producer-a", "producer-b"] {
        let seqs: Vec<u64> = got
            .iter()
            .filter(|e| e.writer == w)
            .map(|e| e.seq)
            .collect();
        assert!(
            seqs.windows(2).all(|p| p[0] < p[1]),
            "{w} in order: {seqs:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_poll_is_empty_only_when_caught_up_even_past_a_batch_dedup_emptied() {
    // replica b's copy of a's event is a batch with every event dropped by dedup; a reader in
    // small steps reaches it, and an empty poll there would read as "caught up" before b's next
    let c = cluster(&[0, 1, 2]).await;
    let client = Client::connect(&c.nodes, "tok").unwrap();
    let (mut a, mut b) = (client.producer("s", "a"), client.producer("s", "b"));
    a.send(1, b"id0", b"t0");
    a.sync().await.unwrap();
    b.send(2, b"id0", b"t0");
    b.sync().await.unwrap();
    b.send(3, b"id1", b"t1");
    b.sync().await.unwrap();
    let mut r = client.consumer("s", "g", Start::Beginning).await.unwrap();
    let mut got = vec![];
    loop {
        let evs = r.poll(1).await.unwrap();
        if evs.is_empty() {
            break;
        }
        got.extend(evs);
    }
    assert_eq!(names(&got), ["t0", "t1"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_committed_position_resumes_on_another_node_losing_and_repeating_nothing() {
    let c = cluster(&[1, 2]).await; // node 0 is down: every client fails over
    let client = Client::connect(&c.nodes, "tok").unwrap();
    let mut p = client.producer("raw.trades.x", "producer-a");
    for i in 0..40u64 {
        p.send(i, format!("id{i}").as_bytes(), format!("t{i}").as_bytes());
        if i % 5 == 4 {
            p.sync().await.unwrap(); // batches of 5: the read unit
        }
    }
    let mut r = client
        .consumer("raw.trades.x", "archiver", Start::Beginning)
        .await
        .unwrap();
    let first = r.poll(300).await.unwrap();
    assert!(!first.is_empty() && first.len() < 40, "{}", first.len());
    r.commit().await.unwrap();
    // a new archiver pod: resumes at the commit through whichever node answers
    let mut r2 = client
        .consumer(
            "raw.trades.x",
            "archiver",
            Start::Committed {
                or: Box::new(Start::Beginning),
            },
        )
        .await
        .unwrap();
    let mut rest = vec![];
    loop {
        let evs = r2.poll(1 << 20).await.unwrap();
        if evs.is_empty() {
            break;
        }
        rest.extend(evs);
    }
    let all: Vec<String> = names(&first).into_iter().chain(names(&rest)).collect();
    assert_eq!(all, (0..40).map(|i| format!("t{i}")).collect::<Vec<_>>());
    // brrrrr keeps its own cursor in its checkpoint and resumes from it
    let mut r3 = client.consumer_at("raw.trades.x", r.cursor().clone());
    assert_eq!(names(&r3.poll(1 << 20).await.unwrap()), names(&rest));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_commits_the_cursor_it_kept_not_how_far_its_reader_got() {
    // The archiver reads ahead of its uploads: it may commit only what is durable in the object
    // store, so it keeps that cursor itself and commits it, while its consumer is further on.
    let c = cluster(&[0, 1, 2]).await;
    let client = Client::connect(&c.nodes, "tok").unwrap();
    let mut p = client.producer("raw.trades.x", "producer-a");
    for i in 0..20u64 {
        p.send(i, format!("id{i}").as_bytes(), format!("t{i}").as_bytes());
    }
    p.sync().await.unwrap();
    let mut r = client
        .consumer("raw.trades.x", "archiver", Start::Beginning)
        .await
        .unwrap();
    let read = r.poll(1 << 20).await.unwrap();
    assert_eq!(read.len(), 20);
    let mut uploaded = precog_client::Cursor::new();
    let log = r.cursor().keys().next().unwrap().clone();
    uploaded.insert(log.clone(), 10); // t0..t9 are durable; t10.. only read
    client
        .commit("archiver", "raw.trades.x", &uploaded)
        .await
        .unwrap();
    let mut resumed = client
        .consumer(
            "raw.trades.x",
            "archiver",
            Start::Committed {
                or: Box::new(Start::End),
            },
        )
        .await
        .unwrap();
    assert_eq!(resumed.cursor().get(&log), Some(&10));
    assert_eq!(
        names(&resumed.poll(1 << 20).await.unwrap()),
        (10..20).map(|i| format!("t{i}")).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_group_can_start_at_a_time_and_compacted_state_is_served() {
    let c = cluster(&[0, 1, 2]).await;
    let client = Client::connect(&c.nodes, "tok").unwrap();
    let mut p = client.producer("raw.trades.x", "producer-a");
    p.send(1, b"old", b"old");
    p.sync().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let since = precog_client::now_ms();
    p.send(2, b"new", b"new");
    p.sync().await.unwrap();
    let mut r = client
        .consumer(
            "raw.trades.x",
            "seeded",
            Start::Committed {
                or: Box::new(Start::Time(since)),
            },
        )
        .await
        .unwrap();
    assert_eq!(names(&r.poll(1 << 20).await.unwrap()), ["new"]);
    let mut s = client.producer("state", "collector");
    s.put(1, b"k1", b"v1");
    s.put(2, b"k1", b"v2");
    s.put(3, b"k2", b"w");
    s.sync().await.unwrap();
    let latest = client.latest("state").await.unwrap();
    assert_eq!(
        latest,
        vec![
            (b"k1".to_vec(), b"v2".to_vec().into()),
            (b"k2".to_vec(), b"w".to_vec().into())
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_producer_keeps_several_batches_in_flight_and_sync_waits_for_all() {
    let c = cluster(&[0, 1, 2]).await;
    let client = Client::connect(&c.nodes, "tok").unwrap();
    let mut p = client.producer("raw.trades.x", "producer-a");
    for i in 0..200u64 {
        p.send(i, format!("id{i}").as_bytes(), format!("t{i}").as_bytes());
        if i % 10 == 9 {
            p.flush().await.unwrap(); // queues the batch; at most 8 wait for their acks
            assert!(p.in_flight() <= 8);
        }
    }
    p.sync().await.unwrap();
    assert_eq!(p.in_flight(), 0);
    let mut r = client.consumer_at("raw.trades.x", Default::default());
    let mut got = vec![];
    loop {
        let evs = r.poll(1 << 20).await.unwrap();
        if evs.is_empty() {
            break;
        }
        got.extend(evs);
    }
    assert_eq!(
        names(&got),
        (0..200).map(|i| format!("t{i}")).collect::<Vec<_>>()
    );
}

#[test]
fn dedup_ids_travel_as_8_byte_hashes() {
    assert_eq!(
        precog_client::dedup_key(b"binance-futures-p3-123456").len(),
        8
    );
    assert_eq!(
        precog_client::dedup_key(b"a"),
        precog_client::dedup_key(b"a")
    );
    assert_ne!(
        precog_client::dedup_key(b"a"),
        precog_client::dedup_key(b"b")
    );
    assert!(
        precog_client::dedup_key(b"").is_empty(),
        "no id: never deduplicated"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producers_compress_by_default_and_readers_decompress() {
    let c = cluster(&[0, 1, 2]).await;
    let client = Client::connect(&c.nodes, "tok").unwrap();
    let mut p = client.producer("raw.trades.x", "producer-a");
    let mut plain = client.producer("raw.trades.x", "producer-b");
    plain.set_compression(false);
    for i in 0..100u64 {
        let payload =
            format!(r#"{{"exchange":"binance","symbol":"BTCUSDT","trade_id":{i},"side":"buy"}}"#);
        p.send(i, format!("a{i}").as_bytes(), payload.as_bytes());
        plain.send(i, format!("b{i}").as_bytes(), payload.as_bytes());
    }
    p.sync().await.unwrap();
    plain.sync().await.unwrap();
    // (an acknowledged batch is on two nodes of the three, and on the third a moment later:
    // the node a poll reads may be that one, and an empty poll is its end, not the stream's)
    let mut r = client.consumer_at("raw.trades.x", Default::default());
    let mut evs = vec![];
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while evs.len() < 200 && std::time::Instant::now() < deadline {
        evs.extend(r.poll(1 << 20).await.unwrap());
        if evs.len() < 200 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }
    assert_eq!(evs.len(), 200);
    let bytes = c.metric(0, "precog_bytes").unwrap() as u64;
    assert_eq!(
        evs.iter()
            .filter(|e| e.payload.starts_with(b"{\"exchange\""))
            .count(),
        200
    );
    // the compressed writer's batch is the smaller of the two
    let a: u64 = evs
        .iter()
        .filter(|e| e.writer == "producer-a")
        .map(|e| e.payload.len() as u64)
        .sum();
    assert!(
        bytes < 2 * a,
        "stored {bytes} bytes for {a} + {a} payload bytes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_end_of_a_stream_is_every_acked_event_even_when_one_node_is_behind() {
    // Why: an acked batch is on 2 of 3 nodes, not on all 3. A reader that takes one node's end
    // as the stream's end (brrrrr's replay, a new group at Start::End) would stop short, or skip
    // events, whenever that node is behind: just restarted, or repairing.
    let mut c = cluster(&[1, 2]).await;
    let client = Client::connect(&c.nodes, "tok").unwrap();
    // a stream whose clients ask node 0 first: the node that will be behind
    let stream = (0..)
        .map(|i| format!("raw.trades.{i}"))
        .find(|s| xxhash_rust::xxh3::xxh3_64(s.as_bytes()).is_multiple_of(3))
        .unwrap();
    let mut p = client.producer(&stream, "w");
    for i in 0..40u64 {
        p.send(i, format!("id{i}").as_bytes(), b"x");
        if i % 10 == 9 {
            p.flush().await.unwrap();
        }
    }
    p.sync().await.unwrap();
    start(&mut c, 0, 3_600_000).await; // empty, and no repair within the test

    let end = client.end(&stream).await.unwrap();
    assert_eq!(end.values().sum::<u64>(), 40, "every acked event: {end:?}");
    let at_end = client.consumer(&stream, "g", Start::End).await.unwrap();
    assert_eq!(
        at_end.cursor(),
        &end,
        "a new group at the end skips nothing it should not"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_end_of_a_stream_on_four_nodes_merges_enough_ends_to_meet_every_acked_copy() {
    // Why: an acked batch is on `copies` nodes, so only the ends of nodes - copies + 1 nodes are
    // sure to include one of them. On four nodes with 2 copies, two ends can both miss it, and a
    // reader starting at the end would read acked events as new. Found by spec/mesh (MC_end4).
    let mut c = cluster_of(4, &[0, 1]).await;
    let client = Client::connect(&c.nodes, "tok").unwrap();
    // clients of this stream ask node 2 first, then 3: the two nodes without the batches. The
    // stream is on three of the four nodes, 0 and 1 among them: its third, and the node that
    // does not hold it (it passes the question to one that does), are the two that are empty
    let domains: Vec<String> = (0..4).map(|i| format!("d{i}")).collect();
    let nodes: Vec<(u32, &str)> = (0..4).map(|i| (i, domains[i as usize].as_str())).collect();
    let stream = (0..)
        .map(|i| format!("raw.trades.{i}"))
        .filter(|s| xxhash_rust::xxh3::xxh3_64(s.as_bytes()) % 4 == 2)
        .find(|s| {
            let its = precog_client::placement::nodes_of(s, &nodes, 3);
            its.contains(&0) && its.contains(&1)
        })
        .unwrap();
    let mut p = client.producer(&stream, "w");
    for i in 0..10u64 {
        p.send(i, format!("id{i}").as_bytes(), b"x");
    }
    p.sync().await.unwrap();
    for i in [2, 3] {
        start(&mut c, i, 3_600_000).await; // empty, and no repair within the test
    }
    let end = client.end(&stream).await.unwrap();
    assert_eq!(end.values().sum::<u64>(), 10, "every acked event: {end:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reader_is_caught_up_only_when_no_node_holds_more_even_when_its_node_is_behind() {
    // Why: an acked batch is on 2 of 3 nodes. A reader whose node is behind (its producer failed
    // over past it, it restarted, it repairs) got an empty poll there and took it for the end:
    // acked events missing from a full read (the client model test found it under load).
    let mut c = cluster(&[1, 2]).await;
    let client = Client::connect(&c.nodes, "tok").unwrap();
    // a stream whose clients ask node 0 first: the node that will be behind
    let stream = (0..)
        .map(|i| format!("raw.trades.{i}"))
        .find(|s| xxhash_rust::xxh3::xxh3_64(s.as_bytes()).is_multiple_of(3))
        .unwrap();
    let mut p = client.producer(&stream, "w");
    for i in 0..40u64 {
        p.send(i, format!("id{i}").as_bytes(), format!("e{i}").as_bytes());
    }
    p.sync().await.unwrap();
    start(&mut c, 0, 3_600_000).await; // empty, and no repair within the test
    let mut r = client
        .consumer(&stream, "all", Start::Beginning)
        .await
        .unwrap();
    let mut got = vec![];
    loop {
        let evs = r.poll(1 << 20).await.unwrap();
        if evs.is_empty() {
            break;
        }
        got.extend(evs);
    }
    assert_eq!(got.len(), 40, "every acked event: {:?}", names(&got));
}

/// Clients and a stream's nodes in a cluster of more nodes than copies (a formal check:
/// scripts/formal-gate.py counts the lines these run).
mod props_nodes {
    use super::*;

    /// A stream of a five-node cluster whose nodes are `want`, the first of them first.
    fn stream_on(want: [u32; 3]) -> String {
        let domains: Vec<String> = (0..5).map(|i| format!("d{i}")).collect();
        let nodes: Vec<(u32, &str)> = (0..5).map(|i| (i, domains[i as usize].as_str())).collect();
        (0..)
            .map(|i| format!("raw.trades.{i}"))
            .find(|s| {
                let mut its = precog_client::placement::nodes_of(s, &nodes, 3);
                let first = its[0];
                its.sort();
                first == want[0]
                    && its == {
                        let mut w = want;
                        w.sort();
                        w
                    }
            })
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_end_of_a_stream_is_every_acked_event_through_nodes_that_do_not_hold_it() {
        // Why: a node that does not hold a stream passes a question about it to the stream's first
        // node that answers. A client that counted the ends of two such nodes counted that one
        // node's end twice: on a cluster of five, a client that knew three of the nodes took the
        // end of a node that was behind for the stream's, and a reader starting there read acked
        // events as new. The ends that count are those of the stream's own nodes.
        // a stream on nodes 0, 1 and 2, asked for at node 0 first
        let stream = stream_on([0, 1, 2]);
        let mut c = cluster_of(5, &[1, 2, 3, 4]).await;
        let all = Client::connect(&c.nodes, "tok").unwrap();
        let mut p = all.producer(&stream, "w");
        for i in 0..40u64 {
            p.send(i, format!("id{i}").as_bytes(), b"x");
        }
        p.sync().await.unwrap();
        start(&mut c, 0, 3_600_000).await; // empty, and no repair within the test
        // (its peers dial it again once their back-off from finding it down is over)
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;

        // a client that was given node 0 and the two nodes that do not hold the stream
        let some = [c.nodes[0].clone(), c.nodes[3].clone(), c.nodes[4].clone()];
        let end = Client::connect(&some, "tok").unwrap().end(&stream).await;
        let end = end.unwrap();
        assert_eq!(end.values().sum::<u64>(), 40, "every acked event: {end:?}");
    }

    /// What every node of the cluster passed on to another, in all.
    fn forwarded(c: &Cluster) -> u64 {
        metric(c, "precog_forwarded_total")
    }

    /// The control requests the cluster's nodes took from clients: reads, and where a stream
    /// is. No append, and nothing a peer asks.
    fn asked(c: &Cluster) -> u64 {
        metric(c, "precog_token_requests_total{token=\"*\"}")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_client_asks_where_its_stream_is_once_and_not_with_every_request() {
        // Why: a client asks where its stream is to save the hop of a request passed on. One
        // that asked with every batch it sends or reads would pay a request to save a hop:
        // it asks once, and again only when an answer tells it the stream has moved or what
        // it knows is old.
        let c = cluster_of(5, &[0, 1, 2, 3, 4]).await;
        let client = Client::connect(&c.nodes, "tok").unwrap();
        let stream = stream_on([0, 1, 2]);
        let before = asked(&c);
        let mut p = client.producer(&stream, "w");
        for i in 0..10u64 {
            p.send(i, format!("id{i}").as_bytes(), b"x");
            p.sync().await.unwrap();
        }
        let asks = asked(&c) - before;
        assert!(
            (1..=2).contains(&asks),
            "a producer of 10 batches asked {asks} times"
        );
        // a reader's requests are its reads and the one that asks (a node that is behind by
        // a batch is one read more: far from a request to ask with every read)
        let before = asked(&c);
        let mut r = client.consumer_at(&stream, Default::default());
        let (mut got, mut polls) = (0, 0u64);
        while got < 10 {
            got += r.poll(1).await.unwrap().len();
            polls += 1;
        }
        let requests = asked(&c) - before;
        assert_eq!(polls, 10, "a batch a poll");
        assert!(
            (polls + 1..polls + 5).contains(&requests),
            "{requests} requests for {polls} reads"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_producer_and_a_reader_ask_the_stream_s_nodes_and_no_node_passes_anything_on() {
        // Why: with five nodes and three copies, two in five streams were written and read
        // through a node that does not hold them, at a hop per request. A client that asks
        // where the stream is sends to its nodes, whichever nodes it was given.
        let c = cluster_of(5, &[0, 1, 2, 3, 4]).await;
        for given in [&c.nodes[..], &c.nodes[3..4]] {
            let client = Client::connect(given, "tok").unwrap();
            for s in 0..6 {
                let stream = format!("session-{s}-{}", given.len());
                let mut p = client.producer(&stream, "w");
                for i in 0..30u64 {
                    p.send(i, format!("id{i}").as_bytes(), b"x");
                    if i % 10 == 9 {
                        p.flush().await.unwrap();
                    }
                }
                p.sync().await.unwrap();
                let mut r = client.consumer_at(&stream, Default::default());
                let mut got = 0;
                loop {
                    let evs = r.poll(1 << 20).await.unwrap();
                    if evs.is_empty() {
                        break;
                    }
                    got += evs.len();
                }
                assert_eq!(got, 30, "{stream}");
                assert_eq!(client.end(&stream).await.unwrap().values().sum::<u64>(), 30);
            }
        }
        assert_eq!(forwarded(&c), 0, "requests passed on to a stream's node");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_producer_whose_stream_s_first_node_is_down_writes_on_and_a_reader_reads_it() {
        // Why: the stream's first node is where its producers send; while it is down they
        // must fail over as before, and a reader must read what they wrote from the others.
        let stream = stream_on([0, 1, 2]);
        let mut c = cluster_of(5, &[1, 2, 3, 4]).await;
        let client = Client::connect(&c.nodes, "tok").unwrap();
        let mut p = client.producer(&stream, "w");
        for i in 0..20u64 {
            p.send(i, format!("id{i}").as_bytes(), b"x");
            p.flush().await.unwrap();
        }
        p.sync().await.unwrap();
        let mut r = client.consumer_at(&stream, Default::default());
        let mut got = 0;
        while got < 20 {
            let evs = r.poll(1 << 20).await.unwrap();
            assert!(!evs.is_empty(), "caught up at {got} of 20");
            got += evs.len();
        }
        // the node is back: the producer finds it again, and nothing is passed on from then
        start(&mut c, 0, 3_600_000).await;
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        p.send(20, b"id20", b"x");
        p.sync().await.unwrap();
        let before = forwarded(&c);
        for i in 21..30u64 {
            p.send(i, format!("id{i}").as_bytes(), b"x");
            p.sync().await.unwrap();
        }
        assert_eq!(forwarded(&c), before, "appends to the stream's first node");
        assert_eq!(client.end(&stream).await.unwrap().values().sum::<u64>(), 30);
    }
}
