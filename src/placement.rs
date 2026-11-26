//! Which nodes hold a stream (spec/mesh/Placement.tla): a cluster may have more nodes than a
//! stream has copies. Every node computes the same answer from the node list and the stream's
//! name alone, so nothing is stored or agreed on per stream.

/// The nodes that hold `stream`, of `nodes` (id, failure domain), in the order a request for
/// it tries them: at most `copies`, one per failure domain (two copies in one domain are lost
/// together, so they count as one, as an ack's copies do).
///
/// Rendezvous hashing: each node's weight for the stream is a hash of the two, and the
/// heaviest nodes take it. A node added to the list takes a share of the streams from the
/// others and moves no stream between them; a node removed gives up only its own.
///
/// The weight is part of the protocol: every node of a cluster, of any version, must compute
/// the same one (xxh3-64 of the name, seeded with the node's id), or two nodes disagree on
/// where a stream is.
pub fn nodes_of(stream: &str, nodes: &[(u32, &str)], copies: usize) -> Vec<u32> {
    let weight = |id: u32| xxhash_rust::xxh3::xxh3_64_with_seed(stream.as_bytes(), id as u64);
    let mut by_weight: Vec<(u64, u32, &str)> =
        nodes.iter().map(|&(id, d)| (weight(id), id, d)).collect();
    by_weight.sort_unstable_by(|a, b| b.cmp(a)); // (ids are distinct: no tie is left to chance)
    let mut domains: Vec<&str> = Vec::with_capacity(copies);
    let mut out = Vec::with_capacity(copies);
    for (_, id, domain) in by_weight {
        if out.len() == copies {
            break;
        }
        if !domains.contains(&domain) {
            domains.push(domain);
            out.push(id);
        }
    }
    out
}

#[cfg(test)]
mod props {
    use super::*;
    use proptest::prelude::*;
    use std::collections::{BTreeSet, HashSet};

    /// Up to 12 nodes with distinct ids, in up to 5 failure domains.
    fn nodes() -> impl Strategy<Value = Vec<(u32, String)>> {
        proptest::collection::btree_map(0u32..32, 0u8..5, 1..12)
            .prop_map(|m| m.into_iter().map(|(id, d)| (id, format!("d{d}"))).collect())
    }

    fn of(stream: &str, nodes: &[(u32, String)], copies: usize) -> Vec<u32> {
        let nodes: Vec<(u32, &str)> = nodes.iter().map(|(i, d)| (*i, d.as_str())).collect();
        nodes_of(stream, &nodes, copies)
    }

    proptest! {
        /// Why: every node works a stream's nodes out for itself, from a list its config gives
        /// in its own order. They agree only if the order does not matter, and each copy
        /// survives another's failure domain only if no two share one.
        #[test]
        fn a_stream_s_nodes_are_one_per_domain_whatever_order_the_list_is_in(
            nodes in nodes(), stream in "[a-z/]{1,12}", copies in 1usize..6, by in any::<u64>(),
        ) {
            let got = of(&stream, &nodes, copies);
            let domain = |id: &u32| &nodes.iter().find(|(i, _)| i == id).unwrap().1;
            let domains: HashSet<&String> = got.iter().map(domain).collect();
            prop_assert_eq!(domains.len(), got.len(), "two of {:?} share a domain", got);
            let all: HashSet<&String> = nodes.iter().map(|(_, d)| d).collect();
            prop_assert_eq!(got.len(), copies.min(all.len()));
            let mut shuffled = nodes.clone();
            shuffled.sort_by_key(|(id, _)| xxhash_rust::xxh3::xxh3_64_with_seed(&id.to_le_bytes(), by));
            prop_assert_eq!(of(&stream, &shuffled, copies), got);
        }

        /// Why: a node added to a cluster must not reshuffle it. The only streams that move
        /// are the ones the new node takes; a stream it does not take stays where it was.
        #[test]
        fn a_node_added_takes_streams_and_moves_none_between_the_others(
            nodes in nodes(), stream in "[a-z/]{1,12}", copies in 1usize..6, domain in 0u8..6,
        ) {
            let before: BTreeSet<u32> = of(&stream, &nodes, copies).into_iter().collect();
            let new = (0..).find(|id| nodes.iter().all(|(i, _)| i != id)).unwrap();
            let mut grown = nodes.clone();
            grown.push((new, format!("d{domain}")));
            let after: BTreeSet<u32> = of(&stream, &grown, copies).into_iter().collect();
            let moved: Vec<&u32> = after.difference(&before).collect();
            prop_assert!(moved.iter().all(|id| **id == new), "{before:?} -> {after:?}");
            // (it joined a domain the stream had no node in, and room was left: nothing leaves)
            prop_assert!(after.len() >= before.len());
        }
    }

    /// Why: placement is what spreads a cluster's load. Each of five nodes in five domains
    /// should hold about three fifths of the streams at three copies, not one node most of them.
    #[test]
    fn streams_spread_evenly_over_the_nodes() {
        let nodes: Vec<(u32, String)> = (0..5).map(|i| (i, format!("d{i}"))).collect();
        let mut held = [0u32; 5];
        for s in 0..10_000 {
            for id in of(&format!("tenant/session-{s}"), &nodes, 3) {
                held[id as usize] += 1;
            }
        }
        for (id, n) in held.iter().enumerate() {
            assert!(
                (5_700..6_300).contains(n),
                "node {id} holds {n} of 10000: {held:?}"
            );
        }
    }

    /// Why: the weight is part of the protocol. A node of another version that computed
    /// another one would store a stream's batches where no reader looks for them. These are
    /// the answers this version gives; a change to them moves every stream of every cluster.
    #[test]
    fn the_nodes_of_a_stream_are_the_same_in_every_version() {
        let nodes: Vec<(u32, String)> = (0..5).map(|i| (i, format!("d{i}"))).collect();
        assert_eq!(of("session-1", &nodes, 3), [0, 4, 3]);
        assert_eq!(of("acme/run-42", &nodes, 3), [3, 4, 1]);
        assert_eq!(of("s", &nodes, 2), [0, 2]);
    }
}
