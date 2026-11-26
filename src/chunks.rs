//! Events larger than a batch (docs/ARCHITECTURE.md): an event of any size up to
//! `MAX_EVENT_BYTES` travels as several events of its writer log, each with a one-byte tag. The
//! SDK cuts and reassembles them (`Producer::set_chunked`, `Consumer::set_chunked`); a node
//! reassembles them to rebuild one event of an extents stream (`crate::extents::rebuild`).

use crate::proto::Event;
use anyhow::{Result, bail};

/// A chunked producer cuts an event into chunks of at most this many bytes: two fill one of
/// its batches.
pub const CHUNK_BYTES: usize = 512 << 10;
/// The largest event a chunked consumer puts back together: it holds an event's chunks in
/// memory until its last one, and a writer that never ends an event must not grow it for ever.
pub const MAX_EVENT_BYTES: usize = 256 << 20;

/// The first byte of every event of a chunked stream: the event whole, or its first, a middle
/// or its last chunk.
pub const WHOLE: u8 = 0;
pub const FIRST: u8 = 1;
const MIDDLE: u8 = 2;
const LAST: u8 = 3;

/// An event's payload as a chunked producer sends it: tagged chunks of at most `max` bytes.
pub fn chunks(payload: &[u8], max: usize) -> impl Iterator<Item = Vec<u8>> + '_ {
    let n = payload.len().div_ceil(max).max(1);
    (0..n).map(move |i| {
        let tag = match i {
            _ if n == 1 => WHOLE,
            0 => FIRST,
            i if i + 1 == n => LAST,
            _ => MIDDLE,
        };
        let part = &payload[i * max..((i + 1) * max).min(payload.len())];
        [&[tag][..], part].concat()
    })
}

/// A chunked consumer's reassembly of one writer log: the event whose chunks it holds.
#[derive(Default)]
pub struct Assembly {
    /// The first chunk (its sequence and time are the event's), the bytes so far, and the
    /// sequence its next chunk has.
    building: Option<(Event, Vec<u8>, u64)>,
}

impl Assembly {
    /// The next event of the log, in sequence order: the event it completes, if any. An event
    /// is yielded only whole: chunks whose first was not seen are passed over (the reader
    /// started inside the event, the broker dropped its first chunk as a duplicate, retention
    /// cut it), and an event with a chunk missing is dropped. An error leaves it at the next
    /// event's start.
    pub fn push(&mut self, e: Event) -> Result<Option<Event>> {
        // (a gap in the sequence: what was being built never ends)
        let building = self.building.take().filter(|b| b.2 == e.seq);
        let Some((&tag, part)) = e.payload.split_first() else {
            bail!(
                "{}/{} seq {}: an event without a chunk tag",
                e.stream,
                e.writer,
                e.seq
            );
        };
        match (tag, building) {
            (WHOLE, _) => Ok(Some(Event {
                payload: e.payload.slice(1..),
                ..e
            })),
            (FIRST, _) => {
                self.building = Some((e.clone(), part.to_vec(), e.seq + 1));
                Ok(None)
            }
            (MIDDLE | LAST, None) => Ok(None),
            (MIDDLE | LAST, Some((first, mut bytes, _))) => {
                anyhow::ensure!(
                    bytes.len() + part.len() <= MAX_EVENT_BYTES,
                    "{}/{} seq {}: an event over {MAX_EVENT_BYTES} bytes is passed over",
                    first.stream,
                    first.writer,
                    first.seq
                );
                bytes.extend_from_slice(part);
                if tag == MIDDLE {
                    self.building = Some((first, bytes, e.seq + 1));
                    return Ok(None);
                }
                Ok(Some(Event {
                    payload: bytes.into(),
                    ..first
                }))
            }
            _ => bail!("{}/{} seq {}: chunk tag {tag}", e.stream, e.writer, e.seq),
        }
    }

    /// Where the event being built starts: a reader that resumes must start there.
    pub fn boundary(&self) -> Option<u64> {
        self.building.as_ref().map(|b| b.0.seq)
    }
}

/// The chunk codec and the reader's reassembly, as properties: whatever the events' sizes,
/// wherever a reader starts and whatever chunks it never sees, it yields only events that were
/// sent, whole, in order.
#[cfg(test)]
mod props {
    use super::*;
    use bytes::Bytes;
    use proptest::prelude::*;

    /// The tagged events of `payloads` as one writer log's, chunked at `max` bytes.
    fn log_of(payloads: &[Vec<u8>], max: usize) -> Vec<Event> {
        let tagged = payloads.iter().flat_map(|p| chunks(p, max));
        (tagged.enumerate())
            .map(|(seq, payload)| Event {
                stream: "s".into(),
                writer: "w".into(),
                epoch: 1,
                seq: seq as u64,
                event_time_us: 7,
                payload: payload.into(),
            })
            .collect()
    }

    /// What a reader yields of the events it is handed, and where it could resume after each.
    fn read(events: impl IntoIterator<Item = Event>) -> (Vec<Vec<u8>>, Vec<u64>) {
        let (mut a, mut out, mut at) = (Assembly::default(), vec![], vec![]);
        for e in events {
            let next = e.seq + 1;
            out.extend(a.push(e).unwrap().map(|e| e.payload.to_vec()));
            at.push(a.boundary().unwrap_or(next));
        }
        (out, at)
    }

    fn payloads() -> impl Strategy<Value = Vec<Vec<u8>>> {
        let one = (any::<u8>(), 0usize..40).prop_map(|(b, n)| vec![b; n]);
        prop::collection::vec(one, 0..12)
    }

    proptest! {
        /// Why: an event of any size must come back as it was sent, byte for byte, whether it
        /// went as one chunk or many, and each chunk must fit the size it was cut for.
        #[test]
        fn any_events_round_trip_in_chunks_of_any_size(payloads in payloads(), max in 1usize..17) {
            let log = log_of(&payloads, max);
            prop_assert!(log.iter().all(|e| e.payload.len() <= max + 1), "a chunk and its tag");
            let (got, at) = read(log.clone());
            prop_assert_eq!(&got, &payloads);
            prop_assert_eq!(at.last().copied().unwrap_or(0), log.len() as u64, "all read");
        }

        /// Why: a reader resumes from a position it committed, or starts where retention left
        /// the stream, which may be inside an event. It must never yield a tail without its
        /// head: from any start it yields exactly the events that begin at or after it. And
        /// wherever it stops, the position it would commit is no later than the start of an
        /// event it has not yielded: resuming there loses nothing.
        #[test]
        fn a_reader_from_anywhere_yields_whole_events_and_resumes_at_an_event_s_start(
            payloads in payloads(), max in 1usize..17, start: prop::sample::Index, stop: prop::sample::Index,
        ) {
            let log = log_of(&payloads, max);
            // where each event begins in the log, and how many chunks it is
            let begins: Vec<(usize, usize)> = payloads.iter().scan(0, |at, p| {
                let n = chunks(p, max).count();
                *at += n;
                Some((*at - n, n))
            }).collect();
            let (start, stop) = (start.index(log.len() + 1), stop.index(log.len() + 1));
            let (from, to) = (start.min(stop), start.max(stop));
            let (got, at) = read(log[from..to].iter().cloned());
            let want: Vec<Vec<u8>> = (begins.iter().zip(&payloads))
                .filter(|((b, n), _)| *b >= from && b + n <= to)
                .map(|(_, p)| p.clone())
                .collect();
            prop_assert_eq!(&got, &want);
            // resuming at the committed position reads the rest, and no event twice
            let resume = at.last().copied().unwrap_or(from as u64) as usize;
            prop_assert!(resume <= to);
            let (rest, _) = read(log[resume..].iter().cloned());
            let after: Vec<Vec<u8>> = (begins.iter().zip(&payloads))
                .filter(|((b, _), _)| *b >= from)
                .map(|(_, p)| p.clone())
                .collect();
            prop_assert_eq!([got, rest].concat(), after);
        }

        /// Why: a reader does not see every chunk a producer sent: the broker drops an event's
        /// first chunk when its dedup id was seen (the others have none), retention cuts a
        /// stream inside an event, a producer dies between two chunks. Whatever is missing, the
        /// reader yields only events that were sent, each whole and in order: never the head
        /// of one with the tail of another.
        #[test]
        fn whatever_chunks_are_missing_a_reader_yields_only_events_that_were_sent(
            payloads in prop::collection::vec((1u8..250, 0usize..40), 0..12), max in 1usize..17,
            missing in prop::collection::vec(any::<bool>(), 0..200),
        ) {
            // (each event's bytes are its own: a mix of two would not equal either)
            let payloads: Vec<Vec<u8>> = (payloads.iter().enumerate())
                .map(|(i, (b, n))| [vec![i as u8; 1], vec![*b; *n]].concat())
                .collect();
            let log = log_of(&payloads, max);
            let seen = (log.into_iter().enumerate())
                .filter(|(i, _)| !missing.get(*i).copied().unwrap_or(false))
                .map(|x| x.1);
            let (got, _) = read(seen);
            let mut sent = payloads.iter();
            for e in &got {
                prop_assert!(sent.any(|p| p == e), "not sent, or out of order: {:?}", e);
            }
        }
    }

    #[test]
    fn an_event_past_the_readers_bound_is_refused_and_the_next_one_is_read() {
        // Why: a reader holds an event's chunks in memory until its last one. A writer that
        // never ends an event must not grow a reader without bound: past `MAX_EVENT_BYTES` the
        // event is refused (an error, once) and the reader goes on at the next event.
        let mut a = Assembly::default();
        let chunk = |seq: u64, tag: u8, n: usize| Event {
            stream: "s".into(),
            writer: "w".into(),
            epoch: 1,
            seq,
            event_time_us: 0,
            payload: [vec![tag], vec![0u8; n]].concat().into(),
        };
        let half = MAX_EVENT_BYTES / 2 + 1;
        assert!(a.push(chunk(0, FIRST, half)).unwrap().is_none());
        assert!(a.push(chunk(1, MIDDLE, half)).is_err());
        assert!(
            a.push(chunk(2, LAST, 1)).unwrap().is_none(),
            "its tail is passed over"
        );
        assert_eq!(a.boundary(), None);
        let next = a.push(chunk(3, WHOLE, 3)).unwrap().unwrap();
        assert_eq!((next.seq, next.payload.len()), (3, 3));
        // a tag no version of this reader wrote is an error too
        assert!(a.push(chunk(4, 9, 1)).is_err());
        assert!(
            a.push(Event {
                payload: Bytes::new(),
                ..chunk(5, 0, 0)
            })
            .is_err()
        );
    }
}
