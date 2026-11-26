# precog-client

Open-source clients for [Precog](https://github.com/aperiodic-io/precog)'s native protocol, in
Rust and Go:

- **Producers** batch the events of one writer log. Each event carries a dedup key, so two
  producer replicas' copies of an event are stored once. An append is durable on two nodes,
  and a failed append is retried with the same bytes on the next node, which is idempotent.
- **Consumers** read a stream from a cursor that is valid on every node, failing over between
  nodes. They start at a group's committed position, the beginning, the end or a time, and
  commit where they are.
- **Compacted streams** serve the newest value of every key (`latest`).

Both clients speak the protocol of the Precog commit in `testkit::IMAGE`, and their tests run
against nodes of that commit's published image, `ghcr.io/aperiodic-io/precog:sha-<commit>`.

## Rust

```toml
[dependencies]
precog-client = { git = "https://github.com/aperiodic-io/precog-client", rev = "<commit>" }
```

```rust
use precog_client::{Client, Start};

let client = Client::connect(&["tcp://node-0:7000", "tcp://node-1:7000"], "token")?;

let mut p = client.producer("raw.trades.x", "producer-a");
p.send(time_us, b"trade-123", payload);
p.sync().await?; // every event sent so far is acknowledged

let mut c = client.consumer("raw.trades.x", "archiver", Start::Committed { or: Box::new(Start::Beginning) }).await?;
let events = c.poll(1 << 20).await?;
c.commit().await?;
```

`precog_client::{LogId, Addr, Cursor}` are the types a caller keeps: a writer log's id, a node
address and a reader's position. `placement::nodes_of` is the placement every node computes.

### Tests against real nodes: the `testkit` feature

```toml
[dev-dependencies]
precog-client = { git = "https://github.com/aperiodic-io/precog-client", rev = "<commit>", features = ["testkit"] }
```

```rust
use precog_client::testkit::{Cluster, Options};
use serde_json::json;

let mut c = Cluster::new(
    Options::nodes(3)
        .up(&[0, 1]) // node 2 starts later
        .stream("state", json!({"compacted": true}))
        .set("rpc_timeout_ms", json!(300)),
)?;
let client = Client::connect(&c.nodes, &c.token)?;
c.start(2)?; // started, and answering
c.stop(0)?;  // its data kept for its next start
```

The testkit runs each node in a container of the published image and removes them when the
`Cluster` is dropped. It needs `docker` on the `PATH` and a Linux host.
- **On the host** (the default), the nodes share the host's network, each on a port of its own.
- **In a container on a docker network**, set `PRECOG_TESTKIT_NETWORK=<network>`. The nodes
  join that network and are reached by name.
- **Another image** can be set with `PRECOG_TESTKIT_IMAGE`.

## Go

```sh
go get github.com/aperiodic-io/precog-client/go
```

```go
import precog "github.com/aperiodic-io/precog-client/go"

cl, err := precog.Connect([]string{"tcp://node-0:7000"}, "token")
p := cl.Producer("raw.trades.x", "producer-a")
p.Send(timeUs, []byte("trade-123"), payload)
err = p.Sync(ctx)

c, err := cl.Consumer(ctx, "raw.trades.x", "archiver", precog.Committed(precog.Beginning()))
events, err := c.Poll(ctx, 1<<20)
err = c.Commit(ctx)
```

The Go tests run a real cluster of `precog-node` processes. They take the binary from
`$PRECOG_NODE`, and CI copies it out of the published image.

## Development

```sh
cargo test --all-features              # unit, property and model tests; Docker for the clusters
cd go && PRECOG_NODE=/path/to/precog-node go test ./...
```

The code comes from Precog's own SDK (`crates/client`) and the client side of its node crate:
wire formats, RPC, transport, chunking, extents and placement. Nothing of the node itself is
included. A change to the protocol is made in Precog first; this repository then follows the
commit whose image it tests against.

## License

Apache-2.0
