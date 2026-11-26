//! Why: the client is of use only if it speaks the protocol of the nodes that run: one event
//! produced through it is read back through it from a node of the published image.
use precog_client::testkit::{Cluster, Options};
use precog_client::{Client, Start};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_event_produced_is_read_back_from_a_node_of_the_published_image() {
    let c = tokio::task::spawn_blocking(|| Cluster::new(Options::nodes(1)))
        .await
        .unwrap()
        .unwrap();
    let client = Client::connect(&c.nodes, &c.token).unwrap();
    let mut p = client.producer("smoke", "w1");
    p.send(1, b"id-1", b"hello");
    p.flush().await.unwrap();
    let mut r = client
        .consumer("smoke", "g", Start::Beginning)
        .await
        .unwrap();
    let mut got = vec![];
    for _ in 0..50 {
        got.extend(r.poll(1 << 20).await.unwrap());
        if !got.is_empty() {
            break;
        }
    }
    assert_eq!(got.len(), 1);
    assert_eq!(&got[0].payload[..], b"hello");
}
