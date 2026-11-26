//! A Precog cluster in Docker, for tests (feature `testkit`): nodes of the published image
//! (`ghcr.io/aperiodic-io/precog`), each a container on a network of its own, each in a failure
//! domain of its own. Nodes can be stopped and started again with their data, or started late.
//!
//! Nodes tell clients where a stream is by the addresses they know each other by, so a client
//! must reach every node at the address its peers use. Where the test runs decides what that is:
//! - on the host (the default): the nodes share the host's network (`--network host`), each on
//!   a port of its own on 127.0.0.1 (Linux hosts);
//! - in a container on a docker network (`PRECOG_TESTKIT_NETWORK=<network>`): the nodes join that
//!   network and are reached by their container names.
//!
//! The image is `PRECOG_TESTKIT_IMAGE`, else [`IMAGE`]. Every container is labelled
//! `precog-testkit=<cluster>` and removed when the [`Cluster`] is dropped.

use anyhow::{Context, Result, anyhow, bail, ensure};
use serde_json::{Map, Value, json};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::Command;
use std::time::{Duration, Instant};

/// The node image the tests run by default: the precog commit this client's protocol is of.
pub const IMAGE: &str = "ghcr.io/aperiodic-io/precog:sha-6e1d62ed572d305122574e8161f961b827bb7713";
/// The token every node takes, unless [`Options::token`] says otherwise.
pub const TOKEN: &str = "testkit";
const RPC: u16 = 7000;
const METRICS: u16 = 9464;

/// What a cluster is made of.
#[derive(Clone, Debug)]
pub struct Options {
    /// How many nodes the cluster has.
    pub nodes: usize,
    /// Those started at once ([`Cluster::start`] starts the others later). None: all of them.
    pub up: Option<Vec<usize>>,
    pub token: String,
    /// Each stream's config by name (a node's `streams`: `retention_bytes`, `retention_ms`,
    /// `compacted`, `copies`, `extents`, `pack`).
    pub streams: Map<String, Value>,
    /// Any other field of a node's config, over the defaults (`rpc_timeout_ms`,
    /// `repair_interval_ms`, `segment_size`, `dedup_window_ms`, ...).
    pub node: Map<String, Value>,
    /// Fields of one node's config only, over `node`, by node id.
    pub each: std::collections::BTreeMap<usize, Map<String, Value>>,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            nodes: 1,
            up: None,
            token: TOKEN.into(),
            streams: Map::new(),
            node: Map::new(),
            each: Default::default(),
        }
    }
}

impl Options {
    /// `n` nodes, all started.
    pub fn nodes(n: usize) -> Options {
        Options {
            nodes: n,
            ..Options::default()
        }
    }
    /// A stream's config.
    pub fn stream(mut self, name: &str, config: Value) -> Options {
        self.streams.insert(name.into(), config);
        self
    }
    /// A field of every node's config.
    pub fn set(mut self, key: &str, value: Value) -> Options {
        self.node.insert(key.into(), value);
        self
    }
    /// A field of node `i`'s config only.
    pub fn set_on(mut self, i: usize, key: &str, value: Value) -> Options {
        self.each.entry(i).or_default().insert(key.into(), value);
        self
    }
    /// Only these nodes started at once.
    pub fn up(mut self, which: &[usize]) -> Options {
        self.up = Some(which.to_vec());
        self
    }
}

/// A cluster's nodes, removed when it is dropped.
pub struct Cluster {
    /// Each node's address as a client gives it (`tcp://host:port`), by node id.
    pub nodes: Vec<String>,
    pub token: String,
    id: String,
    names: Vec<String>,
    /// The docker network the nodes are on: `host`, or the test's own.
    network: String,
    /// Each node's ports (rpc, metrics) on 127.0.0.1, when the nodes share the host's network.
    ports: Vec<Option<(u16, u16)>>,
    configs: Vec<String>,
    image: String,
    created: Vec<bool>,
}

fn docker(args: &[&str]) -> Result<String> {
    let out = Command::new("docker")
        .args(args)
        .output()
        .context("running docker")?;
    if !out.status.success() {
        bail!(
            "docker {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// A port for a node, free when it is picked and unique in this process. Below Linux's ephemeral
/// range (32768 on), so no outgoing connection (the client's own, a peer's) takes it while a
/// node of a cluster is not started yet; each process starts at its own place in the range.
fn free_port() -> Result<u16> {
    use std::sync::atomic::{AtomicU32, Ordering};
    const LOW: u32 = 20_000;
    const SPAN: u32 = 12_000;
    static NEXT: AtomicU32 = AtomicU32::new(u32::MAX);
    let _ = NEXT.compare_exchange(
        u32::MAX,
        (std::process::id().wrapping_mul(2_654_435_761)) % SPAN,
        Ordering::SeqCst,
        Ordering::SeqCst,
    );
    for _ in 0..SPAN {
        let port = (LOW + NEXT.fetch_add(1, Ordering::SeqCst) % SPAN) as u16;
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return Ok(port);
        }
    }
    bail!("no free port in {LOW}..{}", LOW + SPAN)
}

fn unique() -> String {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{:x}{:x}", std::process::id(), t.as_nanos() & 0xff_ffff)
}

impl Cluster {
    /// A cluster as `o` says, the nodes of `o.up` started and answering.
    pub fn new(o: Options) -> Result<Cluster> {
        ensure!(o.nodes >= 1, "a cluster has a node at least");
        let id = unique();
        let image = std::env::var("PRECOG_TESTKIT_IMAGE").unwrap_or_else(|_| IMAGE.into());
        let network = std::env::var("PRECOG_TESTKIT_NETWORK")
            .ok()
            .filter(|n| !n.is_empty());
        let names: Vec<String> = (0..o.nodes)
            .map(|i| format!("precog-testkit-{id}-n{i}"))
            .collect();
        let ports: Vec<Option<(u16, u16)>> = match network {
            Some(_) => vec![None; o.nodes],
            None => (0..o.nodes)
                .map(|_| Ok(Some((free_port()?, free_port()?))))
                .collect::<Result<_>>()?,
        };
        // (rpc, metrics) of each node: where it listens, and where clients and peers reach it
        let listen: Vec<(String, String)> = (0..o.nodes)
            .map(|i| match ports[i] {
                Some((rpc, m)) => (format!("127.0.0.1:{rpc}"), format!("127.0.0.1:{m}")),
                None => (format!("0.0.0.0:{RPC}"), format!("0.0.0.0:{METRICS}")),
            })
            .collect();
        let nodes: Vec<String> = (0..o.nodes)
            .map(|i| match ports[i] {
                Some((rpc, _)) => format!("tcp://127.0.0.1:{rpc}"),
                None => format!("tcp://{}:{RPC}", names[i]),
            })
            .collect();
        let configs = (0..o.nodes)
            .map(|i| config(&o, i, &nodes, &listen[i]).to_string())
            .collect();
        let network = network.unwrap_or_else(|| "host".into());
        let mut c = Cluster {
            nodes,
            token: o.token.clone(),
            id,
            names,
            network,
            ports,
            configs,
            image,
            created: vec![false; o.nodes],
        };
        let up = o.up.clone().unwrap_or_else(|| (0..o.nodes).collect());
        for &i in &up {
            c.launch(i)?;
        }
        for &i in &up {
            c.wait(i)?;
        }
        Ok(c)
    }

    fn launch(&mut self, i: usize) -> Result<()> {
        if self.created[i] {
            docker(&["start", &self.names[i]])?;
            return Ok(());
        }
        let label = format!("precog-testkit={}", self.id);
        let cfg = format!("PRECOG_TESTKIT_CONFIG={}", self.configs[i]);
        let mut args: Vec<String> = [
            "run",
            "-d",
            "--name",
            &self.names[i],
            "--hostname",
            &self.names[i],
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        args.extend([
            "--network".into(),
            self.network.clone(),
            "--label".into(),
            label,
        ]);
        args.extend(["-e".into(), cfg, "--entrypoint".into(), "sh".into()]);
        args.push(self.image.clone());
        args.extend([
            "-c".into(),
            "printf '%s' \"$PRECOG_TESTKIT_CONFIG\" > /tmp/node.json && exec precog-node serve --config /tmp/node.json".into(),
        ]);
        docker(&args.iter().map(String::as_str).collect::<Vec<_>>())?;
        self.created[i] = true;
        Ok(())
    }

    /// Waits until node `i` serves (`/health` 200), 60 s at most.
    pub fn wait(&self, i: usize) -> Result<()> {
        let at = self.metrics_at(i);
        let until = Instant::now() + Duration::from_secs(60);
        let mut last = String::new();
        while Instant::now() < until {
            match health(&at) {
                Ok(true) => return Ok(()),
                Ok(false) => last = "not serving yet".into(),
                Err(e) => last = e.to_string(),
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        let logs = docker(&["logs", "--tail", "20", &self.names[i]]).unwrap_or_default();
        Err(anyhow!(
            "node {i} did not serve within 60 s ({last}); its last lines:\n{logs}"
        ))
    }

    /// Starts node `i` (one not started yet, or stopped) and waits until it serves.
    pub fn start(&mut self, i: usize) -> Result<()> {
        self.launch(i)?;
        self.wait(i)
    }

    /// Stops node `i` (SIGTERM, then SIGKILL after 10 s); its data stays for its next start.
    pub fn stop(&self, i: usize) -> Result<()> {
        docker(&["stop", &self.names[i]]).map(drop)
    }

    /// Kills node `i` at once (SIGKILL); its data stays for its next start.
    pub fn kill(&self, i: usize) -> Result<()> {
        docker(&["kill", &self.names[i]]).map(drop)
    }

    /// Node `i`'s metrics, as Prometheus scrapes them (`/metrics`).
    pub fn metrics(&self, i: usize) -> Result<String> {
        let mut s = TcpStream::connect(self.metrics_at(i))?;
        s.set_read_timeout(Some(Duration::from_secs(5)))?;
        s.write_all(b"GET /metrics HTTP/1.0\r\nHost: node\r\n\r\n")?;
        let mut r = String::new();
        s.read_to_string(&mut r)?;
        let body = r.split_once("\r\n\r\n").map_or("", |(_, b)| b);
        Ok(body.to_string())
    }

    /// The value of the metric line that starts with `series ` (its name and labels) on node
    /// `i`, summed over the lines with that start.
    pub fn metric(&self, i: usize, series: &str) -> Result<f64> {
        let m = self.metrics(i)?;
        let head = format!("{series} ");
        let vals: Vec<f64> = m
            .lines()
            .filter_map(|l| l.strip_prefix(&head))
            .filter_map(|v| v.trim().parse().ok())
            .collect();
        ensure!(!vals.is_empty(), "no {series} on node {i}");
        Ok(vals.iter().sum())
    }

    fn metrics_at(&self, i: usize) -> String {
        match self.ports[i] {
            Some((_, m)) => format!("127.0.0.1:{m}"),
            None => format!("{}:{METRICS}", self.names[i]),
        }
    }

    /// Node `i`'s container, for `docker` commands of a test's own (pause, logs).
    pub fn container(&self, i: usize) -> &str {
        &self.names[i]
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let made: Vec<&str> = (0..self.names.len())
            .filter(|&i| self.created[i])
            .map(|i| self.names[i].as_str())
            .collect();
        if !made.is_empty() {
            let mut args = vec!["rm", "-f", "-v"];
            args.extend(made);
            let _ = docker(&args);
        }
    }
}

/// Node `i`'s config: what `precog-node init` writes, with the testkit's addresses (`nodes`, by
/// id, as clients and peers reach them; `listen`, rpc and metrics) and `o`'s fields over it.
fn config(o: &Options, i: usize, nodes: &[String], listen: &(String, String)) -> Value {
    let peers: Vec<Value> = (0..nodes.len())
        .filter(|&j| j != i)
        .map(|j| json!({"id": j, "domain": format!("d{j}"), "addr": nodes[j]}))
        .collect();
    let mut c = json!({
        "id": i,
        "domain": format!("d{i}"),
        "data_dir": "/tmp/precog",
        "token": o.token,
        "tokens": [],
        "peer_token": null,
        "listen_tcp": listen.0,
        "listen_metrics": listen.1,
        "tls": null,
        "peers": peers,
        "target_copies": nodes.len().min(3),
        "rpc_timeout_ms": 1000,
        "repair_interval_ms": 1000,
        "segment_size": 1 << 20,
        "retention_bytes": null,
        "retention_ms": null,
        "copy_ratio": null,
        "retention_interval_ms": 10000,
        "flush_delay_ms": 0,
        "flush_bytes": 262144,
        "tail_cache_bytes": 16 << 20,
        "dedup_window_ms": 300000,
        "dedup_max_entries": 1_000_000,
        "streams": o.streams,
        "dormant_after_ms": 600000,
        "hot_streams_bytes": 64 << 20,
        "control": false,
        "summary_cache_bytes": 16 << 20,
        "recovery_progress_ms": 5000,
        "inflight_bytes": 64 << 20,
        "packs": null
    });
    let m = c.as_object_mut().expect("an object");
    for (k, v) in o.node.iter().chain(o.each.get(&i).into_iter().flatten()) {
        m.insert(k.clone(), v.clone());
    }
    c
}

/// Whether the node's metrics port answers `/health` with 200.
fn health(at: &str) -> Result<bool> {
    let mut s = TcpStream::connect(at)?;
    s.set_read_timeout(Some(Duration::from_secs(2)))?;
    s.write_all(b"GET /health HTTP/1.0\r\nHost: node\r\n\r\n")?;
    let mut r = String::new();
    s.read_to_string(&mut r)?;
    Ok(r.starts_with("HTTP/1.1 200") || r.starts_with("HTTP/1.0 200"))
}
