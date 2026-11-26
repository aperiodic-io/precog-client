//! Connectivity: TCP to `tcp://<host>:<port>`, or TLS over it to `tls://<host>:<port>`, where
//! host is an IP address or a DNS name (resolved at every dial, so a StatefulSet pod that moves
//! keeps its name). TLS is rustls: the server's certificate is checked against the process's
//! trusted CAs (`trust`) and the address's host; clients are known by their token, not by a
//! certificate.

use crate::rpc::RpcClient;
use anyhow::{Context, Result, anyhow, bail, ensure};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use tokio_rustls::rustls;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject};
use tokio_rustls::rustls::sign::CertifiedKey;

/// `host:port` and whether it is dialed with TLS, checked when parsed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Addr {
    tls: bool,
    at: String,
}

impl Addr {
    pub fn is_tls(&self) -> bool {
        self.tls
    }

    /// The host as a certificate names it: without the port, and an IPv6 address's brackets.
    fn host(&self) -> &str {
        let host = self.at.rsplit_once(':').map_or(&self.at[..], |x| x.0);
        host.trim_start_matches('[').trim_end_matches(']')
    }
}

impl FromStr for Addr {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Addr> {
        let (tls, rest) = match (s.strip_prefix("tcp://"), s.strip_prefix("tls://")) {
            (Some(rest), _) => (false, rest),
            (_, Some(rest)) => (true, rest),
            _ => bail!("address must start with tcp:// or tls://: {s}"),
        };
        match rest.rsplit_once(':') {
            Some((host, port)) if !host.is_empty() && port.parse::<u16>().is_ok() => Ok(Addr {
                tls,
                at: rest.to_string(),
            }),
            _ => bail!("bad address (want tcp://host:port or tls://host:port): {s}"),
        }
    }
}

impl std::fmt::Display for Addr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let scheme = if self.tls { "tls" } else { "tcp" };
        write!(f, "{scheme}://{}", self.at)
    }
}

/// The CAs a `tls://` address is checked against.
/// ponytail: one trust store a process; per client if one process ever talks to two clusters
/// with different CAs.
static TRUST: RwLock<Option<Arc<rustls::ClientConfig>>> = RwLock::new(None);

/// rustls with ring, named here: the workspace also builds rustls with another provider.
fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Trusts the CA certificates in the PEM file `ca` for every `tls://` address this process
/// dials from now on (in place of those trusted before).
pub fn trust(ca: &Path) -> Result<()> {
    let mut roots = rustls::RootCertStore::empty();
    let certs = CertificateDer::pem_file_iter(ca).with_context(|| format!("{}", ca.display()))?;
    for c in certs {
        roots.add(c.with_context(|| format!("{}", ca.display()))?)?;
    }
    ensure!(!roots.is_empty(), "no certificate in {}", ca.display());
    let cfg = rustls::ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    *TRUST.write().unwrap() = Some(Arc::new(cfg));
    Ok(())
}

/// Outbound side shared by nodes (peer links) and clients. `timeout` covers the TCP connect
/// and, for `tls://`, the handshake.
pub async fn connect(addr: &Addr, timeout: Duration) -> Result<RpcClient> {
    let dial = async {
        let s = tokio::net::TcpStream::connect(addr.at.as_str()).await?;
        s.set_nodelay(true)?;
        if !addr.tls {
            let (r, w) = s.into_split();
            return Ok(RpcClient::start(Box::new(r), Box::new(w)));
        }
        let cfg = TRUST.read().unwrap().clone();
        let cfg = cfg.with_context(|| format!("{addr}: no CA to check it against (`trust`)"))?;
        let name = ServerName::try_from(addr.host().to_string())?;
        let s = tokio_rustls::TlsConnector::from(cfg)
            .connect(name, s)
            .await?;
        let (r, w) = tokio::io::split(s);
        anyhow::Ok(RpcClient::start(Box::new(r), Box::new(w)))
    };
    tokio::time::timeout(timeout, dial)
        .await
        .map_err(|_| anyhow!("connect to {addr} timed out"))?
        .with_context(|| format!("connect to {addr}"))
}

/// When the certificate and key files were last modified (None: not there).
type Stamp = [Option<SystemTime>; 2];

/// A node's certificate chain and key from PEM files, read again when either file changes:
/// each handshake looks at the files' modification times. A pair that does not load, or whose
/// halves do not belong together (one file replaced, the other not yet), leaves the pair it
/// has serving.
#[derive(Debug)]
struct Reloading {
    files: [PathBuf; 2],
    /// The pair it serves, and the files' times when it last tried to load them.
    held: parking_lot::Mutex<(Stamp, Arc<CertifiedKey>)>,
}

impl Reloading {
    fn stamp(files: &[PathBuf; 2]) -> Stamp {
        let at = |p: &PathBuf| std::fs::metadata(p).and_then(|m| m.modified()).ok();
        [at(&files[0]), at(&files[1])]
    }

    fn load([cert, key]: &[PathBuf; 2]) -> Result<CertifiedKey> {
        let chain = CertificateDer::pem_file_iter(cert)
            .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
            .with_context(|| format!("{}", cert.display()))?;
        ensure!(!chain.is_empty(), "no certificate in {}", cert.display());
        let key =
            PrivateKeyDer::from_pem_file(key).with_context(|| format!("{}", key.display()))?;
        // (refuses a key that is not the certificate's)
        Ok(CertifiedKey::from_der(chain, key, &provider())?)
    }
}

impl rustls::server::ResolvesServerCert for Reloading {
    fn resolve(&self, _: rustls::server::ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let now = Self::stamp(&self.files);
        let mut held = self.held.lock();
        if held.0 != now {
            held.0 = now;
            match Self::load(&self.files) {
                Ok(pair) => held.1 = Arc::new(pair),
                Err(e) => {
                    tracing::error!("the new certificate is not served, the old one is: {e:#}")
                }
            }
        }
        Some(held.1.clone())
    }
}

/// What a node's listener accepts TLS with: the certificate and key in the PEM files `cert`
/// and `key`, replaced without a restart when the files are (`Reloading`).
pub fn acceptor(cert: &Path, key: &Path) -> Result<tokio_rustls::TlsAcceptor> {
    let files = [cert.to_path_buf(), key.to_path_buf()];
    let first = Reloading::load(&files).context("the node's certificate and key")?;
    let held = parking_lot::Mutex::new((Reloading::stamp(&files), Arc::new(first)));
    let cfg = rustls::ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_cert_resolver(Arc::new(Reloading { files, held }));
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(cfg)))
}

#[cfg(test)]
mod props {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// Why: an address is config an operator writes and a node dials for as long as it
        /// runs. One that parses prints back as it was written, keeps its scheme (a `tls://`
        /// peer is never dialed in the clear), and nothing else parses: no other scheme, no
        /// missing host or port.
        #[test]
        fn an_address_keeps_its_scheme_host_and_port_and_nothing_else_parses(
            scheme in prop_oneof![Just("tcp"), Just("tls"), Just("udp"), Just("")],
            host in "[a-z0-9.\\[\\]:-]{0,12}",
            port in prop_oneof![(0u32..70_000).prop_map(|p| p.to_string()), "[a-z]{0,3}"],
        ) {
            let s = format!("{scheme}://{host}:{port}");
            let ok = matches!(scheme, "tcp" | "tls") && !host.is_empty() && port.parse::<u16>().is_ok();
            match s.parse::<Addr>() {
                Ok(a) => {
                    prop_assert!(ok, "{} parsed", s);
                    prop_assert_eq!(a.to_string(), s);
                    prop_assert_eq!(a.is_tls(), scheme == "tls");
                }
                Err(_) => prop_assert!(!ok, "{} did not parse", s),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses_are_ip_or_dns_names_with_a_port() {
        for ok in [
            "tcp://127.0.0.1:7001",
            "tcp://precog-1.precog.live-ingestion.svc.cluster.local:7000",
            "tcp://[::1]:7000",
        ] {
            let a: Addr = ok.parse().unwrap();
            assert_eq!(a.to_string(), ok);
        }
        let secure: Addr = "tls://precog-1.example:7000".parse().unwrap();
        assert_eq!((secure.is_tls(), secure.host()), (true, "precog-1.example"));
        assert_eq!("tls://[::1]:7000".parse::<Addr>().unwrap().host(), "::1");
        for bad in [
            "tcp://precog-1",
            "tcp://host:port",
            "tcp://:7000",
            "udp://1.2.3.4:5",
            "tcp://h:70000",
        ] {
            assert!(bad.parse::<Addr>().is_err(), "{bad}");
        }
    }

    #[tokio::test]
    async fn a_dns_name_is_resolved_when_dialing() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        tokio::spawn(async move { while l.accept().await.is_ok() {} });
        let a: Addr = format!("tcp://localhost:{port}").parse().unwrap();
        connect(&a, Duration::from_secs(5)).await.unwrap();
    }
}
