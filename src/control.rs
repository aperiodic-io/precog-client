//! The control plane's commands and a key's scope, as the nodes take and give them (JSON).
//! The same types as precog's `control` module: only their wire form, none of its state.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Command {
    CreateTenant {
        id: String,
    },
    /// Its keys stop at once, and its id is no tenant's again. Its streams stay as they are:
    /// they are deleted by whoever sent this, as any stream is (`Control::Delete`).
    DeleteTenant {
        id: String,
    },
    IssueKey {
        tenant: String,
        key: String,
        /// The digest of the key's secret.
        digest: [u8; 32],
        read: bool,
        write: bool,
        /// The key is not taken from this time on (ms since the epoch).
        expires_ms: Option<u64>,
    },
    RevokeKey {
        tenant: String,
        key: String,
    },
    /// What the tenant's streams are kept by, and its rate.
    SetClass {
        tenant: String,
        class: Class,
    },
}

/// A tenant's streams' class: the part of `StreamConfig` a tenant has, and its append rate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Class {
    pub retention_ms: Option<u64>,
    pub extents: bool,
    pub append_bytes_per_s: Option<u64>,
    /// What the tenant may store, in bytes on the nodes' disks, every copy counted: a
    /// gateway takes no append from a tenant that stores this much (`gateway::Keys::over`).
    #[serde(default)]
    pub stored_bytes: Option<u64>,
    /// The tenant's sessions leave the nodes' disks for the object store past their age
    /// limit (`StreamConfig::pack`).
    #[serde(default)]
    pub pack: bool,
}

/// What a key lets its holder do.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scope {
    /// The tenant's streams: every name that starts with this.
    pub prefix: String,
    pub read: bool,
    pub write: bool,
    pub class: Class,
}
