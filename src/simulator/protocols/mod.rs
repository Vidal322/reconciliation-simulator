pub mod bf_iblt;
pub mod full_state_transfer;
pub mod hybrid_rbf_riblt;
pub mod multi_replica;
pub mod riblt;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::simulator::network::{ProtocolMsg, Outbox};
use crate::simulator::replica::{Element, ReplicaView};
use crate::simulator::topology::Topology;

/// Per-replica, cross-round carry state. Produced by `recv_phase` of round N
/// and handed back to `send_phase` of round N+1 for the *same* replica.
/// Routed exclusively by the engine; protocols never see another replica's
/// carry.
pub type PendingElements = HashMap<usize, Vec<Element>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtocolKind {
    FullStateTransfer,
    HybridRbfRiblt,
    MultiReplica,
    Riblt,
    StaticBfIblt,
}

impl fmt::Display for ProtocolKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ProtocolKind::FullStateTransfer => "FullStateTransfer",
            ProtocolKind::HybridRbfRiblt => "HybridRbfRiblt",
            ProtocolKind::MultiReplica => "MultiReplica",
            ProtocolKind::Riblt => "Riblt",
            ProtocolKind::StaticBfIblt => "StaticBfIblt",
        };
        write!(f, "{s}")
    }
}

pub trait Protocol {
    fn kind(&self) -> ProtocolKind;

    fn send_phase(
        &self,
        local: ReplicaView<'_>,
        topology: &Topology,
        outbox: &mut Outbox<'_>,
        carry: Option<PendingElements>,
    );

    fn recv_phase(
        &self,
        local: ReplicaView<'_>,
        topology: &Topology,
        inbox: &mut [(usize, ProtocolMsg)],
    ) -> ProtocolStepResult;
}

#[derive(Clone, Debug, Default)]
pub struct ProtocolStepResult {
    pub next_set: HashSet<Element>,
    pub metrics: LocalMetrics,
    pub carry: Option<PendingElements>,
}

#[derive(Clone, Debug, Default)]
pub struct LocalMetrics {
    pub encode_time: Duration,
    pub decode_time: Duration,
    pub false_matches: usize,
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use crate::simulator::replica::{Element, Replica};
    use std::collections::HashSet;

    pub fn make_element(digest: u64, payload_byte: u8, payload_len: usize) -> Element {
        Element::new(digest, vec![payload_byte; payload_len])
    }

    pub fn make_replica(id: usize, digests: &[u64]) -> Replica {
        let set = digests
            .iter()
            .map(|&d| make_element(d, d as u8, 4))
            .collect::<HashSet<_>>();
        Replica::new(id, set)
    }
}
