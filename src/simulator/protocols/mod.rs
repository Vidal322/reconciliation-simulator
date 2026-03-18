pub mod bf_iblt;
pub mod full_state_transfer;
pub mod hybrid_rbf_riblt;
pub mod riblt;
pub mod topology_aware_cbf_riblt;
pub mod topology_aware_riblt;

use std::collections::HashSet;
use std::fmt;
use std::time::Duration;

use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolKind {
    FullStateTransfer,
    HybridRbfRiblt,
    Riblt,
    StaticBfIblt,
    TopologyAwareCbfRiblt,
    TopologyAwareRiblt,
}

impl fmt::Display for ProtocolKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ProtocolKind::FullStateTransfer => "FullStateTransfer",
            ProtocolKind::HybridRbfRiblt => "HybridRbfRiblt",
            ProtocolKind::Riblt => "Riblt",
            ProtocolKind::StaticBfIblt => "StaticBfIblt",
            ProtocolKind::TopologyAwareCbfRiblt => "TopologyAwareCbfRiblt",
            ProtocolKind::TopologyAwareRiblt => "TopologyAwareRiblt",
        };
        write!(f, "{s}")
    }
}

#[derive(Clone, Debug, Default)]
pub struct ProtocolMetrics {
    pub state_bytes: usize,
    pub metadata_bytes: usize,
    pub encode_time: Duration,
    pub decode_time: Duration,
    pub false_matches: usize,
}

#[derive(Clone, Debug, Default)]
pub struct ProtocolStepResult {
    pub next_set: HashSet<Element>,
    pub metrics: ProtocolMetrics,
}

pub trait Protocol {
    fn kind(&self) -> ProtocolKind;

    fn step_replica(
        &self,
        replica_id: usize,
        replicas: &[Replica],
        topology: &Topology,
    ) -> ProtocolStepResult;
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
