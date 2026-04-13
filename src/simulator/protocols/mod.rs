pub mod bf_iblt;
pub mod full_state_transfer;
pub mod hybrid_rbf_riblt;
pub mod messages;
pub mod multi_replica_v2;
pub mod riblt;

use std::collections::HashSet;
use std::fmt;
use std::time::Duration;

use crate::simulator::network::{RecvView, SendView};
use crate::simulator::protocols::messages::{ProtocolMsg, SimulatorHint};
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolKind {
    FullStateTransfer,
    HybridRbfRiblt,
    MultiReplicaV2,
    Riblt,
    StaticBfIblt,
}

impl fmt::Display for ProtocolKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ProtocolKind::FullStateTransfer => "FullStateTransfer",
            ProtocolKind::HybridRbfRiblt => "HybridRbfRiblt",
            ProtocolKind::MultiReplicaV2 => "MultiReplicaV2",
            ProtocolKind::Riblt => "Riblt",
            ProtocolKind::StaticBfIblt => "StaticBfIblt",
        };
        write!(f, "{s}")
    }
}

pub trait Protocol {
    fn kind(&self) -> ProtocolKind;

    fn send_phase(
        &mut self,
        replica_id: usize,
        local: &Replica,
        topology: &Topology,
        network: &mut SendView<ProtocolMsg>,
    );

    fn recv_phase(
        &mut self,
        replica_id: usize,
        local: &Replica,
        topology: &Topology,
        inbox: Vec<(usize, ProtocolMsg, SimulatorHint)>,
        network: &mut RecvView<ProtocolMsg>,
    ) -> ProtocolStepResult;
}

#[derive(Clone, Debug, Default)]
pub struct ProtocolStepResult {
    pub next_set: HashSet<Element>,
    pub metrics: LocalMetrics,
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
