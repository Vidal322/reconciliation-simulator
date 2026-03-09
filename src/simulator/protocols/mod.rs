pub mod full_state_transfer;

use std::collections::HashSet;
use std::fmt;

use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolKind {
    FullStateTransfer,
    HybridRbfRiblt,
    Riblt,
    StaticBfIblt,
}

impl fmt::Display for ProtocolKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ProtocolKind::FullStateTransfer => "FullStateTransfer",
            ProtocolKind::HybridRbfRiblt => "HybridRbfRiblt",
            ProtocolKind::Riblt => "Riblt",
            ProtocolKind::StaticBfIblt => "StaticBfIblt",
        };
        write!(f, "{s}")
    }
}
pub trait Protocol {
    fn kind(&self) -> ProtocolKind;

    fn next_set(
        &self,
        replica_id: usize,
        replicas: &[Replica],
        topology: &Topology,
    ) -> HashSet<Element>;
}
