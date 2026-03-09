use std::collections::HashSet;

use crate::simulator::protocols::{Protocol, ProtocolKind};
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

#[derive(Clone, Debug, Default)]
pub struct FullStateTransfer;

impl FullStateTransfer {
    pub fn new() -> Self {
        Self
    }
}

impl Protocol for FullStateTransfer {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::FullStateTransfer
    }

    fn next_set(
        &self,
        replica_id: usize,
        replicas: &[Replica],
        topology: &Topology,
    ) -> HashSet<Element> {
        let mut merged = replicas[replica_id].snapshot_set();

        for &neighbor_id in topology.neighbors(replica_id) {
            merged.extend(replicas[neighbor_id].set.iter().cloned());
        }

        merged
    }
}
