use crate::simulator::network::{ProtocolMsg, Outbox};
use crate::simulator::protocols::{
    LocalMetrics, PendingElements, Protocol, ProtocolKind, ProtocolStepResult,
};
use crate::simulator::replica::{Element, ReplicaView};
use crate::simulator::topology::Topology;

/// Agent-target protocol. Initial implementation: full state transfer.
/// The autoresearch agent will iteratively improve this file to reduce
/// bandwidth while maintaining convergence.
pub struct MultiReplicaProtocol;

impl MultiReplicaProtocol {
    pub fn new() -> Self {
        Self
    }
}

impl Protocol for MultiReplicaProtocol {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::MultiReplica
    }

    fn send_phase(
        &self,
        local: ReplicaView<'_>,
        topology: &Topology,
        outbox: &mut Outbox<'_>,
        _carry: Option<PendingElements>,
    ) {
        let payload: Vec<Element> = local.set.iter().cloned().collect();
        for &neighbor_id in topology.neighbors(local.id) {
            outbox.send_elements(neighbor_id, payload.clone());
        }
    }

    fn recv_phase(
        &self,
        local: ReplicaView<'_>,
        _topology: &Topology,
        inbox: &mut [(usize, ProtocolMsg)],
    ) -> ProtocolStepResult {
        let mut next_set = local.snapshot_set();
        for (_from, msg) in inbox.iter_mut() {
            if let Some(els) = msg.take_elements() {
                for element in els {
                    next_set.insert(element);
                }
            }
        }

        ProtocolStepResult {
            next_set,
            metrics: LocalMetrics::default(),
            carry: None,
        }
    }
}
