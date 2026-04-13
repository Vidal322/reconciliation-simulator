use crate::simulator::network::{RecvView, SendView};
use crate::simulator::protocols::messages::{ProtocolMsg, SimulatorHint};
use crate::simulator::protocols::{LocalMetrics, Protocol, ProtocolKind, ProtocolStepResult};
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

/// Agent-target protocol. Initial implementation: full state transfer.
/// The autoresearch agent will iteratively improve this file to reduce
/// bandwidth while maintaining convergence.
pub struct MultiReplicaV2Protocol;

impl MultiReplicaV2Protocol {
    pub fn new() -> Self {
        Self
    }
}

impl Protocol for MultiReplicaV2Protocol {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::MultiReplicaV2
    }

    fn send_phase(
        &mut self,
        replica_id: usize,
        local: &Replica,
        topology: &Topology,
        network: &mut SendView<ProtocolMsg>,
    ) {
        let payload: Vec<Element> = local.set.iter().cloned().collect();
        for &neighbor_id in topology.neighbors(replica_id) {
            network.send(
                replica_id,
                neighbor_id,
                ProtocolMsg::Elements(payload.clone()),
                SimulatorHint::None,
            );
        }
    }

    fn recv_phase(
        &mut self,
        _replica_id: usize,
        local: &Replica,
        _topology: &Topology,
        inbox: Vec<(usize, ProtocolMsg, SimulatorHint)>,
        _network: &mut RecvView<ProtocolMsg>,
    ) -> ProtocolStepResult {
        let mut next_set = local.snapshot_set();
        for (_from, msg, _hint) in inbox {
            if let ProtocolMsg::Elements(els) = msg {
                for element in els {
                    next_set.insert(element);
                }
            }
        }

        ProtocolStepResult {
            next_set,
            metrics: LocalMetrics::default(),
        }
    }
}
