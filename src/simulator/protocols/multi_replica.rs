use std::collections::HashMap;

use crate::simulator::algorithms::riblt::RatelessIBLT;
use crate::simulator::network::{ProtocolMsg, Outbox};
use crate::simulator::protocols::{
    LocalMetrics, PendingElements, Protocol, ProtocolKind, ProtocolStepResult,
};
use crate::simulator::replica::{Element, ReplicaView};
use crate::simulator::topology::Topology;

/// Multi-round RIBLT reconciliation.
///
/// Alternates between sketch rounds (send RIBLT to all neighbours) and
/// delivery rounds (send the missing elements identified last sketch round).
/// Repeats until the engine detects full convergence. Each new sketch round
/// uses the current (grown) set, so newly-received elements propagate
/// transitively through the topology.
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
        carry: Option<PendingElements>,
    ) {
        let pending = carry.unwrap_or_default();

        if pending.is_empty() {
            // Sketch phase: send RIBLT of current set to every neighbour.
            let digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();
            for &nbr in topology.neighbors(local.id) {
                let riblt = RatelessIBLT::riblt_from(digests.iter().copied());
                outbox.send_riblt(nbr, riblt);
            }
        } else {
            // Delivery phase: forward elements identified last round.
            for (nbr, elements) in pending {
                if !elements.is_empty() {
                    outbox.send_elements(nbr, elements);
                }
            }
        }
    }

    fn recv_phase(
        &self,
        local: ReplicaView<'_>,
        _topology: &Topology,
        inbox: &mut [(usize, ProtocolMsg)],
    ) -> ProtocolStepResult {
        let mut next_set = local.snapshot_set();
        let local_digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();
        let mut pending: PendingElements = HashMap::new();

        for (from, msg) in inbox.iter_mut() {
            if let Some(els) = msg.take_elements() {
                for e in els {
                    next_set.insert(e);
                }
                continue;
            }

            if let Some(riblt_msg) = msg.as_riblt() {
                let mut local_riblt = RatelessIBLT::riblt_from(local_digests.iter().copied());
                riblt_msg.decode_against(&mut local_riblt);

                let local_only: std::collections::HashSet<u64> =
                    riblt_msg.remote_only().into_iter().collect();
                let to_send: Vec<Element> = local
                    .set
                    .iter()
                    .filter(|e| local_only.contains(&e.digest))
                    .cloned()
                    .collect();
                if !to_send.is_empty() {
                    pending.entry(*from).or_default().extend(to_send);
                }
            }
        }

        ProtocolStepResult {
            next_set,
            metrics: LocalMetrics::default(),
            carry: if pending.is_empty() { None } else { Some(pending) },
        }
    }
}
