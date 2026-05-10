use std::collections::HashMap;

use crate::simulator::algorithms::bloom::BloomFilter;
use crate::simulator::network::{ProtocolMsg, Outbox};
use crate::simulator::protocols::{
    LocalMetrics, PendingElements, Protocol, ProtocolKind, ProtocolStepResult,
};
use crate::simulator::replica::{Element, ReplicaView};
use crate::simulator::topology::Topology;

/// Multi-round BloomFilter reconciliation (no RIBLT).
///
/// Sketch rounds: each node broadcasts a BloomFilter of its current set to
/// all neighbours. Each receiver finds the elements it has that the sender's
/// BF doesn't contain — those are sent back in the next delivery round.
/// Because RandomState reseeds each BF, false positives are independent
/// across rounds and resolve within a few cycles.
pub struct MultiReplicaProtocol {
    fpr: f64,
}

impl MultiReplicaProtocol {
    pub fn new() -> Self {
        Self { fpr: 0.01 }
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
            // Sketch phase: broadcast a fresh BloomFilter to every neighbour.
            let n = local.set.len().max(1);
            for &nbr in topology.neighbors(local.id) {
                let mut bf: BloomFilter<u64> = BloomFilter::new(n, self.fpr);
                for e in local.set.iter() {
                    bf.insert(&e.digest);
                }
                outbox.send_bloom(nbr, bf);
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
        let mut pending: PendingElements = HashMap::new();

        for (from, msg) in inbox.iter_mut() {
            if let Some(els) = msg.take_elements() {
                for e in els {
                    next_set.insert(e);
                }
                continue;
            }

            if let Some(bf) = msg.as_bloom() {
                // Elements I have that are NOT in sender's BF → sender is missing them.
                let to_send: Vec<Element> = local
                    .set
                    .iter()
                    .filter(|e| !bf.contains(&e.digest))
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
