use std::collections::HashMap;

use crate::simulator::algorithms::rateless_bloom::bayesian_cost::RATELESS_SET_RECONCILIATION_OVERHEAD;
use crate::simulator::algorithms::rateless_bloom::expected_cost::ExpectedCostFactory;
use crate::simulator::algorithms::rateless_bloom::{RatelessBF, StoppingStrategyFactory};
use crate::simulator::algorithms::riblt::RatelessIBLT;
use crate::simulator::network::{ProtocolMsg, Outbox};
use crate::simulator::protocols::{
    LocalMetrics, PendingElements, Protocol, ProtocolKind, ProtocolStepResult,
};
use crate::simulator::replica::{Element, ReplicaView};
use crate::simulator::topology::Topology;

/// Multi-round BF+RIBLT reconciliation.
///
/// Alternates between sketch rounds (send RatelessBF+RIBLT to all neighbours)
/// and delivery rounds (send the missing elements identified in the previous
/// sketch round). Repeats until the engine detects full convergence.
pub struct MultiReplicaProtocol {
    m_ratio: f64,
}

impl MultiReplicaProtocol {
    pub fn new() -> Self {
        Self { m_ratio: 1.0 }
    }

    fn bloom_bits_for(&self, n: usize) -> usize {
        let requested = ((n as f64 * self.m_ratio).ceil() as usize).max(1);
        let minimum = RATELESS_SET_RECONCILIATION_OVERHEAD * 8;
        requested.max(minimum)
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
            // Sketch phase: broadcast BF+RIBLT to every neighbour.
            let digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();
            let bloom_bits = self.bloom_bits_for(digests.len());
            for &nbr in topology.neighbors(local.id) {
                let bf = RatelessBF::new(digests.clone(), bloom_bits);
                let riblt = RatelessIBLT::riblt_from(digests.iter().copied());
                outbox.send_rateless_bloom_riblt(nbr, bf, riblt);
            }
        } else {
            // Delivery phase: send pending elements identified last round.
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

            if let Some((bf, riblt)) = msg.as_rateless_bloom_riblt() {
                let bloom_bits = bf.bits_per_filter();
                let sender_size = bf.source_size();
                let effective_m = bloom_bits as f64 / sender_size.max(1) as f64;

                let local_digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();
                let strategy = ExpectedCostFactory::new(effective_m)
                    .create(local_digests, sender_size);

                let (common, mut local_only) = bf.extend_until(strategy);

                // RIBLT cross-check: recovers local elements that were false
                // positives in the sender's BF (appeared in `common` but are
                // not in sender's RIBLT).
                if !common.is_empty() {
                    let mut common_riblt = RatelessIBLT::riblt_from(common.iter().copied());
                    riblt.decode_against(&mut common_riblt);
                    local_only.extend(riblt.remote_only());
                }

                let local_only_set: std::collections::HashSet<u64> =
                    local_only.into_iter().collect();
                let to_send: Vec<Element> = local
                    .set
                    .iter()
                    .filter(|e| local_only_set.contains(&e.digest))
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
