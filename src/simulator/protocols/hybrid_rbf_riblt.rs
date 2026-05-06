use std::collections::HashMap;
use std::time::Duration;

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

/// Two-round Hybrid Rateless-Bloom + RIBLT reconciliation.
///
/// Round N: send_phase ships `(RatelessBF, RIBLT)` pairs; recv_phase
/// runs the rateless-bloom stopping strategy + RIBLT cross-check and
/// returns the per-neighbour local-only elements as `carry`.
/// Round N+1: send_phase consumes the carry and emits Elements.
#[derive(Clone, Debug)]
pub struct HybridRbfRibltProtocol {
    m_ratio: f64,
}

impl Default for HybridRbfRibltProtocol {
    fn default() -> Self {
        Self::new()
    }
}

impl HybridRbfRibltProtocol {
    pub fn new() -> Self {
        Self { m_ratio: 0.5 }
    }

    pub fn with_m_ratio(m_ratio: f64) -> Self {
        assert!(m_ratio > 0.0, "m_ratio must be positive");
        Self { m_ratio }
    }

    fn bloom_bits_for(&self, n: usize) -> usize {
        let requested = ((n as f64) * self.m_ratio).ceil().max(1.0) as usize;
        let minimum_for_nonzero_threshold = RATELESS_SET_RECONCILIATION_OVERHEAD * 8;
        requested.max(minimum_for_nonzero_threshold)
    }

    fn effective_m_ratio(bloom_bits: usize, n: usize) -> f64 {
        bloom_bits as f64 / n.max(1) as f64
    }
}

impl Protocol for HybridRbfRibltProtocol {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::HybridRbfRiblt
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
            let digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();
            let bloom_bits = self.bloom_bits_for(digests.len());

            for &neighbor_id in topology.neighbors(local.id) {
                let bf = RatelessBF::new(digests.clone(), bloom_bits);
                let riblt = RatelessIBLT::riblt_from(digests.iter().copied());
                outbox.send_rateless_bloom_riblt(neighbor_id, bf, riblt);
            }
        } else {
            for (neighbor_id, elements) in pending {
                if !elements.is_empty() {
                    outbox.send_elements(neighbor_id, elements);
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

        let mut encode_time = Duration::ZERO;
        let mut decode_time = Duration::ZERO;

        let mut pending: PendingElements = HashMap::new();

        for (from, msg) in inbox.iter_mut() {
            if let Some(els) = msg.take_elements() {
                for element in els {
                    next_set.insert(element);
                }
                continue;
            }

            if let Some((sender_filter, sender_riblt)) = msg.as_rateless_bloom_riblt() {
                let bloom_bits = sender_filter.bits_per_filter();
                let sender_size = sender_filter.source_size();
                let effective_m_ratio = Self::effective_m_ratio(bloom_bits, sender_size);

                let local_digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();

                let stopping_strategy = ExpectedCostFactory::new(effective_m_ratio)
                    .create(local_digests, sender_size);

                let (common, definitely_missing) =
                    sender_filter.extend_until(stopping_strategy);

                encode_time += sender_filter.t_enc();
                decode_time += sender_filter.t_dec();

                let mut recovered_local_only: Vec<u64> = definitely_missing;

                if !common.is_empty() {
                    let mut common_riblt = RatelessIBLT::riblt_from(common);

                    sender_riblt.decode_against(&mut common_riblt);
                    encode_time += sender_riblt.t_enc();
                    decode_time += sender_riblt.t_dec();

                    recovered_local_only.extend(sender_riblt.remote_only());
                }

                let local_only_set: std::collections::HashSet<u64> =
                    recovered_local_only.into_iter().collect();
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
            metrics: LocalMetrics {
                encode_time,
                decode_time,
            },
            carry: if pending.is_empty() { None } else { Some(pending) },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulator::network::Network;
    use crate::simulator::protocols::test_helpers::make_replica;
    use std::collections::HashSet;

    #[test]
    fn hybrid_converges_in_two_rounds() {
        let protocol = HybridRbfRibltProtocol::new();
        let topology = Topology::star(2);
        let mut replicas = vec![make_replica(0, &[1, 2, 3]), make_replica(1, &[2, 3, 4])];
        let mut network: Network<ProtocolMsg> = Network::from_topology(&topology);
        let mut carries: Vec<Option<PendingElements>> =
            (0..replicas.len()).map(|_| None).collect();

        for _ in 0..2 {
            for id in 0..replicas.len() {
                let carry = carries[id].take();
                let mut outbox = Outbox::for_replica(&mut network, id);
                protocol.send_phase(replicas[id].view(), &topology, &mut outbox, carry);
            }
            let mut inboxes: Vec<Vec<(usize, ProtocolMsg)>> = (0..replicas.len())
                .map(|id| network.drain_inbox(id))
                .collect();
            let results: Vec<_> = inboxes
                .iter_mut()
                .enumerate()
                .map(|(id, inbox)| {
                    protocol.recv_phase(replicas[id].view(), &topology, inbox)
                })
                .collect();
            for inbox in &inboxes {
                network.bill_metadata_post_recv(inbox);
            }
            for (id, (replica, result)) in
                replicas.iter_mut().zip(results).enumerate()
            {
                replica.set = result.next_set;
                carries[id] = result.carry;
            }
        }

        let union0: HashSet<u64> = replicas[0].set.iter().map(|e| e.digest).collect();
        let union1: HashSet<u64> = replicas[1].set.iter().map(|e| e.digest).collect();
        assert!(union0.contains(&1));
        assert!(union0.contains(&2));
        assert!(union0.contains(&3));
        assert!(union0.contains(&4));
        assert_eq!(union0, union1);
        assert!(network.stats().bytes_metadata > 0);
    }

    #[test]
    fn hybrid_identical_sets_no_elements_sent() {
        let protocol = HybridRbfRibltProtocol::new();
        let topology = Topology::star(2);
        let replicas = vec![make_replica(0, &[5, 6, 7]), make_replica(1, &[5, 6, 7])];
        let mut network: Network<ProtocolMsg> = Network::from_topology(&topology);
        let mut carries: Vec<Option<PendingElements>> =
            (0..replicas.len()).map(|_| None).collect();

        for id in 0..replicas.len() {
            let carry = carries[id].take();
            let mut outbox = Outbox::for_replica(&mut network, id);
            protocol.send_phase(replicas[id].view(), &topology, &mut outbox, carry);
        }
        let mut inboxes: Vec<Vec<(usize, ProtocolMsg)>> = (0..replicas.len())
            .map(|id| network.drain_inbox(id))
            .collect();
        for (id, inbox) in inboxes.iter_mut().enumerate() {
            let result = protocol.recv_phase(replicas[id].view(), &topology, inbox);
            carries[id] = result.carry;
        }
        for inbox in &inboxes {
            network.bill_metadata_post_recv(inbox);
        }

        // With identical sets, the rateless-bloom + RIBLT pipeline finds
        // zero local-only elements, so no carry is produced.
        assert!(carries.iter().all(|c| c.is_none()));

        network.reset();
        for id in 0..replicas.len() {
            let carry = carries[id].take();
            let mut outbox = Outbox::for_replica(&mut network, id);
            protocol.send_phase(replicas[id].view(), &topology, &mut outbox, carry);
        }
        for id in 0..replicas.len() {
            for (_from, mut msg) in network.drain_inbox(id) {
                assert!(msg.as_rateless_bloom_riblt().is_some());
            }
        }
    }
}
