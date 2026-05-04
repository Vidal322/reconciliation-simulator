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
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

/// Two-round Hybrid Rateless-Bloom + RIBLT reconciliation.
#[derive(Clone, Debug)]
pub struct HybridRbfRibltProtocol {
    m_ratio: f64,
    state: HashMap<usize, HybridState>,
}

#[derive(Clone, Debug, Default)]
struct HybridState {
    pending: HashMap<usize, Vec<Element>>,
}

impl Default for HybridRbfRibltProtocol {
    fn default() -> Self {
        Self::new()
    }
}

impl HybridRbfRibltProtocol {
    pub fn new() -> Self {
        Self {
            m_ratio: 0.5,
            state: HashMap::new(),
        }
    }

    pub fn with_m_ratio(m_ratio: f64) -> Self {
        assert!(m_ratio > 0.0, "m_ratio must be positive");
        Self {
            m_ratio,
            state: HashMap::new(),
        }
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
        &mut self,
        replica_id: usize,
        local: &Replica,
        topology: &Topology,
        outbox: &mut Outbox<'_>,
        _carry: Option<PendingElements>,
    ) {
        let state = self.state.entry(replica_id).or_default();

        if state.pending.is_empty() {
            let digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();
            let bloom_bits = self.bloom_bits_for(digests.len());

            for &neighbor_id in topology.neighbors(replica_id) {
                let bf = RatelessBF::new(digests.clone(), bloom_bits);
                let riblt = RatelessIBLT::riblt_from(digests.iter().copied());
                outbox.send_rateless_bloom_riblt(replica_id, neighbor_id, bf, riblt);
            }
        } else {
            let pending = std::mem::take(&mut state.pending);
            for (neighbor_id, elements) in pending {
                if !elements.is_empty() {
                    outbox.send_elements(replica_id, neighbor_id, elements);
                }
            }
        }
    }

    fn recv_phase(
        &mut self,
        replica_id: usize,
        local: &Replica,
        _topology: &Topology,
        inbox: &mut [(usize, ProtocolMsg)],
    ) -> ProtocolStepResult {
        let mut next_set = local.snapshot_set();

        let mut encode_time = Duration::ZERO;
        let mut decode_time = Duration::ZERO;
        let mut false_matches = 0usize;

        let state = self.state.entry(replica_id).or_default();

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

                    let riblt_local_only = sender_riblt.remote_only();
                    false_matches += riblt_local_only.len();
                    recovered_local_only.extend(riblt_local_only);
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
                    state.pending.entry(*from).or_default().extend(to_send);
                }
            }
        }

        ProtocolStepResult {
            next_set,
            metrics: LocalMetrics {
                encode_time,
                decode_time,
                false_matches,
            },
            carry: None,
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
        let mut protocol = HybridRbfRibltProtocol::new();
        let topology = Topology::star(2);
        let mut replicas = vec![make_replica(0, &[1, 2, 3]), make_replica(1, &[2, 3, 4])];
        let mut network: Network<ProtocolMsg> = Network::from_topology(&topology);

        for _ in 0..2 {
            {
                let mut outbox = Outbox::new(&mut network);
                for id in 0..replicas.len() {
                    protocol.send_phase(id, &replicas[id], &topology, &mut outbox, None);
                }
            }
            let mut inboxes: Vec<Vec<(usize, ProtocolMsg)>> = (0..replicas.len())
                .map(|id| network.drain_inbox(id))
                .collect();
            let next_sets: Vec<_> = inboxes
                .iter_mut()
                .enumerate()
                .map(|(id, inbox)| {
                    protocol
                        .recv_phase(id, &replicas[id], &topology, inbox)
                        .next_set
                })
                .collect();
            for inbox in &inboxes {
                network.bill_metadata_post_recv(inbox);
            }
            for (replica, next) in replicas.iter_mut().zip(next_sets) {
                replica.set = next;
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
        let mut protocol = HybridRbfRibltProtocol::new();
        let topology = Topology::star(2);
        let replicas = vec![make_replica(0, &[5, 6, 7]), make_replica(1, &[5, 6, 7])];
        let mut network: Network<ProtocolMsg> = Network::from_topology(&topology);

        {
            let mut outbox = Outbox::new(&mut network);
            for id in 0..replicas.len() {
                protocol.send_phase(id, &replicas[id], &topology, &mut outbox, None);
            }
        }
        let mut inboxes: Vec<Vec<(usize, ProtocolMsg)>> = (0..replicas.len())
            .map(|id| network.drain_inbox(id))
            .collect();
        for (id, inbox) in inboxes.iter_mut().enumerate() {
            let _ = protocol.recv_phase(id, &replicas[id], &topology, inbox);
        }
        for inbox in &inboxes {
            network.bill_metadata_post_recv(inbox);
        }

        network.reset();
        {
            let mut outbox = Outbox::new(&mut network);
            for id in 0..replicas.len() {
                protocol.send_phase(id, &replicas[id], &topology, &mut outbox, None);
            }
        }
        for id in 0..replicas.len() {
            for (_from, mut msg) in network.drain_inbox(id) {
                assert!(msg.as_rateless_bloom_riblt().is_some());
            }
        }
    }
}
