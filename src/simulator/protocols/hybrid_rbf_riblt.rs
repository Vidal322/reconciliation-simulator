use std::collections::HashMap;
use std::mem;
use std::time::Duration;

use crate::simulator::algorithms::rateless_bloom::bayesian_cost::RATELESS_SET_RECONCILIATION_OVERHEAD;
use crate::simulator::algorithms::rateless_bloom::expected_cost::ExpectedCostFactory;
use crate::simulator::algorithms::rateless_bloom::{RatelessBF, StoppingStrategyFactory};
use crate::simulator::algorithms::riblt::RatelessIBLT;
use crate::simulator::network::{RecvView, SendView};
use crate::simulator::protocols::messages::{ProtocolMsg, SimulatorHint};
use crate::simulator::protocols::{LocalMetrics, Protocol, ProtocolKind, ProtocolStepResult};
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

/// Two-round Hybrid Rateless-Bloom + RIBLT reconciliation.
///
/// Round N:
///   - send_phase ships a `RatelessBloom` message (carrying local
///     digests and bloom_bits as auxiliary) to every neighbour.
///   - recv_phase rebuilds the sender's RatelessBF, runs extend_until
///     with the ExpectedCost stopping strategy to partition the
///     receiver's own digests into definitely-missing and ambiguous,
///     runs RIBLT on the ambiguous subset, and stashes elements to send.
///
/// Round N+1:
///   - send_phase drains the stash and emits `Elements` messages.
///   - recv_phase merges received elements into the local set.
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
        network: &mut SendView<ProtocolMsg>,
    ) {
        let state = self.state.entry(replica_id).or_default();

        if state.pending.is_empty() {
            let digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();
            let bloom_bits = self.bloom_bits_for(digests.len());

            for &neighbor_id in topology.neighbors(replica_id) {
                network.send(
                    replica_id,
                    neighbor_id,
                    ProtocolMsg::RatelessBloom { byte_len: 0 },
                    SimulatorHint::RatelessBloomDigests {
                        digests: digests.clone(),
                        bloom_bits,
                    },
                );
            }
        } else {
            let pending = std::mem::take(&mut state.pending);
            for (neighbor_id, elements) in pending {
                if !elements.is_empty() {
                    network.send(
                        replica_id,
                        neighbor_id,
                        ProtocolMsg::Elements(elements),
                        SimulatorHint::None,
                    );
                }
            }
        }
    }

    fn recv_phase(
        &mut self,
        replica_id: usize,
        local: &Replica,
        _topology: &Topology,
        inbox: Vec<(usize, ProtocolMsg, SimulatorHint)>,
        network: &mut RecvView<ProtocolMsg>,
    ) -> ProtocolStepResult {
        let mut next_set = local.snapshot_set();

        let mut encode_time = Duration::ZERO;
        let mut decode_time = Duration::ZERO;
        let mut false_matches = 0usize;

        let state = self.state.entry(replica_id).or_default();

        for (from, msg, hint) in inbox {
            match msg {
                ProtocolMsg::RatelessBloom { .. } => {
                    let (sender_digests, bloom_bits) = match hint {
                        SimulatorHint::RatelessBloomDigests {
                            digests,
                            bloom_bits,
                        } => (digests, bloom_bits),
                        _ => panic!("expected RatelessBloomDigests hint"),
                    };

                    // Rebuild the sender's RatelessBF from its digests.
                    let mut sender_filter = RatelessBF::new(sender_digests.clone(), bloom_bits);

                    let effective_m_ratio =
                        Self::effective_m_ratio(bloom_bits, sender_digests.len());

                    // Create stopping strategy with OUR digests as the
                    // elements to be tested against the sender's filter.
                    let local_digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();

                    let stopping_strategy = ExpectedCostFactory::new(effective_m_ratio)
                        .create(local_digests, sender_digests.len());

                    let (common, definitely_missing) =
                        sender_filter.extend_until(stopping_strategy);

                    // Bill the rateless Bloom metadata.
                    let bloom_meta = sender_filter.size_of() as u64;
                    network.record_decoded_metadata(replica_id, bloom_meta);
                    encode_time += sender_filter.t_enc();
                    decode_time += sender_filter.t_dec();

                    // definitely_missing: our digests that the sender
                    // definitely doesn't have → we should send these.
                    let mut recovered_local_only: Vec<u64> = definitely_missing;

                    // Resolve the ambiguous subset via RIBLT.
                    if !common.is_empty() {
                        let mut sender_riblt = RatelessIBLT::riblt_from(sender_digests);
                        let mut common_riblt = RatelessIBLT::riblt_from(common);

                        let sketch_len = sender_riblt.find_all_differences(&mut common_riblt);

                        let riblt_meta = (sketch_len * mem::size_of::<u64>()) as u64;
                        network.record_decoded_metadata(replica_id, riblt_meta);
                        encode_time += sender_riblt.t_enc();
                        decode_time += sender_riblt.t_dec();

                        let riblt_local_only = sender_riblt.get_remote_only_symbols();
                        false_matches += riblt_local_only.len();
                        recovered_local_only.extend(riblt_local_only);
                    }

                    // Stash the elements we need to send to the sender.
                    let local_only_set: std::collections::HashSet<u64> =
                        recovered_local_only.into_iter().collect();
                    let to_send: Vec<Element> = local
                        .set
                        .iter()
                        .filter(|e| local_only_set.contains(&e.digest))
                        .cloned()
                        .collect();
                    if !to_send.is_empty() {
                        state.pending.entry(from).or_default().extend(to_send);
                    }
                }
                ProtocolMsg::Elements(els) => {
                    for element in els {
                        next_set.insert(element);
                    }
                }
                _ => {}
            }
        }

        ProtocolStepResult {
            next_set,
            metrics: LocalMetrics {
                encode_time,
                decode_time,
                false_matches,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulator::network::{HintStore, Network};
    use crate::simulator::protocols::test_helpers::make_replica;
    use std::collections::HashSet;

    #[test]
    fn hybrid_converges_in_two_rounds() {
        let mut protocol = HybridRbfRibltProtocol::new();
        let topology = Topology::star(2);
        let mut replicas = vec![make_replica(0, &[1, 2, 3]), make_replica(1, &[2, 3, 4])];
        let mut network: Network<ProtocolMsg> = Network::from_topology(&topology);
        let mut hints = HintStore::new();

        for _ in 0..2 {
            {
                let mut send_view = SendView::new(&mut network, &mut hints);
                for id in 0..replicas.len() {
                    protocol.send_phase(id, &replicas[id], &topology, &mut send_view);
                }
            }
            let next_sets: Vec<_> = {
                let inboxes: Vec<_> = (0..replicas.len())
                    .map(|id| {
                        network
                            .drain_inbox(id)
                            .into_iter()
                            .map(|(from, msg)| {
                                let hint = hints.drain_for(from, id);
                                (from, msg, hint)
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect();
                let mut recv_view = RecvView::new(&mut network);
                inboxes
                    .into_iter()
                    .enumerate()
                    .map(|(id, inbox)| {
                        protocol
                            .recv_phase(id, &replicas[id], &topology, inbox, &mut recv_view)
                            .next_set
                    })
                    .collect()
            };
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
        let mut hints = HintStore::new();

        {
            let mut send_view = SendView::new(&mut network, &mut hints);
            for id in 0..replicas.len() {
                protocol.send_phase(id, &replicas[id], &topology, &mut send_view);
            }
        }
        {
            let inboxes: Vec<_> = (0..replicas.len())
                .map(|id| {
                    network
                        .drain_inbox(id)
                        .into_iter()
                        .map(|(from, msg)| {
                            let hint = hints.drain_for(from, id);
                            (from, msg, hint)
                        })
                        .collect::<Vec<_>>()
                })
                .collect();
            let mut recv_view = RecvView::new(&mut network);
            for (id, inbox) in inboxes.into_iter().enumerate() {
                let _ = protocol.recv_phase(id, &replicas[id], &topology, inbox, &mut recv_view);
            }
        }

        network.reset();
        hints.reset();
        {
            let mut send_view = SendView::new(&mut network, &mut hints);
            for id in 0..replicas.len() {
                protocol.send_phase(id, &replicas[id], &topology, &mut send_view);
            }
        }
        for id in 0..replicas.len() {
            for (_from, msg) in network.drain_inbox(id) {
                assert!(matches!(msg, ProtocolMsg::RatelessBloom { .. }));
            }
        }
    }
}
