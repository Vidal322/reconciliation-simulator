use std::collections::HashMap;
use std::mem;
use std::time::Duration;

use crate::simulator::algorithms::bloom::BloomFilter;
use crate::simulator::algorithms::riblt::RatelessIBLT;
use crate::simulator::network::{RecvView, SendView};
use crate::simulator::protocols::messages::{ProtocolMsg, SimulatorHint};
use crate::simulator::protocols::{
    LocalMetrics, Protocol, ProtocolStepResult, ProtocolKind,
};
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

/// Two-round Bloom Filter + IBLT reconciliation.
///
/// Round N:
///   - send_phase ships a `BloomFilter` message (carrying local digests
///     as auxiliary) to every neighbour. The Bloom bit-array cost is
///     billed via `WireSized`; the digest list is unbilled.
///   - recv_phase rebuilds the sender's Bloom from its auxiliary
///     digests, tests the receiver's own digests against it, runs RIBLT
///     on the candidate false-positives, and stashes elements to send.
///     Bloom and RIBLT metadata are billed via `record_decoded_metadata`.
///
/// Round N+1:
///   - send_phase drains the stash and emits `Elements` messages.
///   - recv_phase merges received elements into the local set.
#[derive(Clone, Debug)]
pub struct StaticBfIbltProtocol {
    false_positive_rate: f64,
    state: HashMap<usize, BfIbltState>,
}

#[derive(Clone, Debug, Default)]
struct BfIbltState {
    pending: HashMap<usize, Vec<Element>>,
}

impl Default for StaticBfIbltProtocol {
    fn default() -> Self {
        Self::new()
    }
}

impl StaticBfIbltProtocol {
    pub fn new() -> Self {
        Self {
            false_positive_rate: 0.01,
            state: HashMap::new(),
        }
    }

    pub fn with_false_positive_rate(false_positive_rate: f64) -> Self {
        assert!(
            (0.0..1.0).contains(&false_positive_rate),
            "false_positive_rate must be in (0, 1)"
        );
        Self {
            false_positive_rate,
            state: HashMap::new(),
        }
    }
}

impl Protocol for StaticBfIbltProtocol {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::StaticBfIblt
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

            let bloom: BloomFilter<u64> = BloomFilter::new(
                digests.len().max(1),
                self.false_positive_rate,
            );
            let bit_len = bloom.bit_len();

            for &neighbor_id in topology.neighbors(replica_id) {
                network.send(
                    replica_id,
                    neighbor_id,
                    ProtocolMsg::BloomFilter { bit_len },
                    SimulatorHint::BloomDigests {
                        digests: digests.clone(),
                        false_positive_rate: self.false_positive_rate,
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
                ProtocolMsg::BloomFilter { .. } => {
                    let (sender_digests, false_positive_rate) = match hint {
                        SimulatorHint::BloomDigests { digests, false_positive_rate } => {
                            (digests, false_positive_rate)
                        }
                        _ => panic!("expected BloomDigests hint"),
                    };

                    // Rebuild the sender's Bloom from its digests.
                    let mut bloom = BloomFilter::new(
                        sender_digests.len().max(1),
                        false_positive_rate,
                    );
                    for digest in &sender_digests {
                        bloom.timed_insert(digest);
                    }

                    // Bill the Bloom metadata (bit array + header).
                    let bloom_meta = bloom.byte_len()
                        + mem::size_of::<usize>()
                        + mem::size_of::<u64>();
                    network.record_decoded_metadata(
                        replica_id,
                        bloom_meta as u64,
                    );
                    encode_time += bloom.t_enc();

                    // Test OUR digests against the sender's Bloom.
                    let local_digests: Vec<u64> =
                        local.set.iter().map(|e| e.digest).collect();

                    let mut confirmed_local_only = Vec::new();
                    let mut candidate_positives = Vec::new();

                    for &digest in &local_digests {
                        if bloom.timed_contains(&digest) {
                            candidate_positives.push(digest);
                        } else {
                            confirmed_local_only.push(digest);
                        }
                    }
                    decode_time += bloom.t_dec();

                    // Resolve false positives via RIBLT.
                    let mut recovered_local_only = confirmed_local_only;

                    if !candidate_positives.is_empty() {
                        let mut sender_riblt =
                            RatelessIBLT::riblt_from(sender_digests);
                        let mut candidate_riblt =
                            RatelessIBLT::riblt_from(candidate_positives);

                        let sketch_len = sender_riblt
                            .find_all_differences(&mut candidate_riblt);

                        let riblt_meta =
                            (sketch_len * mem::size_of::<u64>()) as u64;
                        network.record_decoded_metadata(
                            replica_id,
                            riblt_meta,
                        );
                        encode_time += sender_riblt.t_enc();
                        decode_time += sender_riblt.t_dec();

                        let riblt_local_only =
                            sender_riblt.get_remote_only_symbols();
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
                        state
                            .pending
                            .entry(from)
                            .or_default()
                            .extend(to_send);
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
    fn bf_iblt_converges_in_two_rounds() {
        let mut protocol = StaticBfIbltProtocol::new();
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
    fn bf_iblt_identical_sets_no_elements_sent() {
        let mut protocol = StaticBfIbltProtocol::new();
        let topology = Topology::star(2);
        let replicas = vec![make_replica(0, &[5, 6, 7]), make_replica(1, &[5, 6, 7])];
        let mut network: Network<ProtocolMsg> = Network::from_topology(&topology);
        let mut hints = HintStore::new();

        // Bloom round
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

        // Next send_phase should emit Blooms again (stash empty), not Elements.
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
                assert!(matches!(msg, ProtocolMsg::BloomFilter { .. }));
            }
        }
    }
}
