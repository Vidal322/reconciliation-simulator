use std::collections::{HashMap, HashSet};
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
use crate::simulator::topology::{Topology, TopologyKind};

/// Pipelined topology-aware reconciliation:
///
/// - Sketches are sent whenever the local set has grown since the last sketch,
///   rather than alternating sketch/element rounds.  This means an element
///   send and a fresh sketch can occur in the same round, halving the
///   effective propagation delay.
/// - Newly received elements are eagerly forwarded to all neighbours in the
///   very next send_phase, using `sent_to` + `received_from` dedup so we
///   never waste bytes re-sending what a neighbour already knows about.
/// - Star / Tree: RatelessBF + RIBLT hybrid.
/// - Chord: pure RIBLT.
pub struct MultiReplicaV2Protocol {
    m_ratio: f64,
    state: HashMap<usize, ReplicaState>,
}

#[derive(Clone, Debug, Default)]
struct ReplicaState {
    /// Elements queued for a specific neighbour (from sketch-diff decode).
    pending: HashMap<usize, Vec<Element>>,
    /// Elements newly added to local set since last send_phase.
    /// Forwarded eagerly to all neighbours next round.
    recently_gained: Vec<Element>,
    /// Digests already sent to each neighbour.  Prevents resending.
    sent_to: HashMap<usize, HashSet<u64>>,
    /// Digests received FROM each neighbour.  Prevents echoing back.
    received_from: HashMap<usize, HashSet<u64>>,
    /// Set size the last time we sent a sketch.  0 ⟹ not yet sent.
    sketch_set_size: usize,
}

impl MultiReplicaV2Protocol {
    pub fn new() -> Self {
        Self {
            m_ratio: 0.5,
            state: HashMap::new(),
        }
    }

    fn bloom_bits_for(&self, n: usize) -> usize {
        let requested = ((n as f64) * self.m_ratio).ceil().max(1.0) as usize;
        let minimum = RATELESS_SET_RECONCILIATION_OVERHEAD * 8;
        requested.max(minimum)
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
        let state = self.state.entry(replica_id).or_default();
        let neighbors: Vec<usize> = topology.neighbors(replica_id).to_vec();

        // ── 1. Drain RIBLT/BF-computed pending (targeted to specific neighbours). ──
        let pending = mem::take(&mut state.pending);
        for (neighbor_id, elements) in pending {
            let sent = state.sent_to.entry(neighbor_id).or_default();
            let to_send: Vec<Element> =
                elements.into_iter().filter(|e| sent.insert(e.digest)).collect();
            if !to_send.is_empty() {
                network.send(
                    replica_id,
                    neighbor_id,
                    ProtocolMsg::Elements(to_send),
                    SimulatorHint::None,
                );
            }
        }

        // ── 2. Eager-forward recently gained elements to ALL neighbours. ──
        //    Only for Star/Tree: Chord's dense connectivity causes too many
        //    redundant sends when forwarding (multiple neighbours all see the
        //    same element and all forward it simultaneously).
        //    Skip: elements we already sent to that neighbour (sent_to),
        //          elements we received FROM that neighbour (received_from).
        let recent = mem::take(&mut state.recently_gained);
        if !recent.is_empty() && topology.kind != TopologyKind::Chord {
            for &neighbor_id in &neighbors {
                let sent = state.sent_to.entry(neighbor_id).or_default();
                let recvd = state.received_from.entry(neighbor_id).or_default();
                let to_forward: Vec<Element> = recent
                    .iter()
                    .filter(|e| !recvd.contains(&e.digest) && sent.insert(e.digest))
                    .cloned()
                    .collect();
                if !to_forward.is_empty() {
                    network.send(
                        replica_id,
                        neighbor_id,
                        ProtocolMsg::Elements(to_forward),
                        SimulatorHint::None,
                    );
                }
            }
        }

        // ── 3. Send sketch if our set has grown since the last sketch. ──
        let current_size = local.set.len();
        if current_size != state.sketch_set_size {
            state.sketch_set_size = current_size;
            let digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();

            match topology.kind {
                TopologyKind::Chord => {
                    for &neighbor_id in &neighbors {
                        network.send(
                            replica_id,
                            neighbor_id,
                            ProtocolMsg::RibltSketch { symbols: 0 },
                            SimulatorHint::RibltDigests { digests: digests.clone() },
                        );
                    }
                }
                _ => {
                    let bloom_bits = self.bloom_bits_for(digests.len());
                    for &neighbor_id in &neighbors {
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

        // ── Pass 1: apply all Elements messages first so the updated set is
        //    available when we decode sketches below. ──────────────────────
        let mut sketch_inbox: Vec<(usize, ProtocolMsg, SimulatorHint)> = Vec::new();
        for (from, msg, hint) in inbox {
            match msg {
                ProtocolMsg::Elements(els) => {
                    let recvd = state.received_from.entry(from).or_default();
                    let mut newly_gained: Vec<Element> = Vec::new();
                    for element in els {
                        recvd.insert(element.digest);
                        // Only track as "recently gained" if truly new to us.
                        if next_set.insert(element.clone()) {
                            newly_gained.push(element);
                        }
                    }
                    state.recently_gained.extend(newly_gained);
                }
                other => sketch_inbox.push((from, other, hint)),
            }
        }

        // Build local digest list from the updated set so sketch diffs are
        // computed against the freshest state.
        let local_digests: Vec<u64> = next_set.iter().map(|e| e.digest).collect();

        // ── Pass 2: decode sketches. ──────────────────────────────────────
        for (from, msg, hint) in sketch_inbox {
            match msg {
                ProtocolMsg::RibltSketch { .. } => {
                    let remote_digests = match hint {
                        SimulatorHint::RibltDigests { digests } => digests,
                        _ => panic!("expected RibltDigests hint"),
                    };

                    let mut local_riblt = RatelessIBLT::riblt_from(local_digests.clone());
                    let mut remote_riblt = RatelessIBLT::riblt_from(remote_digests);

                    let sketch_len = local_riblt.find_all_differences(&mut remote_riblt);
                    let metadata_bytes = (sketch_len * mem::size_of::<u64>()) as u64;
                    network.record_decoded_metadata(replica_id, metadata_bytes);

                    encode_time += local_riblt.t_enc();
                    decode_time += local_riblt.t_dec();

                    let local_only: HashSet<u64> =
                        local_riblt.get_local_only_symbols().into_iter().collect();
                    let to_send: Vec<Element> = next_set
                        .iter()
                        .filter(|e| local_only.contains(&e.digest))
                        .cloned()
                        .collect();
                    if !to_send.is_empty() {
                        state.pending.entry(from).or_default().extend(to_send);
                    }
                }

                ProtocolMsg::RatelessBloom { .. } => {
                    let (sender_digests, bloom_bits) = match hint {
                        SimulatorHint::RatelessBloomDigests { digests, bloom_bits } => {
                            (digests, bloom_bits)
                        }
                        _ => panic!("expected RatelessBloomDigests hint"),
                    };

                    let mut sender_filter = RatelessBF::new(sender_digests.clone(), bloom_bits);
                    let effective_m_ratio =
                        bloom_bits as f64 / sender_digests.len().max(1) as f64;

                    let stopping_strategy = ExpectedCostFactory::new(effective_m_ratio)
                        .create(local_digests.clone(), sender_digests.len());

                    let (common, definitely_missing) =
                        sender_filter.extend_until(stopping_strategy);

                    let bloom_meta = sender_filter.size_of() as u64;
                    network.record_decoded_metadata(replica_id, bloom_meta);
                    encode_time += sender_filter.t_enc();
                    decode_time += sender_filter.t_dec();

                    let mut recovered_local_only: Vec<u64> = definitely_missing;

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

                    let local_only_set: HashSet<u64> =
                        recovered_local_only.into_iter().collect();
                    let to_send: Vec<Element> = next_set
                        .iter()
                        .filter(|e| local_only_set.contains(&e.digest))
                        .cloned()
                        .collect();
                    if !to_send.is_empty() {
                        state.pending.entry(from).or_default().extend(to_send);
                    }
                }

                _ => {}
            }
        }

        ProtocolStepResult {
            next_set,
            metrics: LocalMetrics { encode_time, decode_time, false_matches },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulator::network::{HintStore, Network};
    use crate::simulator::protocols::test_helpers::make_replica;
    use std::collections::HashSet;

    fn run_protocol(
        protocol: &mut MultiReplicaV2Protocol,
        replicas: &mut Vec<crate::simulator::replica::Replica>,
        topology: &Topology,
        max_rounds: usize,
    ) {
        let mut network: Network<ProtocolMsg> = Network::from_topology(topology);
        let mut hints = HintStore::new();

        for _ in 0..max_rounds {
            {
                let mut send_view = SendView::new(&mut network, &mut hints);
                for id in 0..replicas.len() {
                    protocol.send_phase(id, &replicas[id], topology, &mut send_view);
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
                            .recv_phase(id, &replicas[id], topology, inbox, &mut recv_view)
                            .next_set
                    })
                    .collect()
            };
            for (replica, next) in replicas.iter_mut().zip(next_sets) {
                replica.set = next;
            }
        }
    }

    #[test]
    fn converges_on_star() {
        let mut protocol = MultiReplicaV2Protocol::new();
        let topology = Topology::star(2);
        let mut replicas = vec![make_replica(0, &[1, 2, 3]), make_replica(1, &[2, 3, 4])];
        run_protocol(&mut protocol, &mut replicas, &topology, 4);
        let union: HashSet<u64> = replicas[0].set.iter().map(|e| e.digest).collect();
        assert!(union.contains(&1) && union.contains(&4));
        let union1: HashSet<u64> = replicas[1].set.iter().map(|e| e.digest).collect();
        assert_eq!(union, union1);
    }

    #[test]
    fn converges_on_chord() {
        let mut protocol = MultiReplicaV2Protocol::new();
        let topology = Topology::chord(4);
        let mut replicas = vec![
            make_replica(0, &[1, 5]),
            make_replica(1, &[2, 6]),
            make_replica(2, &[3, 7]),
            make_replica(3, &[4, 8]),
        ];
        run_protocol(&mut protocol, &mut replicas, &topology, 8);
        let expected: HashSet<u64> = (1..=8).collect();
        for r in &replicas {
            let got: HashSet<u64> = r.set.iter().map(|e| e.digest).collect();
            assert_eq!(got, expected);
        }
    }
}
