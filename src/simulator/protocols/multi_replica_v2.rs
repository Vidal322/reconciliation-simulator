use std::collections::{HashMap, HashSet};
use std::mem;

use crate::simulator::algorithms::rateless_bloom::bayesian_cost::RATELESS_SET_RECONCILIATION_OVERHEAD;
use crate::simulator::algorithms::rateless_bloom::expected_cost::ExpectedCostFactory;
use crate::simulator::algorithms::rateless_bloom::{RatelessBF, StoppingStrategyFactory};
use crate::simulator::algorithms::riblt::RatelessIBLT;
use crate::simulator::network::{RecvView, SendView};
use crate::simulator::protocols::messages::{ProtocolMsg, SimulatorHint};
use crate::simulator::protocols::{LocalMetrics, Protocol, ProtocolKind, ProtocolStepResult};
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::{Topology, TopologyKind};

/// Decode a single BF+RIBLT sketch message against `local_digests`.
/// Bills decoded metadata bytes via `network`.
/// Returns the list of local-only digests (elements receiver has, sender lacks).
fn decode_bf_sketch(
    replica_id: usize,
    sender_digests: Vec<u64>,
    bloom_bits: usize,
    local_digests: &[u64],
    network: &mut RecvView<ProtocolMsg>,
) -> Vec<u64> {
    let effective_m_ratio = bloom_bits as f64 / sender_digests.len().max(1) as f64;
    let mut sender_filter = RatelessBF::new(sender_digests.clone(), bloom_bits);
    let stopping_strategy = ExpectedCostFactory::new(effective_m_ratio)
        .create(local_digests.to_vec(), sender_digests.len());
    let (common, definitely_missing) = sender_filter.extend_until(stopping_strategy);
    network.record_decoded_metadata(replica_id, sender_filter.size_of() as u64);

    let mut local_only: Vec<u64> = definitely_missing;
    if !common.is_empty() {
        let mut sender_riblt = RatelessIBLT::riblt_from(sender_digests);
        let mut common_riblt = RatelessIBLT::riblt_from(common);
        let sketch_len = sender_riblt.find_all_differences(&mut common_riblt);
        network.record_decoded_metadata(
            replica_id,
            (sketch_len * mem::size_of::<u64>()) as u64,
        );
        local_only.extend(sender_riblt.get_remote_only_symbols());
    }
    local_only
}

/// Topology-aware hybrid protocol.
///
/// Star/Tree: one BF+RIBLT sketch in round 1, then eager-forward every
///   newly received element to all other neighbours (sent_to dedup).
///   Elements propagate across the network in O(depth) rounds without
///   further sketch overhead.
///
/// Chord: pairwise BF+RIBLT, like HybridRbfRiblt.  Repeated sketch→element
///   cycles because Chord's diameter requires multi-hop propagation and
///   naive eager flooding causes O(degree) cascade storms that more than
///   double the bandwidth.
pub struct MultiReplicaV2Protocol {
    m_ratio: f64,
    /// Higher m_ratio for Chord: 50% Jaccard → large diffs → need lower BF FPR
    /// to reduce RIBLT false-positive overhead.
    chord_m_ratio: f64,
    state: HashMap<usize, ReplicaState>,
}

#[derive(Default)]
struct ReplicaState {
    pending: HashMap<usize, Vec<Element>>,
    /// Dedup gate for Star/Tree eager-forward path only.
    sent_to: HashMap<usize, HashSet<u64>>,
    /// One-shot sketch flag for Star/Tree.
    sketch_sent: bool,
    /// Chord sequential: which power (0-4 → distances 1,2,4,8,16) to sketch next.
    chord_sketch_power: usize,
}

impl MultiReplicaV2Protocol {
    pub fn new() -> Self {
        Self {
            m_ratio: 0.5,
            chord_m_ratio: 1.0,
            state: HashMap::new(),
        }
    }

    fn bloom_bits_for(&self, n: usize, is_chord: bool) -> usize {
        let ratio = if is_chord { self.chord_m_ratio } else { self.m_ratio };
        let requested = ((n as f64) * ratio).ceil().max(1.0) as usize;
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
        // Compute sketch params before taking the state borrow.
        let local_digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();
        let bloom_bits = self.bloom_bits_for(local_digests.len(), topology.kind == TopologyKind::Chord);

        let state = self.state.entry(replica_id).or_default();

        if topology.kind == TopologyKind::Chord {
            // Sequential distance-doubling: sketch only the neighbors at
            // distance 2^power in each sketch round (power advances 0→4).
            // Each directed edge sketched exactly once → 3× fewer decode
            // events than sketch-all-neighbors.  After power 4 (distance 16),
            // window covers all 32 nodes; cycle restarts if not yet converged.
            if state.pending.is_empty() {
                let n = topology.node_count();
                let power = state.chord_sketch_power % 5;
                let offset = 1usize << power;
                let forward = (replica_id + offset) % n;
                let backward = (replica_id + n - offset) % n;
                let sketch_targets: Vec<usize> = if forward == backward {
                    vec![forward]
                } else {
                    vec![forward, backward]
                };
                for nb in sketch_targets {
                    if topology.is_neighbor(replica_id, nb) {
                        if power == 0 {
                            // Power 0: large initial diff, BF pre-filtering saves more than BF costs.
                            network.send(
                                replica_id,
                                nb,
                                ProtocolMsg::RatelessBloom { byte_len: 0 },
                                SimulatorHint::RatelessBloomDigests {
                                    digests: local_digests.clone(),
                                    bloom_bits,
                                },
                            );
                        } else {
                            // Powers 1-4: BF layer cost (∝ grown set size) exceeds savings
                            // over pure RIBLT whose cost scales only with diff size.
                            network.send(
                                replica_id,
                                nb,
                                ProtocolMsg::RibltSketch { symbols: 0 },
                                SimulatorHint::RibltDigests {
                                    digests: local_digests.clone(),
                                },
                            );
                        }
                    }
                }
                state.chord_sketch_power += 1;
            } else {
                let pending = std::mem::take(&mut state.pending);
                for (nb, elements) in pending {
                    if !elements.is_empty() {
                        network.send(
                            replica_id,
                            nb,
                            ProtocolMsg::Elements(elements),
                            SimulatorHint::None,
                        );
                    }
                }
            }
        } else {
            // Star/Tree: one sketch up front, then drain elements every round.
            if !state.sketch_sent {
                for &nb in topology.neighbors(replica_id) {
                    network.send(
                        replica_id,
                        nb,
                        ProtocolMsg::RatelessBloom { byte_len: 0 },
                        SimulatorHint::RatelessBloomDigests {
                            digests: local_digests.clone(),
                            bloom_bits,
                        },
                    );
                }
                state.sketch_sent = true;
            }
            let pending = std::mem::take(&mut state.pending);
            for (nb, elements) in pending {
                if !elements.is_empty() {
                    network.send(
                        replica_id,
                        nb,
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
        topology: &Topology,
        inbox: Vec<(usize, ProtocolMsg, SimulatorHint)>,
        network: &mut RecvView<ProtocolMsg>,
    ) -> ProtocolStepResult {
        let mut next_set = local.snapshot_set();

        if topology.kind == TopologyKind::Chord {
            // ── Chord: pairwise, no eager forwarding ─────────────────────────

            // Pass 1: merge received elements.
            for (_, msg, _) in &inbox {
                if let ProtocolMsg::Elements(els) = msg {
                    for el in els {
                        next_set.insert(el.clone());
                    }
                }
            }

            // Pass 2: decode sketches.  Collect (from, to_send) without
            // holding a state borrow so decode_bf_sketch can take &mut network.
            let local_digests: Vec<u64> = next_set.iter().map(|e| e.digest).collect();
            let mut pending_updates: Vec<(usize, Vec<Element>)> = Vec::new();

            for (from, msg, hint) in inbox {
                let local_only: Vec<u64> = match msg {
                    ProtocolMsg::RatelessBloom { .. } => {
                        let (sender_digests, bloom_bits) = match hint {
                            SimulatorHint::RatelessBloomDigests { digests, bloom_bits } => {
                                (digests, bloom_bits)
                            }
                            _ => panic!("expected RatelessBloomDigests hint"),
                        };
                        decode_bf_sketch(replica_id, sender_digests, bloom_bits, &local_digests, network)
                    }
                    ProtocolMsg::RibltSketch { .. } => {
                        let sender_digests = match hint {
                            SimulatorHint::RibltDigests { digests } => digests,
                            _ => panic!("expected RibltDigests hint"),
                        };
                        let mut local_riblt = RatelessIBLT::riblt_from(local_digests.iter().cloned());
                        let mut remote_riblt = RatelessIBLT::riblt_from(sender_digests);
                        let sketch_len = local_riblt.find_all_differences(&mut remote_riblt);
                        network.record_decoded_metadata(
                            replica_id,
                            (sketch_len * mem::size_of::<u64>()) as u64,
                        );
                        local_riblt.get_local_only_symbols()
                    }
                    _ => continue,
                };
                let missing_set: HashSet<u64> = local_only.into_iter().collect();
                let to_send: Vec<Element> = next_set
                    .iter()
                    .filter(|e| missing_set.contains(&e.digest))
                    .cloned()
                    .collect();
                if !to_send.is_empty() {
                    pending_updates.push((from, to_send));
                }
            }

            // Flush collected updates into state.
            let state = self.state.entry(replica_id).or_default();
            for (from, to_send) in pending_updates {
                state.pending.entry(from).or_default().extend(to_send);
            }
        } else {
            // ── Star/Tree: single sketch + eager forwarding ──────────────────

            let mut newly_received: Vec<(Element, usize)> = Vec::new();

            // Pass 1: merge elements and track what's new.
            // (no state borrow needed here)
            for (from, msg, _) in &inbox {
                if let ProtocolMsg::Elements(els) = msg {
                    for element in els {
                        if next_set.insert(element.clone()) {
                            newly_received.push((element.clone(), *from));
                        }
                    }
                }
            }

            // Pass 2: decode sketches.  Collect results first (no state borrow).
            let local_digests: Vec<u64> = next_set.iter().map(|e| e.digest).collect();
            let mut sketch_results: Vec<(usize, HashSet<u64>)> = Vec::new();

            for (from, msg, hint) in inbox {
                if let ProtocolMsg::RatelessBloom { .. } = msg {
                    let (sender_digests, bloom_bits) = match hint {
                        SimulatorHint::RatelessBloomDigests { digests, bloom_bits } => {
                            (digests, bloom_bits)
                        }
                        _ => panic!("expected RatelessBloomDigests hint"),
                    };
                    let local_only =
                        decode_bf_sketch(replica_id, sender_digests, bloom_bits, &local_digests, network);
                    sketch_results.push((from, local_only.into_iter().collect()));
                }
            }

            // Update state: queue elements from sketch decodes + eager forwards.
            let state = self.state.entry(replica_id).or_default();

            for (from, missing_set) in sketch_results {
                let to_send: Vec<Element> = {
                    let sent = state.sent_to.entry(from).or_default();
                    next_set
                        .iter()
                        .filter(|e| missing_set.contains(&e.digest) && sent.insert(e.digest))
                        .cloned()
                        .collect()
                };
                if !to_send.is_empty() {
                    state.pending.entry(from).or_default().extend(to_send);
                }
            }

            // Eager-forward newly received elements to all other neighbours.
            let neighbors: Vec<usize> = topology.neighbors(replica_id).to_vec();
            for (element, from_nb) in &newly_received {
                for &nb in &neighbors {
                    if nb == *from_nb {
                        continue;
                    }
                    let queued = {
                        let sent = state.sent_to.entry(nb).or_default();
                        sent.insert(element.digest)
                    };
                    if queued {
                        state.pending.entry(nb).or_default().push(element.clone());
                    }
                }
            }
        }

        ProtocolStepResult {
            next_set,
            metrics: LocalMetrics::default(),
        }
    }
}
