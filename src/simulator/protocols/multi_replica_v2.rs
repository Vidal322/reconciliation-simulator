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
/// Chord: pairwise BF+RIBLT backbone + source-neighbor-suppressed eager
///   forwarding.  When receiving element E from neighbor A, forward to
///   neighbor B only if A is NOT a direct neighbor of B (since B would
///   otherwise receive E from A via pairwise sketch directly).  This
///   accelerates multi-hop propagation without full cascade storms.
pub struct MultiReplicaV2Protocol {
    m_ratio: f64,
    state: HashMap<usize, ReplicaState>,
}

#[derive(Default)]
struct ReplicaState {
    pending: HashMap<usize, Vec<Element>>,
    /// Dedup gate for eager-forward (both Star/Tree and Chord).
    sent_to: HashMap<usize, HashSet<u64>>,
    /// One-shot sketch flag for Star/Tree.
    sketch_sent: bool,
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
        // Compute sketch params before taking the state borrow.
        let local_digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();
        let bloom_bits = self.bloom_bits_for(local_digests.len());

        let state = self.state.entry(replica_id).or_default();

        if topology.kind == TopologyKind::Chord {
            // Pairwise mode: sketch when nothing is pending, elements otherwise.
            if state.pending.is_empty() {
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
            // ── Chord: pairwise backbone + source-neighbor-suppressed eager fwd ─

            // Pass 1: merge received elements, track what's new and from whom.
            let mut newly_received: Vec<(Element, usize)> = Vec::new();
            for (from, msg, _) in &inbox {
                if let ProtocolMsg::Elements(els) = msg {
                    for el in els {
                        if next_set.insert(el.clone()) {
                            newly_received.push((el.clone(), *from));
                        }
                    }
                }
            }

            // Pass 2: decode sketches.  Collect (from, to_send) without
            // holding a state borrow so decode_bf_sketch can take &mut network.
            let local_digests: Vec<u64> = next_set.iter().map(|e| e.digest).collect();
            let mut pending_updates: Vec<(usize, Vec<Element>)> = Vec::new();

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
            }

            // Flush collected updates into state.
            let state = self.state.entry(replica_id).or_default();
            for (from, to_send) in pending_updates {
                state.pending.entry(from).or_default().extend(to_send);
            }

            // Source-neighbor-suppressed eager forwarding for Chord:
            // Forward element E (received from A) to neighbor B only if
            // A is NOT a direct neighbor of B — if A IS B's neighbor, B
            // will receive E from A directly via pairwise sketch.
            let neighbors: Vec<usize> = topology.neighbors(replica_id).to_vec();
            for (element, from_nb) in &newly_received {
                for &nb in &neighbors {
                    if nb == *from_nb {
                        continue;
                    }
                    // Suppress if from_nb is a direct neighbor of nb
                    // (nb will get this element from from_nb via pairwise sketch)
                    if topology.is_neighbor(*from_nb, nb) {
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
