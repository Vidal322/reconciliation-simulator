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

/// Topology-dispatched hybrid BF+RIBLT reconciliation protocol.
///
/// Star/Tree: symmetric RatelessBloom sketch exchange with all neighbours
///   each sketch round, then eager forwarding of newly received elements
///   to all other neighbours.
///
/// Chord: sequential ascending-distance reconciliation. Each round pair
///   (sketch + elements) covers the neighbours at distance 2^k, cycling
///   k = 0, 1, …, floor(log2 N)−1. No eager forwarding on Chord.
pub struct MultiReplicaProtocol {
    m_ratio: f64,
    state: HashMap<usize, NodeState>,
}

#[derive(Default)]
struct NodeState {
    /// Elements queued to send to each neighbour next element round.
    pending: HashMap<usize, Vec<Element>>,
    /// Per-neighbour dedup: digests we have already queued (prevents duplicates).
    queued: HashMap<usize, HashSet<u64>>,
    /// Chord: current power level (0 = distance-1, 1 = distance-2, …).
    chord_power: usize,
    /// Chord: true while draining pending (element round), false = sketch round.
    in_element_phase: bool,
    /// Star/Tree: true after the initial sketch has been sent. Prevents redundant
    /// re-sketches: eager forwarding alone is sufficient for multi-hop propagation.
    has_sketched: bool,
}

impl NodeState {
    fn queue_element(&mut self, neighbour: usize, element: &Element) {
        if self
            .queued
            .entry(neighbour)
            .or_default()
            .insert(element.digest)
        {
            self.pending
                .entry(neighbour)
                .or_default()
                .push(element.clone());
        }
    }

    fn has_pending(&self) -> bool {
        self.pending.values().any(|v| !v.is_empty())
    }

    fn take_pending(&mut self) -> HashMap<usize, Vec<Element>> {
        std::mem::take(&mut self.pending)
    }
}

impl MultiReplicaProtocol {
    pub fn new() -> Self {
        Self {
            m_ratio: 1.0,
            state: HashMap::new(),
        }
    }

    fn bloom_bits_for(&self, n: usize) -> usize {
        let requested = ((n as f64) * self.m_ratio).ceil().max(1.0) as usize;
        requested.max(RATELESS_SET_RECONCILIATION_OVERHEAD * 8)
    }

    /// Number of power levels for a Chord ring of size `n`.
    fn chord_num_powers(n: usize) -> usize {
        if n <= 1 {
            return 0;
        }
        let mut p = 0usize;
        while (1usize << p) < n {
            p += 1;
        }
        p
    }

    /// Forward and backward neighbours at distance 2^power in a ring of size `n`.
    fn chord_power_neighbors(node: usize, n: usize, power: usize) -> Vec<usize> {
        let offset = 1usize << power;
        if offset >= n {
            return vec![];
        }
        let fwd = (node + offset) % n;
        let bwd = (node + n - offset) % n;
        if fwd == bwd {
            vec![fwd]
        } else {
            vec![fwd, bwd]
        }
    }
}

impl Protocol for MultiReplicaProtocol {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::MultiReplica
    }

    fn send_phase(
        &mut self,
        replica_id: usize,
        local: &Replica,
        topology: &Topology,
        network: &mut SendView<ProtocolMsg>,
    ) {
        let digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();
        let bloom_bits = self.bloom_bits_for(digests.len());
        let state = self.state.entry(replica_id).or_default();

        if topology.kind == TopologyKind::Chord {
            let n = topology.node_count();
            let num_powers = Self::chord_num_powers(n);
            if num_powers == 0 {
                return;
            }

            if state.in_element_phase {
                // Element round: drain pending and send elements.
                let to_send = state.take_pending();
                for (nb, elements) in to_send {
                    if !elements.is_empty() {
                        network.send(
                            replica_id,
                            nb,
                            ProtocolMsg::Elements(elements),
                            SimulatorHint::None,
                        );
                    }
                }
                state.in_element_phase = false;
            } else {
                // Sketch round: send BF sketch to power-k neighbours.
                let power = state.chord_power;
                for nb in Self::chord_power_neighbors(replica_id, n, power) {
                    network.send(
                        replica_id,
                        nb,
                        ProtocolMsg::RatelessBloom { byte_len: 0 },
                        SimulatorHint::RatelessBloomDigests {
                            digests: digests.clone(),
                            bloom_bits,
                        },
                    );
                }
                // Advance power (ascending) and transition to element phase.
                state.chord_power = (power + 1) % num_powers;
                state.in_element_phase = true;
            }
        } else {
            // Star / Tree: one initial sketch, then pure eager-forwarding.
            // Re-sketching is redundant: BF+RIBLT fully identifies all 1-hop diffs
            // in round 1, and eager forwarding propagates multi-hop elements without
            // any additional sketch rounds.
            if state.has_pending() {
                // Element round: drain pending.
                let to_send = state.take_pending();
                for (nb, elements) in to_send {
                    if !elements.is_empty() {
                        network.send(
                            replica_id,
                            nb,
                            ProtocolMsg::Elements(elements),
                            SimulatorHint::None,
                        );
                    }
                }
            } else if !state.has_sketched {
                // Initial sketch round: send BF sketch to every neighbour once.
                state.has_sketched = true;
                for &nb in topology.neighbors(replica_id) {
                    network.send(
                        replica_id,
                        nb,
                        ProtocolMsg::RatelessBloom { byte_len: 0 },
                        SimulatorHint::RatelessBloomDigests {
                            digests: digests.clone(),
                            bloom_bits,
                        },
                    );
                }
            }
            // else: no pending and already sketched → idle this round.
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
        let mut encode_time = Duration::ZERO;
        let mut decode_time = Duration::ZERO;
        let mut false_matches = 0usize;

        let local_digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();
        let local_digest_set: HashSet<u64> = local_digests.iter().cloned().collect();

        // Collect newly received elements for eager forwarding (non-Chord).
        let mut new_elements: Vec<(usize, Element)> = Vec::new();

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

                    let effective_m_ratio =
                        bloom_bits as f64 / sender_digests.len().max(1) as f64;
                    let mut sender_filter =
                        RatelessBF::new(sender_digests.clone(), bloom_bits);

                    let stopping_strategy = ExpectedCostFactory::new(effective_m_ratio)
                        .create(local_digests.clone(), sender_digests.len());

                    // Partition local digests:
                    //   definitely_missing: local has, sender BF says NO  → send
                    //   common:             ambiguous (passed BF, might be FP) → resolve via RIBLT
                    let (common, definitely_missing) =
                        sender_filter.extend_until(stopping_strategy);

                    network.record_decoded_metadata(
                        replica_id,
                        sender_filter.size_of() as u64,
                    );
                    encode_time += sender_filter.t_enc();
                    decode_time += sender_filter.t_dec();

                    // Start with elements we definitely have that sender doesn't.
                    let mut local_only: Vec<u64> = definitely_missing;

                    // Run RIBLT on ambiguous subset to find BF false positives.
                    if !common.is_empty() {
                        let mut sender_riblt =
                            RatelessIBLT::riblt_from(sender_digests);
                        let mut common_riblt = RatelessIBLT::riblt_from(common);
                        let sketch_len =
                            sender_riblt.find_all_differences(&mut common_riblt);
                        network.record_decoded_metadata(
                            replica_id,
                            (sketch_len * mem::size_of::<u64>()) as u64,
                        );
                        encode_time += sender_riblt.t_enc();
                        decode_time += sender_riblt.t_dec();
                        // remote_only: in common but not in sender = BF FP (local has, sender doesn't)
                        let extra = sender_riblt.get_remote_only_symbols();
                        false_matches += extra.len();
                        local_only.extend(extra);
                    }

                    // Queue matching elements to ship to sender next element round.
                    let send_set: HashSet<u64> = local_only.into_iter().collect();
                    for element in local.set.iter() {
                        if send_set.contains(&element.digest) {
                            state.queue_element(from, element);
                        }
                    }
                }
                ProtocolMsg::Elements(els) => {
                    for element in els {
                        if !local_digest_set.contains(&element.digest)
                            && !next_set.contains(&element)
                        {
                            if topology.kind != TopologyKind::Chord {
                                new_elements.push((from, element.clone()));
                            }
                            next_set.insert(element);
                        }
                    }
                }
                _ => {}
            }
        }

        // Eager forwarding for Star/Tree: queue new elements for all neighbours
        // except the one we received them from.
        if topology.kind != TopologyKind::Chord {
            for (from, element) in new_elements {
                for &nb in topology.neighbors(replica_id) {
                    if nb != from {
                        state.queue_element(nb, &element);
                    }
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
        }
    }
}
