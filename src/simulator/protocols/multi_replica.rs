use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::simulator::algorithms::rateless_bloom::bayesian_cost::RATELESS_SET_RECONCILIATION_OVERHEAD;
use crate::simulator::algorithms::rateless_bloom::expected_cost::ExpectedCostFactory;
use crate::simulator::algorithms::rateless_bloom::{RatelessBF, StoppingStrategyFactory};
use crate::simulator::network::{RecvView, SendView};
use crate::simulator::protocols::messages::{ProtocolMsg, SimulatorHint};
use crate::simulator::protocols::{LocalMetrics, Protocol, ProtocolKind, ProtocolStepResult};
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::{Topology, TopologyKind};

/// Topology-dispatched hybrid BF+RIBLT protocol.
///
/// Star/Tree: symmetric BF+RIBLT sketch to all neighbours each round,
///   then element exchange, with eager forwarding of newly received
///   elements to all other neighbours.
///
/// Chord: sequential ascending-distance reconciliation. Each round
///   pair (sketch + elements) covers the neighbours at distance 2^k,
///   cycling k = 0, 1, …, floor(log2 N)−1.  No eager forwarding
///   (avoids cascade storms on high-degree Chord).
pub struct MultiReplicaProtocol {
    m_ratio: f64,
    state: HashMap<usize, NodeState>,
}

struct NodeState {
    /// Elements queued to send to each neighbour (Vec for ordering).
    pending: HashMap<usize, Vec<Element>>,
    /// Per-neighbour set of already-queued digests (dedup).
    queued: HashMap<usize, HashSet<u64>>,
    /// Chord only: which power level to sketch next.
    chord_power: usize,
    /// Chord only: true while waiting to drain the element send.
    in_element_phase: bool,
    /// Set size when we last sent sketches; skip sketch if unchanged.
    last_sketch_size: usize,
}

impl Default for NodeState {
    fn default() -> Self {
        Self {
            pending: HashMap::new(),
            queued: HashMap::new(),
            chord_power: 0,
            in_element_phase: false,
            last_sketch_size: usize::MAX, // force sketch on first round
        }
    }
}

impl NodeState {
    /// Queue `element` for `neighbour`, skipping duplicates.
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

    /// Neighbours of `node` at distance 2^power in a Chord ring of size `n`.
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

    /// Number of distinct powers in a Chord ring of size `n`.
    fn chord_num_powers(n: usize) -> usize {
        if n <= 1 {
            return 0;
        }
        let mut p = 0;
        while (1usize << p) < n {
            p += 1;
        }
        p
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
        let current_size = local.set.len();

        let state = self.state.entry(replica_id).or_default();

        if topology.kind == TopologyKind::Chord {
            let n = topology.node_count();
            let num_powers = Self::chord_num_powers(n);
            if num_powers == 0 {
                return;
            }

            if state.in_element_phase {
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
                state.in_element_phase = false;
            } else {
                // Sketch round: send BF to power-k neighbours.
                let power = state.chord_power;

                // Only sketch if set has grown since last sketch.
                if current_size != state.last_sketch_size {
                    state.last_sketch_size = current_size;
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
                }
                // Advance power and enter element phase regardless (empty pending is fine).
                state.chord_power = (power + 1) % num_powers;
                state.in_element_phase = true;
            }
        } else {
            // Star / Tree
            if state.has_pending() {
                // Element round.
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
            } else {
                // Sketch round: skip if set unchanged.
                if current_size == state.last_sketch_size {
                    return;
                }
                state.last_sketch_size = current_size;
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

        let local_digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();
        let local_digest_set: HashSet<u64> = local_digests.iter().cloned().collect();

        // Newly received elements for eager forwarding (non-Chord).
        let mut new_elements: Vec<(usize, Element)> = Vec::new();

        let state = self.state.entry(replica_id).or_default();

        for (from, msg, hint) in inbox {
            match msg {
                ProtocolMsg::RatelessBloom { .. } => {
                    let (sender_digests, bloom_bits) = match hint {
                        SimulatorHint::RatelessBloomDigests { digests, bloom_bits } => {
                            (digests, bloom_bits)
                        }
                        _ => panic!("expected RatelessBloomDigests hint"),
                    };

                    let effective_m_ratio =
                        bloom_bits as f64 / sender_digests.len().max(1) as f64;
                    let mut sender_filter =
                        RatelessBF::new(sender_digests.clone(), bloom_bits);

                    let stopping_strategy = ExpectedCostFactory::new(effective_m_ratio)
                        .create(local_digests.clone(), sender_digests.len());

                    let (common, definitely_missing) =
                        sender_filter.extend_until(stopping_strategy);

                    network.record_decoded_metadata(
                        replica_id,
                        sender_filter.size_of() as u64,
                    );
                    encode_time += sender_filter.t_enc();
                    decode_time += sender_filter.t_dec();

                    // Use BF pre-filtering only: send definitely_missing elements.
                    // BF false positives (~5%) are resolved by re-sketching next round.
                    // This avoids the O(n × |diff|) RIBLT cost for large diffs.
                    let send_set: HashSet<u64> = definitely_missing.into_iter().collect();
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

        // Eager forwarding: queue new elements for all neighbours except sender.
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
                false_matches: 0,
            },
        }
    }
}
