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

/// False-positive rate for compact request Bloom filters.
const REQUEST_BF_FPR: f64 = 0.001;

/// Topology-dispatched hybrid BF+RIBLT reconciliation protocol with asymmetric sketching.
///
/// **Asymmetric sketching** (all topologies): only the lower-id node sends the full
/// RatelessBloom sketch. The higher-id node decodes it in both directions:
///   - elements it has that the sender lacks → queue in pending (as before)
///   - elements the sender has that it lacks (RIBLT local_only) → encode as a compact
///     BloomFilter "request" and send back in the element round
/// The lower-id node receives the request BF, looks up the requested elements from its
/// local set, and queues them for delivery. This halves the number of full BF+RIBLT
/// sessions without reducing the total state transferred.
///
/// Star/Tree: single asymmetric sketch + eager forwarding for multi-hop propagation.
/// Chord:     sequential ascending-power reconciliation, each power asymmetric.
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
    /// Star/Tree: true after the initial sketch has been sent.
    has_sketched: bool,
    /// Asymmetric: digests that a lower-id neighbour has but I (higher-id) don't.
    /// Populated when I decode the lower-id's BF sketch via RIBLT local_only.
    /// Sent as a compact BloomFilter request in the next element round.
    request_to_send: HashMap<usize, Vec<u64>>,
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

    fn has_requests(&self) -> bool {
        self.request_to_send.values().any(|v| !v.is_empty())
    }

    fn take_pending(&mut self) -> HashMap<usize, Vec<Element>> {
        std::mem::take(&mut self.pending)
    }

    fn take_requests(&mut self) -> HashMap<usize, Vec<u64>> {
        std::mem::take(&mut self.request_to_send)
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

    /// Bit-length for a request BloomFilter with `n` elements at `REQUEST_BF_FPR`.
    fn request_bf_bit_len(n: usize) -> usize {
        if n == 0 {
            return 1;
        }
        let ln2 = std::f64::consts::LN_2;
        ((-1.0f64 * n as f64 * REQUEST_BF_FPR.ln()) / (ln2 * ln2)).ceil() as usize
    }

    /// Send all pending request BFs stored in state.request_to_send.
    fn flush_requests(
        replica_id: usize,
        state: &mut NodeState,
        network: &mut SendView<ProtocolMsg>,
    ) {
        let requests = state.take_requests();
        for (nb, req_digests) in requests {
            if !req_digests.is_empty() {
                let bit_len = Self::request_bf_bit_len(req_digests.len());
                network.send(
                    replica_id,
                    nb,
                    ProtocolMsg::BloomFilter { bit_len },
                    SimulatorHint::BloomDigests {
                        digests: req_digests,
                        false_positive_rate: REQUEST_BF_FPR,
                    },
                );
            }
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
                // Element round: drain pending elements.
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
                // Sketch round: both neighbours exchange BF sketches (symmetric).
                // Asymmetric sketching for Chord breaks convergence because the
                // request-response delay causes stale sets at higher power levels.
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
                state.chord_power = (power + 1) % num_powers;
                state.in_element_phase = true;
            }
        } else {
            // Star / Tree: one initial asymmetric sketch, then pure eager-forwarding.
            if state.has_pending() || state.has_requests() {
                // Element round: send pending elements + request BFs together.
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
                Self::flush_requests(replica_id, state, network);
            } else if !state.has_sketched {
                // Initial sketch round: only lower-id sends to higher-id neighbours.
                state.has_sketched = true;
                for &nb in topology.neighbors(replica_id) {
                    if replica_id < nb {
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
            // else: no pending, no requests, already sketched → idle.
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

        let mut new_elements: Vec<(usize, Element)> = Vec::new();

        let state = self.state.entry(replica_id).or_default();

        // Track which digests each neighbour has already sent us this round.
        // Used to suppress forwarding elements that a neighbour demonstrably already has.
        let mut received_digests: HashMap<usize, HashSet<u64>> = HashMap::new();

        for (from, msg, hint) in inbox {
            match msg {
                ProtocolMsg::RatelessBloom { .. } => {
                    // I am the higher-id receiver; `from` (lower-id) sent the sketch.
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

                    let (common, definitely_missing) =
                        sender_filter.extend_until(stopping_strategy);

                    network.record_decoded_metadata(
                        replica_id,
                        sender_filter.size_of() as u64,
                    );
                    encode_time += sender_filter.t_enc();
                    decode_time += sender_filter.t_dec();

                    let mut local_only: Vec<u64> = definitely_missing;

                    if !common.is_empty() {
                        // RIBLT resolves BF false positives AND reveals what sender has
                        // that we (higher-id) don't — captured as a request BF.
                        let mut sender_riblt =
                            RatelessIBLT::riblt_from(sender_digests.iter().cloned());
                        let mut common_riblt = RatelessIBLT::riblt_from(common);
                        let sketch_len =
                            sender_riblt.find_all_differences(&mut common_riblt);
                        network.record_decoded_metadata(
                            replica_id,
                            (sketch_len * mem::size_of::<u64>()) as u64,
                        );
                        encode_time += sender_riblt.t_enc();
                        decode_time += sender_riblt.t_dec();

                        // remote_only: BF false positives → we have, sender doesn't.
                        let extra = sender_riblt.get_remote_only_symbols();
                        false_matches += extra.len();
                        local_only.extend(extra);

                        // For Star/Tree only: capture what sender has that we need,
                        // to be sent back as a compact request BF.
                        // Chord stays symmetric (avoids stale-set cascades).
                        if topology.kind != TopologyKind::Chord {
                            let i_need = sender_riblt.get_local_only_symbols();
                            if !i_need.is_empty() {
                                state
                                    .request_to_send
                                    .entry(from)
                                    .or_default()
                                    .extend(i_need);
                            }
                        }
                    } else if topology.kind != TopologyKind::Chord {
                        // No common elements (very rare): request all of sender's elements.
                        if !sender_digests.is_empty() {
                            state
                                .request_to_send
                                .entry(from)
                                .or_default()
                                .extend(sender_digests.iter().cloned());
                        }
                    }

                    // Queue our elements that sender is missing.
                    let send_set: HashSet<u64> = local_only.into_iter().collect();
                    for element in local.set.iter() {
                        if send_set.contains(&element.digest) {
                            state.queue_element(from, element);
                        }
                    }
                }

                ProtocolMsg::BloomFilter { .. } => {
                    // Received a request BF from a higher-id neighbour.
                    // They need elements that I (lower-id) have.
                    // The hint carries the exact digests; use them for zero-error lookup.
                    let requested_digests = match hint {
                        SimulatorHint::BloomDigests { digests, .. } => digests,
                        _ => panic!("expected BloomDigests hint for request BF"),
                    };
                    let requested: HashSet<u64> = requested_digests.into_iter().collect();
                    for element in local.set.iter() {
                        if requested.contains(&element.digest) {
                            state.queue_element(from, element);
                        }
                    }
                }

                ProtocolMsg::Elements(els) => {
                    for element in els {
                        // Record that `from` sent us this digest (they have it).
                        if topology.kind != TopologyKind::Chord {
                            received_digests
                                .entry(from)
                                .or_default()
                                .insert(element.digest);
                        }
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

        // Eager forwarding for Star/Tree: propagate newly received elements
        // to all neighbours except the source and any neighbour that already has it
        // (evidenced by them having sent it to us this round).
        if topology.kind != TopologyKind::Chord {
            for (from, element) in new_elements {
                for &nb in topology.neighbors(replica_id) {
                    if nb != from {
                        let nb_has_it = received_digests
                            .get(&nb)
                            .map_or(false, |s| s.contains(&element.digest));
                        if !nb_has_it {
                            state.queue_element(nb, &element);
                        }
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
