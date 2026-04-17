use std::collections::{HashMap, HashSet};
use std::mem;

use crate::simulator::algorithms::bloom::BloomFilter;
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
/// Returns (b_only, a_only):
///   b_only = digests receiver has that sender lacks (local-only)
///   a_only = digests sender has that receiver lacks (remote-only)
fn decode_bf_sketch(
    replica_id: usize,
    sender_digests: Vec<u64>,
    bloom_bits: usize,
    local_digests: &[u64],
    network: &mut RecvView<ProtocolMsg>,
) -> (Vec<u64>, Vec<u64>) {
    let effective_m_ratio = bloom_bits as f64 / sender_digests.len().max(1) as f64;
    let mut sender_filter = RatelessBF::new(sender_digests.clone(), bloom_bits);
    let stopping_strategy = ExpectedCostFactory::new(effective_m_ratio)
        .create(local_digests.to_vec(), sender_digests.len());
    let (common, definitely_missing) = sender_filter.extend_until(stopping_strategy);
    network.record_decoded_metadata(replica_id, sender_filter.size_of() as u64);

    let mut b_only: Vec<u64> = definitely_missing;
    let mut a_only: Vec<u64> = Vec::new();
    if !common.is_empty() {
        let mut sender_riblt = RatelessIBLT::riblt_from(sender_digests);
        let mut common_riblt = RatelessIBLT::riblt_from(common);
        let sketch_len = sender_riblt.find_all_differences(&mut common_riblt);
        network.record_decoded_metadata(
            replica_id,
            (sketch_len * mem::size_of::<u64>()) as u64,
        );
        b_only.extend(sender_riblt.get_remote_only_symbols());
        a_only.extend(sender_riblt.get_local_only_symbols());
    }
    (b_only, a_only)
}

const REQUEST_FPR: f64 = 0.001;

/// Topology-aware hybrid protocol.
///
/// Star/Tree: one BF+RIBLT sketch in round 1, then eager-forward every
///   newly received element to all other neighbours (sent_to dedup).
///
/// Chord: asymmetric distance-doubling.  For each power 0→4, a 3-step cycle:
///   Step 0: lower-id node sketches to higher-id (one direction only).
///   Step 1: higher-id drains b-only elements to lower-id + sends RibltSketch
///           request listing a-only digests it needs.
///   Step 2: lower-id drains a-only response elements to higher-id.
///
/// This halves the BF+RIBLT metadata vs symmetric pairwise sketching:
/// the receiver decodes both directions of the diff from one sketch,
/// and communicates the missing a-only digests via a cheap per-digest request.
pub struct MultiReplicaV2Protocol {
    m_ratio: f64,
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
    /// Chord: overall round counter (power = chord_round/3 % 5, step = chord_round%3).
    chord_round: usize,
    /// Chord asymmetric: a-only digests to request from the sketching node in step 1.
    chord_pending_requests: HashMap<usize, Vec<u64>>,
}

impl MultiReplicaV2Protocol {
    pub fn new() -> Self {
        Self {
            m_ratio: 1.0,
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
        let local_digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();
        let bloom_bits = self.bloom_bits_for(
            local_digests.len(),
            topology.kind == TopologyKind::Chord,
        );

        let state = self.state.entry(replica_id).or_default();

        if topology.kind == TopologyKind::Chord {
            let n = topology.node_count();
            let power = (state.chord_round / 3) % 5;
            let step = state.chord_round % 3;
            state.chord_round += 1;

            let offset = 1usize << power;
            let forward = (replica_id + offset) % n;
            let backward = (replica_id + n - offset) % n;

            if step == 0 {
                // Asymmetric sketch: only lower-id → higher-id.
                // At power 4 (large sets, small diff), pure RIBLT is cheaper than BF+RIBLT
                // because BF overhead scales with set size, not diff size.
                let targets: Vec<usize> = if forward == backward {
                    vec![forward]
                } else {
                    vec![forward, backward]
                };
                for nb in targets {
                    if topology.is_neighbor(replica_id, nb) && replica_id < nb {
                        if power == 4 {
                            // Pure RIBLT sketch: billed at receiver via record_decoded_metadata.
                            network.send(
                                replica_id,
                                nb,
                                ProtocolMsg::RibltSketch { symbols: 0 },
                                SimulatorHint::RibltDigests {
                                    digests: local_digests.clone(),
                                },
                            );
                        } else {
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
                    }
                }
                // Drain any leftover pending from previous power's step 2.
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
            } else if step == 1 {
                // Higher-id drains b-only to lower-id + sends BF-encoded request for a-only.
                // BF billing (~9.6 bits/element at FPR=1%) is ~6.7× cheaper than raw digest
                // list (64 bits/element via RibltSketch).
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
                let requests = std::mem::take(&mut state.chord_pending_requests);
                for (nb, digests) in requests {
                    if !digests.is_empty() {
                        let bf_byte_len = {
                            let tmp: BloomFilter<u64> = BloomFilter::new(digests.len(), REQUEST_FPR);
                            tmp.byte_len()
                        };
                        network.send(
                            replica_id,
                            nb,
                            ProtocolMsg::RatelessBloom { byte_len: bf_byte_len },
                            SimulatorHint::BloomDigests {
                                digests,
                                false_positive_rate: REQUEST_FPR,
                            },
                        );
                    }
                }
            } else {
                // step == 2: lower-id drains a-only response to higher-id.
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
            let n = topology.node_count();
            let is_star_hub = topology.kind == TopologyKind::Star
                && topology.neighbors(replica_id).len() == n.saturating_sub(1);

            if is_star_hub {
                // Star hub: never sends sketch — uses asymmetric approach.
                // Round 1 send: nothing (pending/requests empty).
                // Round 2 send: hub_only elements to leaves + BF requests for leaf_only.
                state.sketch_sent = true;
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
                let requests = std::mem::take(&mut state.chord_pending_requests);
                for (nb, digests) in requests {
                    if !digests.is_empty() {
                        let bf_byte_len = {
                            let tmp: BloomFilter<u64> = BloomFilter::new(digests.len(), REQUEST_FPR);
                            tmp.byte_len()
                        };
                        network.send(
                            replica_id,
                            nb,
                            ProtocolMsg::RatelessBloom { byte_len: bf_byte_len },
                            SimulatorHint::BloomDigests {
                                digests,
                                false_positive_rate: REQUEST_FPR,
                            },
                        );
                    }
                }
            } else if topology.kind == TopologyKind::Tree {
                // Tree: asymmetric per edge — lower-id sketches to higher-id.
                // Higher-id decodes both b_only (sends to lower-id) and a_only (BF request).
                if !state.sketch_sent {
                    for &nb in topology.neighbors(replica_id) {
                        if replica_id < nb {
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
                let requests = std::mem::take(&mut state.chord_pending_requests);
                for (nb, digests) in requests {
                    if !digests.is_empty() {
                        let bf_byte_len = {
                            let tmp: BloomFilter<u64> = BloomFilter::new(digests.len(), REQUEST_FPR);
                            tmp.byte_len()
                        };
                        network.send(
                            replica_id,
                            nb,
                            ProtocolMsg::RatelessBloom { byte_len: bf_byte_len },
                            SimulatorHint::BloomDigests {
                                digests,
                                false_positive_rate: REQUEST_FPR,
                            },
                        );
                    }
                }
            } else {
                // Star leaves: sketch to hub (which is lower-id = node 0).
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
            // ── Chord: asymmetric pairwise, no eager forwarding ──────────────

            // Pass 1: merge received elements.
            for (_, msg, _) in &inbox {
                if let ProtocolMsg::Elements(els) = msg {
                    for el in els {
                        next_set.insert(el.clone());
                    }
                }
            }

            let local_digests: Vec<u64> = next_set.iter().map(|e| e.digest).collect();
            // Build digest lookup for fulfilling element requests.
            let local_by_digest: HashMap<u64, Element> =
                next_set.iter().map(|e| (e.digest, e.clone())).collect();

            let mut pending_updates: Vec<(usize, Vec<Element>)> = Vec::new();
            let mut request_updates: Vec<(usize, Vec<u64>)> = Vec::new();

            for (from, msg, hint) in inbox {
                match (msg, hint) {
                    (ProtocolMsg::RatelessBloom { .. }, SimulatorHint::RatelessBloomDigests { digests, bloom_bits }) => {
                        // Sketch from lower-id: decode BF+RIBLT to learn b-only and a-only.
                        let (b_only, a_only) = decode_bf_sketch(
                            replica_id,
                            digests,
                            bloom_bits,
                            &local_digests,
                            network,
                        );
                        let b_only_set: HashSet<u64> = b_only.into_iter().collect();
                        let to_send: Vec<Element> = next_set
                            .iter()
                            .filter(|e| b_only_set.contains(&e.digest))
                            .cloned()
                            .collect();
                        if !to_send.is_empty() {
                            pending_updates.push((from, to_send));
                        }
                        if !a_only.is_empty() {
                            request_updates.push((from, a_only));
                        }
                    }
                    (ProtocolMsg::RatelessBloom { .. }, SimulatorHint::BloomDigests { digests, false_positive_rate }) => {
                        // BF request from higher-id: build BF, query local elements (simulates FPR).
                        let mut bf: BloomFilter<u64> = BloomFilter::new(digests.len().max(1), false_positive_rate);
                        for &d in &digests {
                            bf.insert(&d);
                        }
                        let to_send: Vec<Element> = next_set
                            .iter()
                            .filter(|e| bf.contains(&e.digest))
                            .cloned()
                            .collect();
                        if !to_send.is_empty() {
                            pending_updates.push((from, to_send));
                        }
                    }
                    (ProtocolMsg::RibltSketch { .. }, SimulatorHint::RibltDigests { digests }) => {
                        // Pure RIBLT sketch from lower-id (power 4): large sets, small diff.
                        // One-way sketch; decode gives both b-only (remote) and a-only (local).
                        let mut sender_riblt = RatelessIBLT::riblt_from(digests);
                        let mut local_riblt = RatelessIBLT::riblt_from(local_digests.iter().cloned());
                        let sketch_len = sender_riblt.find_all_differences(&mut local_riblt);
                        network.record_decoded_metadata(
                            replica_id,
                            (sketch_len * mem::size_of::<u64>()) as u64,
                        );
                        let b_only = sender_riblt.get_remote_only_symbols();
                        let a_only = sender_riblt.get_local_only_symbols();
                        let b_only_set: HashSet<u64> = b_only.into_iter().collect();
                        let to_send: Vec<Element> = next_set
                            .iter()
                            .filter(|e| b_only_set.contains(&e.digest))
                            .cloned()
                            .collect();
                        if !to_send.is_empty() {
                            pending_updates.push((from, to_send));
                        }
                        if !a_only.is_empty() {
                            request_updates.push((from, a_only));
                        }
                    }
                    _ => {}
                }
            }

            let state = self.state.entry(replica_id).or_default();
            for (from, to_send) in pending_updates {
                state.pending.entry(from).or_default().extend(to_send);
            }
            for (from, digests) in request_updates {
                state
                    .chord_pending_requests
                    .entry(from)
                    .or_default()
                    .extend(digests);
            }
        } else {
            // ── Star/Tree ─────────────────────────────────────────────────────
            let n = topology.node_count();
            let is_star_hub = topology.kind == TopologyKind::Star
                && topology.neighbors(replica_id).len() == n.saturating_sub(1);

            let mut newly_received: Vec<(Element, usize)> = Vec::new();
            for (from, msg, _) in &inbox {
                if let ProtocolMsg::Elements(els) = msg {
                    for element in els {
                        if next_set.insert(element.clone()) {
                            newly_received.push((element.clone(), *from));
                        }
                    }
                }
            }

            let local_digests: Vec<u64> = next_set.iter().map(|e| e.digest).collect();

            if is_star_hub {
                // Star hub: decode leaf sketches → b_only→pending, a_only→requests.
                // Eager-forward elements received from leaves to other leaves.
                // Dedup a_only across leaves: each missing element is requested from
                // exactly ONE leaf (the first processed that has it), avoiding
                // duplicate responses when multiple leaves share an element hub lacks.
                let mut pending_updates: Vec<(usize, Vec<Element>)> = Vec::new();
                let mut request_updates: Vec<(usize, Vec<u64>)> = Vec::new();
                let mut already_requested: HashSet<u64> = HashSet::new();

                for (from, msg, hint) in inbox {
                    match (msg, hint) {
                        (
                            ProtocolMsg::RatelessBloom { .. },
                            SimulatorHint::RatelessBloomDigests { digests, bloom_bits },
                        ) => {
                            let (b_only, a_only) = decode_bf_sketch(
                                replica_id,
                                digests,
                                bloom_bits,
                                &local_digests,
                                network,
                            );
                            let b_only_set: HashSet<u64> = b_only.into_iter().collect();
                            let to_send: Vec<Element> = next_set
                                .iter()
                                .filter(|e| b_only_set.contains(&e.digest))
                                .cloned()
                                .collect();
                            if !to_send.is_empty() {
                                pending_updates.push((from, to_send));
                            }
                            // Only request elements not yet requested from another leaf.
                            let new_requests: Vec<u64> = a_only
                                .into_iter()
                                .filter(|d| already_requested.insert(*d))
                                .collect();
                            if !new_requests.is_empty() {
                                request_updates.push((from, new_requests));
                            }
                        }
                        _ => {}
                    }
                }

                let state = self.state.entry(replica_id).or_default();
                for (from, elements) in pending_updates {
                    let sent = state.sent_to.entry(from).or_default();
                    let deduped: Vec<Element> =
                        elements.into_iter().filter(|e| sent.insert(e.digest)).collect();
                    if !deduped.is_empty() {
                        state.pending.entry(from).or_default().extend(deduped);
                    }
                }
                for (from, digests) in request_updates {
                    state
                        .chord_pending_requests
                        .entry(from)
                        .or_default()
                        .extend(digests);
                }
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
            } else {
                // Star leaves + Tree: decode neighbor sketches; handle BF requests.
                // For Tree: higher-id decodes lower-id's sketch → b_only→pending,
                //           a_only→chord_pending_requests (sent as BF request next round).
                // For Star leaves: receives BF request from hub (BloomDigests path).
                let mut sketch_results: Vec<(usize, HashSet<u64>, Vec<u64>)> = Vec::new();
                let mut bf_request_results: Vec<(usize, Vec<Element>)> = Vec::new();

                for (from, msg, hint) in inbox {
                    if let ProtocolMsg::RatelessBloom { .. } = msg {
                        match hint {
                            SimulatorHint::RatelessBloomDigests { digests, bloom_bits } => {
                                let (b_only, a_only) = decode_bf_sketch(
                                    replica_id,
                                    digests,
                                    bloom_bits,
                                    &local_digests,
                                    network,
                                );
                                sketch_results.push((from, b_only.into_iter().collect(), a_only));
                            }
                            SimulatorHint::BloomDigests {
                                digests,
                                false_positive_rate,
                            } => {
                                // BF request: send back matching local elements.
                                let mut bf: BloomFilter<u64> =
                                    BloomFilter::new(digests.len().max(1), false_positive_rate);
                                for &d in &digests {
                                    bf.insert(&d);
                                }
                                let to_send: Vec<Element> = next_set
                                    .iter()
                                    .filter(|e| bf.contains(&e.digest))
                                    .cloned()
                                    .collect();
                                if !to_send.is_empty() {
                                    bf_request_results.push((from, to_send));
                                }
                            }
                            _ => panic!("unexpected hint for RatelessBloom"),
                        }
                    }
                }

                let state = self.state.entry(replica_id).or_default();
                for (from, missing_set, a_only) in sketch_results {
                    // b_only: queue elements to send to sketcher
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
                    // a_only: request from sketcher via BF next round (Tree only; no-op for Star leaves)
                    if !a_only.is_empty() {
                        state
                            .chord_pending_requests
                            .entry(from)
                            .or_default()
                            .extend(a_only);
                    }
                }
                for (from, elements) in bf_request_results {
                    state.pending.entry(from).or_default().extend(elements);
                }

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
        }

        ProtocolStepResult {
            next_set,
            metrics: LocalMetrics::default(),
        }
    }
}
