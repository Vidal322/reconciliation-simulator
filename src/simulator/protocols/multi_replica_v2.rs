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

/// Hybrid BF+RIBLT with eager forwarding and topology-aware cascade control.
///
/// Round 1: every replica sends a single RatelessBloom sketch to all neighbours.
/// recv round 1: decode sketches → queue missing elements per neighbour.
/// Rounds 2+: drain pending elements; newly received elements are eagerly
/// forwarded to other neighbours with sent_to dedup to prevent echoes.
///
/// For Chord (high-degree, high-redundancy) eager forwarding uses hash-based
/// forwarder selection: for each (element, destination) pair, only the
/// designated neighbour (element.digest % dest_degree == my_rank_at_dest)
/// forwards, eliminating O(degree) cascade storms.
pub struct MultiReplicaV2Protocol {
    m_ratio: f64,
    state: HashMap<usize, ReplicaState>,
}

#[derive(Default)]
struct ReplicaState {
    pending: HashMap<usize, Vec<Element>>,
    /// Dedup gate: digests already committed to each neighbour.
    sent_to: HashMap<usize, HashSet<u64>>,
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
        let (sketch_digests, bloom_bits) = {
            let digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();
            let bits = self.bloom_bits_for(digests.len());
            (digests, bits)
        };

        let state = self.state.entry(replica_id).or_default();

        if !state.sketch_sent {
            for &neighbor_id in topology.neighbors(replica_id) {
                network.send(
                    replica_id,
                    neighbor_id,
                    ProtocolMsg::RatelessBloom { byte_len: 0 },
                    SimulatorHint::RatelessBloomDigests {
                        digests: sketch_digests.clone(),
                        bloom_bits,
                    },
                );
            }
            state.sketch_sent = true;
        }

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

    fn recv_phase(
        &mut self,
        replica_id: usize,
        local: &Replica,
        topology: &Topology,
        inbox: Vec<(usize, ProtocolMsg, SimulatorHint)>,
        network: &mut RecvView<ProtocolMsg>,
    ) -> ProtocolStepResult {
        let mut next_set = local.snapshot_set();
        let mut newly_received: Vec<(Element, usize)> = Vec::new();

        let state = self.state.entry(replica_id).or_default();

        // Pass 1: Elements first — fresher set for sketch decoding.
        for (from, msg, _) in &inbox {
            if let ProtocolMsg::Elements(els) = msg {
                for element in els {
                    if next_set.insert(element.clone()) {
                        newly_received.push((element.clone(), *from));
                    }
                }
            }
        }

        // Pass 2: Decode BF sketches.
        for (from, msg, hint) in inbox {
            if let ProtocolMsg::RatelessBloom { .. } = msg {
                let (sender_digests, bloom_bits) = match hint {
                    SimulatorHint::RatelessBloomDigests { digests, bloom_bits } => {
                        (digests, bloom_bits)
                    }
                    _ => panic!("expected RatelessBloomDigests hint"),
                };

                let effective_m_ratio =
                    bloom_bits as f64 / sender_digests.len().max(1) as f64;
                let mut sender_filter = RatelessBF::new(sender_digests.clone(), bloom_bits);
                let local_digests: Vec<u64> = next_set.iter().map(|e| e.digest).collect();
                let stopping_strategy = ExpectedCostFactory::new(effective_m_ratio)
                    .create(local_digests, sender_digests.len());
                let (common, definitely_missing) =
                    sender_filter.extend_until(stopping_strategy);

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

                let missing_set: HashSet<u64> = local_only.into_iter().collect();
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
        }

        // Eager-forward newly received elements to other neighbours.
        // For Chord: use hash-based forwarder selection to prevent cascade
        // storms.  Only forward element E to neighbour nb if I am the
        // designated forwarder: (E.digest % nb's degree) == my rank in nb's
        // neighbour list.  This guarantees exactly one of nb's neighbours
        // forwards each element to nb.
        let use_forwarder_selection = topology.kind == TopologyKind::Chord;
        let neighbors: Vec<usize> = topology.neighbors(replica_id).to_vec();

        for (element, from_nb) in &newly_received {
            for &nb in &neighbors {
                if nb == *from_nb {
                    continue;
                }

                if use_forwarder_selection {
                    let nb_neighbors = topology.neighbors(nb);
                    let my_rank = nb_neighbors
                        .iter()
                        .position(|&x| x == replica_id)
                        .unwrap_or(0);
                    if (element.digest as usize) % nb_neighbors.len() != my_rank {
                        continue; // not the designated forwarder for this (element, nb) pair
                    }
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

        ProtocolStepResult {
            next_set,
            metrics: LocalMetrics::default(),
        }
    }
}
