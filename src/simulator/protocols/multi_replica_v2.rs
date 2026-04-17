use std::collections::{HashMap, HashSet};
use std::mem;

use crate::simulator::algorithms::rateless_bloom::bayesian_cost::RATELESS_SET_RECONCILIATION_OVERHEAD;
use crate::simulator::algorithms::rateless_bloom::expected_cost::ExpectedCostFactory;
use crate::simulator::algorithms::rateless_bloom::RatelessBF;
use crate::simulator::algorithms::riblt::RatelessIBLT;
use crate::simulator::network::{RecvView, SendView};
use crate::simulator::protocols::messages::{ProtocolMsg, SimulatorHint};
use crate::simulator::protocols::{LocalMetrics, Protocol, ProtocolKind, ProtocolStepResult};
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

/// Hybrid BF+RIBLT with eager forwarding.
///
/// Round 1: every replica sends a RatelessBloom sketch to all neighbours.
/// recv round 1: decode each sketch → determine what the sender is missing →
///   queue those elements to send in the next send_phase.
/// Rounds 2+: drain pending elements; when new elements arrive, eagerly
///   forward them to all other neighbours (with sent_to dedup to prevent
///   echo and re-sends).
pub struct MultiReplicaV2Protocol {
    m_ratio: f64,
    state: HashMap<usize, ReplicaState>,
}

#[derive(Default)]
struct ReplicaState {
    /// Elements queued to send to each neighbour (pre-deduped via sent_to).
    pending: HashMap<usize, Vec<Element>>,
    /// Digests already committed to send to each neighbour.
    /// Acts as a dedup gate: once a digest is in sent_to[N], we never
    /// queue it for N again.
    sent_to: HashMap<usize, HashSet<u64>>,
    /// Whether the initial BF sketch has been sent.
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
        let state = self.state.entry(replica_id).or_default();

        // Send the BF sketch exactly once.
        if !state.sketch_sent {
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
            state.sketch_sent = true;
        }

        // Drain pending element queue.
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
        // (element, source_neighbour) — elements newly added to our set this round.
        let mut newly_received: Vec<(Element, usize)> = Vec::new();

        let state = self.state.entry(replica_id).or_default();

        // Pass 1: Process Elements messages first to get the most up-to-date
        // local set before we decode sketches.
        for (from, msg, _) in &inbox {
            if let ProtocolMsg::Elements(els) = msg {
                for element in els {
                    if next_set.insert(element.clone()) {
                        newly_received.push((element.clone(), *from));
                    }
                }
            }
        }

        // Pass 2: Decode BF sketches and queue missing elements to send.
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

                // Use the updated local set (includes newly received elements).
                let local_digests: Vec<u64> = next_set.iter().map(|e| e.digest).collect();
                let stopping_strategy = ExpectedCostFactory::new(effective_m_ratio)
                    .create(local_digests, sender_digests.len());
                let (common, definitely_missing) =
                    sender_filter.extend_until(stopping_strategy);

                network.record_decoded_metadata(replica_id, sender_filter.size_of() as u64);

                // Resolve ambiguous subset via RIBLT.
                // definitely_missing: our digests the BF says sender definitely lacks.
                // After RIBLT: sender_riblt.get_remote_only_symbols() = BF false
                // positives = digests in common that are NOT in sender's set.
                // Both are elements we have that sender doesn't → queue to send.
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

                // Queue elements the sender is missing (guarded by sent_to dedup).
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

        // Eager-forward: any element newly added to our set gets forwarded to
        // all neighbours except the one we got it from.  sent_to dedup ensures
        // we never queue the same element to the same neighbour twice.
        let neighbors: Vec<usize> = topology.neighbors(replica_id).to_vec();
        for (element, from_nb) in &newly_received {
            for &nb in &neighbors {
                if nb == *from_nb {
                    continue; // don't echo back
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
