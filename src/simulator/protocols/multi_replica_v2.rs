use std::collections::{HashMap, HashSet};
use std::mem;
use std::time::Duration;

use crate::simulator::algorithms::riblt::RatelessIBLT;
use crate::simulator::network::{RecvView, SendView};
use crate::simulator::protocols::messages::{ProtocolMsg, SimulatorHint};
use crate::simulator::protocols::{LocalMetrics, Protocol, ProtocolKind, ProtocolStepResult};
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

/// RIBLT-based reconciliation with gossip suppression.
///
/// Each 2-round cycle:
/// - Sketch round: send RIBLT sketch to every neighbour.
/// - Element round: send only elements the neighbour is missing.
///
/// Gossip suppression: when building to_send for neighbour X, skip
/// elements that were received from a node that is also X's neighbour.
/// Since that node sent the element to X simultaneously, X likely
/// already received it. This reduces redundant multi-path sends on
/// high-degree topologies (Chord).
pub struct MultiReplicaV2Protocol {
    state: HashMap<usize, ReplicaState>,
}

#[derive(Clone, Debug, Default)]
struct ReplicaState {
    /// Elements queued to ship to each neighbour in the next send_phase.
    pending: HashMap<usize, Vec<Element>>,
    /// For each digest, which neighbours sent it to us (for gossip suppression).
    received_from: HashMap<u64, Vec<usize>>,
}

impl MultiReplicaV2Protocol {
    pub fn new() -> Self {
        Self {
            state: HashMap::new(),
        }
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

        if state.pending.is_empty() {
            // Sketch round: send RIBLT sketch to every neighbour.
            // Symbols are billed at decode time in recv_phase.
            let digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();
            for &neighbor_id in topology.neighbors(replica_id) {
                network.send(
                    replica_id,
                    neighbor_id,
                    ProtocolMsg::RibltSketch { symbols: 0 },
                    SimulatorHint::RibltDigests { digests: digests.clone() },
                );
            }
        } else {
            // Element round: drain the stash and ship.
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
        topology: &Topology,
        inbox: Vec<(usize, ProtocolMsg, SimulatorHint)>,
        network: &mut RecvView<ProtocolMsg>,
    ) -> ProtocolStepResult {
        let mut next_set = local.snapshot_set();
        let local_digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();

        let mut encode_time = Duration::ZERO;
        let mut decode_time = Duration::ZERO;

        let state = self.state.entry(replica_id).or_default();

        for (from, msg, hint) in inbox {
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

                    // local_only: digests we have that the sender doesn't.
                    let local_only: HashSet<u64> =
                        local_riblt.get_local_only_symbols().into_iter().collect();

                    // Neighbours of `from` — used for gossip suppression.
                    let from_neighbors = topology.neighbors(from);

                    // Build to_send with gossip suppression:
                    // Skip elements that were received from a node that is
                    // also `from`'s neighbour, since `from` likely received
                    // them from that shared neighbour already.
                    let to_send: Vec<Element> = local
                        .set
                        .iter()
                        .filter(|e| {
                            if !local_only.contains(&e.digest) {
                                return false;
                            }
                            // Gossip suppression check.
                            if let Some(sources) = state.received_from.get(&e.digest) {
                                if sources.iter().any(|&src| from_neighbors.contains(&src)) {
                                    return false;
                                }
                            }
                            true
                        })
                        .cloned()
                        .collect();

                    if !to_send.is_empty() {
                        state.pending.entry(from).or_default().extend(to_send);
                    }
                }
                ProtocolMsg::Elements(els) => {
                    for element in els {
                        // Track which neighbour sent us this element.
                        state
                            .received_from
                            .entry(element.digest)
                            .or_default()
                            .push(from);
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
                false_matches: 0,
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
        assert!(union.contains(&1));
        assert!(union.contains(&4));
        let union1: HashSet<u64> = replicas[1].set.iter().map(|e| e.digest).collect();
        assert_eq!(union, union1);
    }

    #[test]
    fn converges_on_three_node_star() {
        let mut protocol = MultiReplicaV2Protocol::new();
        let topology = Topology::star(3);
        let mut replicas = vec![
            make_replica(0, &[1, 2]),
            make_replica(1, &[2, 3]),
            make_replica(2, &[3, 4]),
        ];
        run_protocol(&mut protocol, &mut replicas, &topology, 6);

        let expected: HashSet<u64> = [1, 2, 3, 4].into_iter().collect();
        for r in &replicas {
            let got: HashSet<u64> = r.set.iter().map(|e| e.digest).collect();
            assert_eq!(got, expected);
        }
    }
}
