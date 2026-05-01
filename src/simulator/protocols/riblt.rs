use std::collections::HashMap;
use std::time::Duration;

use crate::simulator::algorithms::riblt::RatelessIBLT;
use crate::simulator::network::{RecvView, SendView};
use crate::simulator::protocols::messages::{ProtocolMsg, SimulatorHint};
use crate::simulator::protocols::{LocalMetrics, Protocol, ProtocolStepResult, ProtocolKind};
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

/// Two-round RIBLT reconciliation.
///
/// Round N:
///   - send_phase ships a `RibltSketch` (digests only, billed 0 here)
///     to every neighbour.
///   - recv_phase decodes each incoming sketch against the local
///     digests, bills the *real* sketch length via
///     `Network::record_decoded_metadata`, and stashes the local-only
///     elements per neighbour for the next send_phase.
///
/// Round N+1:
///   - send_phase drains the stash and emits `Elements` messages.
///   - recv_phase merges received elements into the local set.
///
/// The handshake then repeats: empty stash → fresh sketch round.
#[derive(Clone, Debug, Default)]
pub struct RibltProtocol {
    state: HashMap<usize, ReplicaState>,
}

#[derive(Clone, Debug, Default)]
struct ReplicaState {
    /// Elements queued to ship to each neighbour in the next send_phase.
    /// Empty between rounds → next send_phase will emit fresh sketches.
    pending: HashMap<usize, Vec<Element>>,
}

impl RibltProtocol {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Protocol for RibltProtocol {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::Riblt
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
            // Sketch round: ship our pre-constructed RIBLT to every
            // neighbour. Symbols billed at decode time in recv_phase.
            let digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();
            for &neighbor_id in topology.neighbors(replica_id) {
                let sender_riblt = RatelessIBLT::riblt_from(digests.iter().copied());
                network.send(
                    replica_id,
                    neighbor_id,
                    ProtocolMsg::RibltSketch { symbols: 0 },
                    SimulatorHint::Riblt(sender_riblt),
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
        _topology: &Topology,
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
                    let mut remote_riblt = match hint {
                        SimulatorHint::Riblt(r) => r,
                        _ => panic!("expected Riblt hint"),
                    };

                    // Run the RIBLT decode against the sender's
                    // sketch; bill the true sketch length now.
                    let mut local_riblt =
                        RatelessIBLT::riblt_from(local_digests.iter().copied());

                    let sketch_len = local_riblt.find_all_differences(&mut remote_riblt);
                    let metadata_bytes = (sketch_len * std::mem::size_of::<u64>()) as u64;
                    network.record_decoded_metadata(replica_id, metadata_bytes);

                    encode_time += local_riblt.t_enc();
                    decode_time += local_riblt.t_dec();

                    // local_only: digests we have that the sender doesn't.
                    // Stash the matching elements to ship next round.
                    let local_only: std::collections::HashSet<u64> =
                        local_riblt.get_local_only_symbols().into_iter().collect();
                    let to_send: Vec<Element> = local
                        .set
                        .iter()
                        .filter(|e| local_only.contains(&e.digest))
                        .cloned()
                        .collect();
                    if !to_send.is_empty() {
                        state.pending.entry(from).or_default().extend(to_send);
                    }
                }
                ProtocolMsg::Elements(els) => {
                    for element in els {
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

    #[test]
    fn riblt_converges_in_two_rounds() {
        let mut protocol = RibltProtocol::new();
        let topology = Topology::star(2);
        let mut replicas = vec![make_replica(0, &[1, 2, 3]), make_replica(1, &[2, 3, 4])];
        let mut network: Network<ProtocolMsg> = Network::from_topology(&topology);
        let mut hints = HintStore::new();

        for _ in 0..2 {
            {
                let mut send_view = SendView::new(&mut network, &mut hints);
                for id in 0..replicas.len() {
                    protocol.send_phase(id, &replicas[id], &topology, &mut send_view);
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
                            .recv_phase(id, &replicas[id], &topology, inbox, &mut recv_view)
                            .next_set
                    })
                    .collect()
            };
            for (replica, next) in replicas.iter_mut().zip(next_sets) {
                replica.set = next;
            }
        }

        let union0: HashSet<u64> = replicas[0].set.iter().map(|e| e.digest).collect();
        let union1: HashSet<u64> = replicas[1].set.iter().map(|e| e.digest).collect();
        assert!(union0.contains(&1));
        assert!(union0.contains(&2));
        assert!(union0.contains(&3));
        assert!(union0.contains(&4));
        assert_eq!(union0, union1);
        assert!(network.stats().bytes_metadata > 0);
    }

    #[test]
    fn riblt_zero_diff_only_emits_sketches() {
        let mut protocol = RibltProtocol::new();
        let topology = Topology::star(2);
        let replicas = vec![make_replica(0, &[5, 6, 7]), make_replica(1, &[5, 6, 7])];
        let mut network: Network<ProtocolMsg> = Network::from_topology(&topology);
        let mut hints = HintStore::new();

        // Sketch round: send + recv.
        {
            let mut send_view = SendView::new(&mut network, &mut hints);
            for id in 0..replicas.len() {
                protocol.send_phase(id, &replicas[id], &topology, &mut send_view);
            }
        }
        {
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
            for (id, inbox) in inboxes.into_iter().enumerate() {
                let _ = protocol.recv_phase(id, &replicas[id], &topology, inbox, &mut recv_view);
            }
        }

        // Next send_phase should still emit only sketches because the
        // pending stash is empty (no element diffs).
        network.reset();
        hints.reset();
        {
            let mut send_view = SendView::new(&mut network, &mut hints);
            for id in 0..replicas.len() {
                protocol.send_phase(id, &replicas[id], &topology, &mut send_view);
            }
        }
        for id in 0..replicas.len() {
            for (_from, msg) in network.drain_inbox(id) {
                assert!(matches!(msg, ProtocolMsg::RibltSketch { .. }));
            }
        }
    }
}
