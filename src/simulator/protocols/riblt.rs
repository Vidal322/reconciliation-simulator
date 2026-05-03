use std::collections::HashMap;
use std::time::Duration;

use crate::simulator::algorithms::riblt::RatelessIBLT;
use crate::simulator::network::{ProtocolMsg, Outbox};
use crate::simulator::protocols::{LocalMetrics, Protocol, ProtocolKind, ProtocolStepResult};
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

/// Two-round RIBLT reconciliation.
///
/// Round N:
///   - send_phase ships a `RibltMsg` (the sender's pre-built sketch)
///     to every neighbour. Wire cost is zero at send and grows as the
///     receiver decodes against it; metadata is billed by the network
///     after `recv_phase` returns.
///   - recv_phase decodes each incoming sketch and stashes the local-only
///     elements per neighbour for the next send_phase.
///
/// Round N+1:
///   - send_phase drains the stash and emits Elements messages.
///   - recv_phase merges received elements into the local set.
#[derive(Clone, Debug, Default)]
pub struct RibltProtocol {
    state: HashMap<usize, ReplicaState>,
}

#[derive(Clone, Debug, Default)]
struct ReplicaState {
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
        outbox: &mut Outbox<'_>,
    ) {
        let state = self.state.entry(replica_id).or_default();

        if state.pending.is_empty() {
            let digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();
            for &neighbor_id in topology.neighbors(replica_id) {
                let sender_riblt = RatelessIBLT::riblt_from(digests.iter().copied());
                outbox.send_riblt(replica_id, neighbor_id, sender_riblt);
            }
        } else {
            let pending = std::mem::take(&mut state.pending);
            for (neighbor_id, elements) in pending {
                if !elements.is_empty() {
                    outbox.send_elements(replica_id, neighbor_id, elements);
                }
            }
        }
    }

    fn recv_phase(
        &mut self,
        replica_id: usize,
        local: &Replica,
        _topology: &Topology,
        inbox: &mut [(usize, ProtocolMsg)],
    ) -> ProtocolStepResult {
        let mut next_set = local.snapshot_set();
        let local_digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();

        let mut encode_time = Duration::ZERO;
        let mut decode_time = Duration::ZERO;

        let state = self.state.entry(replica_id).or_default();

        for (from, msg) in inbox.iter_mut() {
            if let Some(els) = msg.take_elements() {
                for element in els {
                    next_set.insert(element);
                }
                continue;
            }

            if let Some(riblt_msg) = msg.as_riblt() {
                let mut local_riblt =
                    RatelessIBLT::riblt_from(local_digests.iter().copied());

                // After this call, riblt_msg.remote_only() == digests
                // the receiver has but the sender doesn't.
                riblt_msg.decode_against(&mut local_riblt);

                encode_time += riblt_msg.t_enc();
                decode_time += riblt_msg.t_dec();

                let local_only: std::collections::HashSet<u64> =
                    riblt_msg.remote_only().into_iter().collect();
                let to_send: Vec<Element> = local
                    .set
                    .iter()
                    .filter(|e| local_only.contains(&e.digest))
                    .cloned()
                    .collect();
                if !to_send.is_empty() {
                    state.pending.entry(*from).or_default().extend(to_send);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulator::network::Network;
    use crate::simulator::protocols::test_helpers::make_replica;
    use std::collections::HashSet;

    #[test]
    fn riblt_converges_in_two_rounds() {
        let mut protocol = RibltProtocol::new();
        let topology = Topology::star(2);
        let mut replicas = vec![make_replica(0, &[1, 2, 3]), make_replica(1, &[2, 3, 4])];
        let mut network: Network<ProtocolMsg> = Network::from_topology(&topology);

        for _ in 0..2 {
            {
                let mut outbox = Outbox::new(&mut network);
                for id in 0..replicas.len() {
                    protocol.send_phase(id, &replicas[id], &topology, &mut outbox);
                }
            }
            let mut inboxes: Vec<Vec<(usize, ProtocolMsg)>> = (0..replicas.len())
                .map(|id| network.drain_inbox(id))
                .collect();
            let next_sets: Vec<_> = inboxes
                .iter_mut()
                .enumerate()
                .map(|(id, inbox)| {
                    protocol
                        .recv_phase(id, &replicas[id], &topology, inbox)
                        .next_set
                })
                .collect();
            for inbox in &inboxes {
                network.bill_metadata_post_recv(inbox);
            }
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

        {
            let mut outbox = Outbox::new(&mut network);
            for id in 0..replicas.len() {
                protocol.send_phase(id, &replicas[id], &topology, &mut outbox);
            }
        }
        let mut inboxes: Vec<Vec<(usize, ProtocolMsg)>> = (0..replicas.len())
            .map(|id| network.drain_inbox(id))
            .collect();
        for (id, inbox) in inboxes.iter_mut().enumerate() {
            let _ = protocol.recv_phase(id, &replicas[id], &topology, inbox);
        }
        for inbox in &inboxes {
            network.bill_metadata_post_recv(inbox);
        }

        // Next send_phase should still emit only sketches (no elements pending).
        network.reset();
        {
            let mut outbox = Outbox::new(&mut network);
            for id in 0..replicas.len() {
                protocol.send_phase(id, &replicas[id], &topology, &mut outbox);
            }
        }
        for id in 0..replicas.len() {
            for (_from, mut msg) in network.drain_inbox(id) {
                assert!(msg.as_riblt().is_some());
            }
        }
    }
}
