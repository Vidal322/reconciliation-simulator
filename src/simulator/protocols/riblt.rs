use std::collections::HashMap;
use std::time::Duration;

use crate::simulator::algorithms::riblt::RatelessIBLT;
use crate::simulator::network::{ProtocolMsg, Outbox};
use crate::simulator::protocols::{
    LocalMetrics, PendingElements, Protocol, ProtocolKind, ProtocolStepResult,
};
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

/// Two-round RIBLT reconciliation.
///
/// Round N:
///   - send_phase ships a `RibltMsg` (the sender's pre-built sketch)
///     to every neighbour. Wire cost is zero at send and grows as the
///     receiver decodes against it; metadata is billed by the network
///     after `recv_phase` returns.
///   - recv_phase decodes each incoming sketch and returns the
///     per-neighbour local-only elements as `carry`; the engine routes
///     that carry back into this same replica's next `send_phase`.
///
/// Round N+1:
///   - send_phase consumes the carry and emits Elements messages.
///   - recv_phase merges received elements into the local set.
#[derive(Clone, Debug, Default)]
pub struct RibltProtocol;

impl RibltProtocol {
    pub fn new() -> Self {
        Self
    }
}

impl Protocol for RibltProtocol {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::Riblt
    }

    fn send_phase(
        &self,
        replica_id: usize,
        local: &Replica,
        topology: &Topology,
        outbox: &mut Outbox<'_>,
        carry: Option<PendingElements>,
    ) {
        let pending = carry.unwrap_or_default();

        if pending.is_empty() {
            let digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();
            for &neighbor_id in topology.neighbors(replica_id) {
                let sender_riblt = RatelessIBLT::riblt_from(digests.iter().copied());
                outbox.send_riblt(neighbor_id, sender_riblt);
            }
        } else {
            for (neighbor_id, elements) in pending {
                if !elements.is_empty() {
                    outbox.send_elements(neighbor_id, elements);
                }
            }
        }
    }

    fn recv_phase(
        &self,
        _replica_id: usize,
        local: &Replica,
        _topology: &Topology,
        inbox: &mut [(usize, ProtocolMsg)],
    ) -> ProtocolStepResult {
        let mut next_set = local.snapshot_set();
        let local_digests: Vec<u64> = local.set.iter().map(|e| e.digest).collect();

        let mut encode_time = Duration::ZERO;
        let mut decode_time = Duration::ZERO;

        let mut pending: PendingElements = HashMap::new();

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
                    pending.entry(*from).or_default().extend(to_send);
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
            carry: if pending.is_empty() { None } else { Some(pending) },
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
        let protocol = RibltProtocol::new();
        let topology = Topology::star(2);
        let mut replicas = vec![make_replica(0, &[1, 2, 3]), make_replica(1, &[2, 3, 4])];
        let mut network: Network<ProtocolMsg> = Network::from_topology(&topology);
        let mut carries: Vec<Option<PendingElements>> =
            (0..replicas.len()).map(|_| None).collect();

        for _ in 0..2 {
            {
                for id in 0..replicas.len() {
                    let carry = carries[id].take();
                    let mut outbox = Outbox::for_replica(&mut network, id);
                    protocol.send_phase(id, &replicas[id], &topology, &mut outbox, carry);
                }
            }
            let mut inboxes: Vec<Vec<(usize, ProtocolMsg)>> = (0..replicas.len())
                .map(|id| network.drain_inbox(id))
                .collect();
            let results: Vec<_> = inboxes
                .iter_mut()
                .enumerate()
                .map(|(id, inbox)| {
                    protocol.recv_phase(id, &replicas[id], &topology, inbox)
                })
                .collect();
            for inbox in &inboxes {
                network.bill_metadata_post_recv(inbox);
            }
            for (id, (replica, result)) in
                replicas.iter_mut().zip(results).enumerate()
            {
                replica.set = result.next_set;
                carries[id] = result.carry;
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
        let protocol = RibltProtocol::new();
        let topology = Topology::star(2);
        let replicas = vec![make_replica(0, &[5, 6, 7]), make_replica(1, &[5, 6, 7])];
        let mut network: Network<ProtocolMsg> = Network::from_topology(&topology);
        let mut carries: Vec<Option<PendingElements>> =
            (0..replicas.len()).map(|_| None).collect();

        for id in 0..replicas.len() {
            let carry = carries[id].take();
            let mut outbox = Outbox::for_replica(&mut network, id);
            protocol.send_phase(id, &replicas[id], &topology, &mut outbox, carry);
        }
        let mut inboxes: Vec<Vec<(usize, ProtocolMsg)>> = (0..replicas.len())
            .map(|id| network.drain_inbox(id))
            .collect();
        for (id, inbox) in inboxes.iter_mut().enumerate() {
            let result = protocol.recv_phase(id, &replicas[id], &topology, inbox);
            carries[id] = result.carry;
        }
        for inbox in &inboxes {
            network.bill_metadata_post_recv(inbox);
        }

        // With identical sets, no carry should have been produced —
        // recv_phase returned `carry: None` for every replica.
        assert!(carries.iter().all(|c| c.is_none()));

        // Next send_phase should still emit only sketches (no elements pending).
        network.reset();
        for id in 0..replicas.len() {
            let carry = carries[id].take();
            let mut outbox = Outbox::for_replica(&mut network, id);
            protocol.send_phase(id, &replicas[id], &topology, &mut outbox, carry);
        }
        for id in 0..replicas.len() {
            for (_from, mut msg) in network.drain_inbox(id) {
                assert!(msg.as_riblt().is_some());
            }
        }
    }
}
