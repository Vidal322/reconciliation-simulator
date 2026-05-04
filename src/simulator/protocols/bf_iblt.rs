use std::collections::HashMap;
use std::time::Duration;

use crate::simulator::algorithms::bloom::BloomFilter;
use crate::simulator::algorithms::riblt::RatelessIBLT;
use crate::simulator::network::{ProtocolMsg, Outbox};
use crate::simulator::protocols::{
    LocalMetrics, PendingElements, Protocol, ProtocolKind, ProtocolStepResult,
};
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

/// Two-round Bloom Filter + IBLT reconciliation.
///
/// Round N: send_phase ships `(BloomFilter, RIBLT)` pairs; recv_phase
/// resolves false positives via the RIBLT and returns the resulting
/// per-neighbour local-only elements as `carry`.
/// Round N+1: send_phase consumes the carry and emits Elements.
#[derive(Clone, Debug)]
pub struct StaticBfIbltProtocol {
    false_positive_rate: f64,
}

impl Default for StaticBfIbltProtocol {
    fn default() -> Self {
        Self::new()
    }
}

impl StaticBfIbltProtocol {
    pub fn new() -> Self {
        Self {
            false_positive_rate: 0.01,
        }
    }

    pub fn with_false_positive_rate(false_positive_rate: f64) -> Self {
        assert!(
            (0.0..1.0).contains(&false_positive_rate),
            "false_positive_rate must be in (0, 1)"
        );
        Self {
            false_positive_rate,
        }
    }
}

impl Protocol for StaticBfIbltProtocol {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::StaticBfIblt
    }

    fn send_phase(
        &mut self,
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
                let mut bf: BloomFilter<u64> =
                    BloomFilter::new(digests.len().max(1), self.false_positive_rate);
                for d in &digests {
                    bf.timed_insert(d);
                }
                let riblt = RatelessIBLT::riblt_from(digests.iter().copied());
                outbox.send_bloom_riblt(replica_id, neighbor_id, bf, riblt);
            }
        } else {
            for (neighbor_id, elements) in pending {
                if !elements.is_empty() {
                    outbox.send_elements(replica_id, neighbor_id, elements);
                }
            }
        }
    }

    fn recv_phase(
        &mut self,
        _replica_id: usize,
        local: &Replica,
        _topology: &Topology,
        inbox: &mut [(usize, ProtocolMsg)],
    ) -> ProtocolStepResult {
        let mut next_set = local.snapshot_set();

        let mut encode_time = Duration::ZERO;
        let mut decode_time = Duration::ZERO;
        let mut false_matches = 0usize;

        let mut pending: PendingElements = HashMap::new();

        for (from, msg) in inbox.iter_mut() {
            if let Some(els) = msg.take_elements() {
                for element in els {
                    next_set.insert(element);
                }
                continue;
            }

            if let Some((bloom, sender_riblt)) = msg.as_bloom_riblt() {
                let local_digests: Vec<u64> =
                    local.set.iter().map(|e| e.digest).collect();

                let mut confirmed_local_only = Vec::new();
                let mut candidate_positives = Vec::new();

                for digest in &local_digests {
                    if bloom.contains(digest) {
                        candidate_positives.push(*digest);
                    } else {
                        confirmed_local_only.push(*digest);
                    }
                }
                encode_time += bloom.t_enc();
                decode_time += bloom.t_dec();

                let mut recovered_local_only = confirmed_local_only;

                if !candidate_positives.is_empty() {
                    let mut candidate_riblt = RatelessIBLT::riblt_from(candidate_positives);

                    sender_riblt.decode_against(&mut candidate_riblt);
                    encode_time += sender_riblt.t_enc();
                    decode_time += sender_riblt.t_dec();

                    let riblt_local_only = sender_riblt.remote_only();
                    false_matches += riblt_local_only.len();
                    recovered_local_only.extend(riblt_local_only);
                }

                let local_only_set: std::collections::HashSet<u64> =
                    recovered_local_only.into_iter().collect();
                let to_send: Vec<Element> = local
                    .set
                    .iter()
                    .filter(|e| local_only_set.contains(&e.digest))
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
                false_matches,
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
    fn bf_iblt_converges_in_two_rounds() {
        let mut protocol = StaticBfIbltProtocol::new();
        let topology = Topology::star(2);
        let mut replicas = vec![make_replica(0, &[1, 2, 3]), make_replica(1, &[2, 3, 4])];
        let mut network: Network<ProtocolMsg> = Network::from_topology(&topology);
        let mut carries: Vec<Option<PendingElements>> =
            (0..replicas.len()).map(|_| None).collect();

        for _ in 0..2 {
            {
                let mut outbox = Outbox::new(&mut network);
                for id in 0..replicas.len() {
                    let carry = carries[id].take();
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
    fn bf_iblt_identical_sets_no_elements_sent() {
        let mut protocol = StaticBfIbltProtocol::new();
        let topology = Topology::star(2);
        let replicas = vec![make_replica(0, &[5, 6, 7]), make_replica(1, &[5, 6, 7])];
        let mut network: Network<ProtocolMsg> = Network::from_topology(&topology);
        let mut carries: Vec<Option<PendingElements>> =
            (0..replicas.len()).map(|_| None).collect();

        {
            let mut outbox = Outbox::new(&mut network);
            for id in 0..replicas.len() {
                let carry = carries[id].take();
                protocol.send_phase(id, &replicas[id], &topology, &mut outbox, carry);
            }
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

        // With identical sets, the BF resolves every probe to a true
        // negative on the receiver side, so no carry is produced.
        assert!(carries.iter().all(|c| c.is_none()));

        network.reset();
        {
            let mut outbox = Outbox::new(&mut network);
            for id in 0..replicas.len() {
                let carry = carries[id].take();
                protocol.send_phase(id, &replicas[id], &topology, &mut outbox, carry);
            }
        }
        for id in 0..replicas.len() {
            for (_from, mut msg) in network.drain_inbox(id) {
                assert!(msg.as_bloom_riblt().is_some());
            }
        }
    }
}
