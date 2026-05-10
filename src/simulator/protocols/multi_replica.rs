use std::collections::HashMap;

use crate::simulator::algorithms::bloom::BloomFilter;
use crate::simulator::network::{ProtocolMsg, Outbox};
use crate::simulator::protocols::{
    LocalMetrics, PendingElements, Protocol, ProtocolKind, ProtocolStepResult,
};
use crate::simulator::replica::{Element, ReplicaView};
use crate::simulator::topology::{Topology, TopologyKind};

/// Sentinel key in the carry map that stores the chord power counter.
/// `usize::MAX` is never a valid neighbour id, so this slot is invisible to
/// the delivery loop in send_phase.
const CHORD_POWER_KEY: usize = usize::MAX;

/// Topology-dispatched BloomFilter reconciliation.
///
/// * Star, Tree: each node sketches its current set to every neighbour
///   per sketch round. Receivers identify the elements the sender lacks
///   and send them back as Elements in the next delivery round.
/// * Chord: pairwise distance-doubling. Each sketch round, every node
///   sketches only the neighbours at distance 2^p (forward and backward)
///   where p cycles 0..floor(log2(N)). Ascending order ensures nearby
///   elements are absorbed before distant pairs reconcile, drastically
///   shrinking later sketch metadata.
///
/// The chord power counter is persisted in the carry under
/// `CHORD_POWER_KEY` and updated in `recv_phase` from the observed
/// sender distance.
pub struct MultiReplicaProtocol;

impl MultiReplicaProtocol {
    pub fn new() -> Self {
        Self
    }

    /// FPR per topology. Star converges in ~few rounds, so a low FPR keeps
    /// it from doubling its round count. Chord/Tree run many rounds anyway,
    /// so a higher FPR shrinks each BF more than it adds rounds.
    fn fpr(kind: TopologyKind) -> f64 {
        match kind {
            TopologyKind::Star => 0.01,
            TopologyKind::Chord => 0.05,
            TopologyKind::Tree => 0.1,
        }
    }

    fn build_bf(
        &self,
        set: &std::collections::HashSet<Element>,
        kind: TopologyKind,
    ) -> BloomFilter<u64> {
        let n = set.len().max(1);
        let mut bf: BloomFilter<u64> = BloomFilter::new(n, Self::fpr(kind));
        for e in set.iter() {
            bf.insert(&e.digest);
        }
        bf
    }

    fn read_chord_power(carry: &PendingElements) -> usize {
        carry
            .get(&CHORD_POWER_KEY)
            .and_then(|v| v.first())
            .map(|e| e.digest as usize)
            .unwrap_or(0)
    }

    fn write_chord_power(carry: &mut PendingElements, power: usize) {
        carry.insert(
            CHORD_POWER_KEY,
            vec![Element {
                digest: power as u64,
                payload: Vec::new(),
            }],
        );
    }

    fn chord_powers(node_count: usize) -> usize {
        if node_count <= 1 {
            return 1;
        }
        ((node_count as f64).log2().floor() as usize).max(1)
    }

    fn chord_distance_power(node_count: usize, local: usize, other: usize) -> Option<usize> {
        if node_count <= 1 {
            return None;
        }
        let fwd = (other + node_count - local) % node_count;
        let bwd = (local + node_count - other) % node_count;
        let dist = fwd.min(bwd);
        if dist > 0 && dist.is_power_of_two() {
            Some(dist.trailing_zeros() as usize)
        } else {
            None
        }
    }
}

impl Protocol for MultiReplicaProtocol {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::MultiReplica
    }

    fn send_phase(
        &self,
        local: ReplicaView<'_>,
        topology: &Topology,
        outbox: &mut Outbox<'_>,
        carry: Option<PendingElements>,
    ) {
        let pending = carry.unwrap_or_default();
        let chord_power = Self::read_chord_power(&pending);
        let has_real_pending = pending
            .iter()
            .any(|(k, v)| *k != CHORD_POWER_KEY && !v.is_empty());

        if has_real_pending {
            for (nbr, elements) in pending {
                if nbr == CHORD_POWER_KEY {
                    continue;
                }
                if !elements.is_empty() {
                    outbox.send_elements(nbr, elements);
                }
            }
            return;
        }

        match topology.kind {
            TopologyKind::Chord => {
                let total = topology.node_count();
                if total <= 1 {
                    return;
                }
                let l = Self::chord_powers(total);
                let p = chord_power % l;
                let dist = 1usize << p;
                let fwd = (local.id + dist) % total;
                let bwd = (local.id + total - dist) % total;

                outbox.send_bloom(fwd, self.build_bf(local.set, topology.kind));
                if bwd != fwd && bwd != local.id {
                    outbox.send_bloom(bwd, self.build_bf(local.set, topology.kind));
                }
            }
            _ => {
                for &nbr in topology.neighbors(local.id) {
                    outbox.send_bloom(nbr, self.build_bf(local.set, topology.kind));
                }
            }
        }
    }

    fn recv_phase(
        &self,
        local: ReplicaView<'_>,
        topology: &Topology,
        inbox: &mut [(usize, ProtocolMsg)],
    ) -> ProtocolStepResult {
        let mut next_set = local.snapshot_set();
        let mut pending: PendingElements = HashMap::new();
        let mut saw_sketch = false;
        let mut saw_elements = false;
        let mut observed_power: Option<usize> = None;
        let total = topology.node_count();
        let is_chord = topology.kind == TopologyKind::Chord;

        for (from, msg) in inbox.iter_mut() {
            if let Some(els) = msg.take_elements() {
                for e in els {
                    next_set.insert(e);
                }
                saw_elements = true;
                if is_chord {
                    if let Some(p) = Self::chord_distance_power(total, local.id, *from) {
                        observed_power = Some(p);
                    }
                }
                continue;
            }

            if let Some(bf) = msg.as_bloom() {
                saw_sketch = true;
                let to_send: Vec<Element> = local
                    .set
                    .iter()
                    .filter(|e| !bf.contains(&e.digest))
                    .cloned()
                    .collect();
                if !to_send.is_empty() {
                    pending.entry(*from).or_default().extend(to_send);
                }
                if is_chord {
                    if let Some(p) = Self::chord_distance_power(total, local.id, *from) {
                        observed_power = Some(p);
                    }
                }
            }
        }

        if is_chord && total > 1 {
            let l = Self::chord_powers(total);
            let next_power = match (observed_power, saw_sketch, saw_elements) {
                (Some(p), true, false) => p,            // sketch received → deliver next at same p
                (Some(p), false, true) => (p + 1) % l,  // delivery received → advance to next p
                (Some(p), _, _) => (p + 1) % l,         // mixed (rare) → advance to be safe
                (None, _, _) => 0,                       // empty inbox → restart
            };
            Self::write_chord_power(&mut pending, next_power);
        }

        ProtocolStepResult {
            next_set,
            metrics: LocalMetrics::default(),
            carry: if pending.is_empty() { None } else { Some(pending) },
        }
    }
}
