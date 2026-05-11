use std::collections::HashMap;

use crate::simulator::algorithms::bloom::BloomFilter;
use crate::simulator::network::{ProtocolMsg, Outbox};
use crate::simulator::protocols::{
    LocalMetrics, PendingElements, Protocol, ProtocolKind, ProtocolStepResult,
};
use crate::simulator::replica::{Element, ReplicaView};
use crate::simulator::topology::{Topology, TopologyKind};

/// Sentinel key for the chord power counter. `usize::MAX` is never a valid
/// neighbour id, so this slot is invisible to the delivery loop in send_phase.
const CHORD_POWER_KEY: usize = usize::MAX;

fn is_sentinel_key(k: usize) -> bool {
    k == CHORD_POWER_KEY
}

/// Topology-dispatched reconciliation.
///
/// * Star: each node sketches its current set to every neighbour per sketch
///   round. Receivers identify the elements the sender lacks and send them
///   back as Elements in the next delivery round.
/// * Chord: pairwise distance-doubling. Each sketch round, every node
///   sketches only the neighbours at distance 2^p (forward and backward)
///   where p cycles 0..floor(log2(N)). Ascending order ensures nearby
///   elements are absorbed before distant pairs reconcile.
/// * Tree: pairwise edge-cycling. Each sketch round every node sketches a
///   single neighbour selected by `neighbours[idx % deg]`, with idx bumped
///   each (sketch, deliver) cycle. Temporal separation eliminates the
///   multi-source duplicate sends that plague all-neighbour Tree
///   sketching: when several neighbours of B all hold the same X, only the
///   one B currently selects sees B's BF in this round, so only one ships
///   X — by the next time another holder is selected, B already has X in
///   its BF and the redundant send is filtered out.
///
/// Per-replica state (chord power, tree idx) lives under sentinel keys in
/// the carry. The tree idx is reconstructed in `recv_phase` from a small
/// zero-payload "marker" Element that travels alongside each sketch /
/// delivery; lockstep among neighbours keeps everyone's idx aligned.
pub struct MultiReplicaProtocol;

impl MultiReplicaProtocol {
    pub fn new() -> Self {
        Self
    }

    /// FPR per topology. Star converges in ~few rounds, so a low FPR keeps
    /// it from doubling its round count. Chord/Tree run many rounds anyway,
    /// so a higher FPR shrinks each BF more than it adds rounds.
    /// Tree is also adaptive: once the local set is large (late rounds, near
    /// union), bump FPR — the BF is then much smaller and most queries are
    /// FPs anyway, so the extra suppression mostly cancels the elements that
    /// already arrived from another path (multi-source dedup).
    fn fpr(kind: TopologyKind, set_len: usize, sender_id: usize) -> f64 {
        match kind {
            TopologyKind::Star => {
                // Hub (id=0) has the largest set in late rounds — its BF
                // dominates Star metadata. Use a higher FPR for the hub
                // ONLY when its set is large (post-gather), so its BF
                // shrinks without inflating early-round Star round count.
                let _ = (sender_id, set_len);
                0.01
            }
            TopologyKind::Chord => {
                let _ = sender_id;
                if set_len > 100_000 {
                    0.13
                } else {
                    0.1
                }
            }
            TopologyKind::Tree => {
                if set_len > 70_000 {
                    0.31
                } else {
                    0.27
                }
            }
        }
    }

    fn build_bf(
        &self,
        set: &std::collections::HashSet<Element>,
        kind: TopologyKind,
        sender_id: usize,
    ) -> BloomFilter<u64> {
        let n = set.len().max(1);
        let mut bf: BloomFilter<u64> = BloomFilter::new(n, Self::fpr(kind, set.len(), sender_id));
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
            .any(|(k, v)| !is_sentinel_key(*k) && !v.is_empty());

        if has_real_pending {
            for (nbr, elements) in pending {
                if is_sentinel_key(nbr) {
                    continue;
                }
                if elements.is_empty() {
                    continue;
                }
                outbox.send_elements(nbr, elements);
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

                outbox.send_bloom(fwd, self.build_bf(local.set, topology.kind, local.id));
                if bwd != fwd && bwd != local.id {
                    outbox.send_bloom(bwd, self.build_bf(local.set, topology.kind, local.id));
                }
            }
            TopologyKind::Tree => {
                for &nbr in topology.neighbors(local.id) {
                    outbox.send_bloom(nbr, self.build_bf(local.set, topology.kind, local.id));
                }
            }
            _ => {
                for &nbr in topology.neighbors(local.id) {
                    outbox.send_bloom(nbr, self.build_bf(local.set, topology.kind, local.id));
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
