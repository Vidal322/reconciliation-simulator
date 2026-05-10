use std::collections::HashMap;

use crate::simulator::algorithms::bloom::BloomFilter;
use crate::simulator::network::{ProtocolMsg, Outbox};
use crate::simulator::protocols::{
    LocalMetrics, PendingElements, Protocol, ProtocolKind, ProtocolStepResult,
};
use crate::simulator::replica::{Element, ReplicaView};
use crate::simulator::topology::{Topology, TopologyKind};

/// Sentinel keys in the carry map. `usize::MAX` and `usize::MAX-1` are never
/// valid neighbour ids, so these slots are invisible to the delivery loop.
const CHORD_POWER_KEY: usize = usize::MAX;
const TREE_IDX_KEY: usize = usize::MAX - 1;

fn is_sentinel_key(k: usize) -> bool {
    k == CHORD_POWER_KEY || k == TREE_IDX_KEY
}

/// Markers are zero-payload Elements (workload always uses `payload_size > 0`,
/// so empty payload uniquely identifies a protocol-internal marker). Digest
/// encodes (idx, phase): bits[1..] = idx, bit[0] = phase (0=sketch, 1=deliver).
fn make_tree_marker(idx: u64, phase: u8) -> Element {
    Element {
        digest: (idx << 1) | (phase as u64 & 1),
        payload: Vec::new(),
    }
}

fn parse_tree_marker(e: &Element) -> Option<(u64, u8)> {
    if e.payload.is_empty() {
        Some((e.digest >> 1, (e.digest & 1) as u8))
    } else {
        None
    }
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
    fn fpr(kind: TopologyKind, set_len: usize) -> f64 {
        match kind {
            TopologyKind::Star => 0.01,
            TopologyKind::Chord => 0.1,
            TopologyKind::Tree => 0.10,
        }
    }

    fn build_bf(
        &self,
        set: &std::collections::HashSet<Element>,
        kind: TopologyKind,
    ) -> BloomFilter<u64> {
        let n = set.len().max(1);
        let mut bf: BloomFilter<u64> = BloomFilter::new(n, Self::fpr(kind, set.len()));
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

    fn read_tree_idx(carry: &PendingElements) -> u64 {
        carry
            .get(&TREE_IDX_KEY)
            .and_then(|v| v.first())
            .map(|e| e.digest)
            .unwrap_or(0)
    }

    fn write_tree_idx(carry: &mut PendingElements, idx: u64) {
        carry.insert(
            TREE_IDX_KEY,
            vec![Element {
                digest: idx,
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
        let tree_idx = Self::read_tree_idx(&pending);
        let has_real_pending = pending
            .iter()
            .any(|(k, v)| !is_sentinel_key(*k) && !v.is_empty());

        if has_real_pending {
            let is_tree = topology.kind == TopologyKind::Tree;
            for (nbr, elements) in pending {
                if is_sentinel_key(nbr) {
                    continue;
                }
                if elements.is_empty() {
                    continue;
                }
                if is_tree {
                    let mut elements = elements;
                    elements.push(make_tree_marker(tree_idx, 1));
                    outbox.send_elements(nbr, elements);
                } else {
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
            TopologyKind::Tree => {
                let neighbours = topology.neighbors(local.id);
                if neighbours.is_empty() {
                    return;
                }
                let deg = neighbours.len();
                // Skip-one-neighbour: sketch all neighbours EXCEPT
                // neighbours[idx % deg]. Provides partial multi-source dedup
                // (each pair sketched (deg-1)/deg of rounds, vs always with
                // all-neighbour) while keeping propagation fast.
                let skip = tree_idx as usize % deg;
                for (i, &nbr) in neighbours.iter().enumerate() {
                    if deg > 1 && i == skip {
                        // Marker only: keep neighbour in lockstep on idx/phase.
                        outbox.send_elements(nbr, vec![make_tree_marker(tree_idx, 0)]);
                    } else {
                        outbox.send_bloom(nbr, self.build_bf(local.set, topology.kind));
                        outbox.send_elements(nbr, vec![make_tree_marker(tree_idx, 0)]);
                    }
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
        let mut observed_tree_marker: Option<(u64, u8)> = None;
        let total = topology.node_count();
        let is_chord = topology.kind == TopologyKind::Chord;
        let is_tree = topology.kind == TopologyKind::Tree;

        for (from, msg) in inbox.iter_mut() {
            if let Some(els) = msg.take_elements() {
                let mut got_data = false;
                for e in els {
                    if is_tree {
                        if let Some((idx, phase)) = parse_tree_marker(&e) {
                            observed_tree_marker = Some((idx, phase));
                            continue;
                        }
                    }
                    next_set.insert(e);
                    got_data = true;
                }
                if got_data {
                    saw_elements = true;
                }
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

        if is_tree {
            let next_idx = match observed_tree_marker {
                Some((idx, 0)) => idx,           // sketch round just ended → deliver next at same idx
                Some((idx, _)) => idx + 1,       // deliver round just ended → advance idx for next sketch
                None => 0,                        // empty inbox → restart
            };
            Self::write_tree_idx(&mut pending, next_idx);
        }

        ProtocolStepResult {
            next_set,
            metrics: LocalMetrics::default(),
            carry: if pending.is_empty() { None } else { Some(pending) },
        }
    }
}
