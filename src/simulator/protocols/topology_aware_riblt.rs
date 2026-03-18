use std::collections::HashSet;
use std::mem;
use std::time::{Duration, Instant};

use std::hash::{DefaultHasher, Hash, Hasher};

use crate::simulator::algorithms::riblt::SymbolMapping;
use crate::simulator::protocols::{Protocol, ProtocolKind, ProtocolMetrics, ProtocolStepResult};
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

// ---------------------------------------------------------------------------
// Finite prime field GF(P)
//
// P is the largest 64-bit prime.  Using a prime field instead of XOR means
// that k copies of the same element contribute k·e to the symbol accumulator
// rather than e⊕e⊕…, which cancels to 0 for even k.  With field arithmetic
// we can always recover e = sym / k via modular inversion.
// ---------------------------------------------------------------------------
pub(super) const P: u64 = 18_446_744_073_709_551_557;

#[inline]
pub(super) fn fadd(a: u64, b: u64) -> u64 {
    let (s, ov) = a.overflowing_add(b);
    if ov || s >= P {
        s.wrapping_sub(P)
    } else {
        s
    }
}

#[inline]
pub(super) fn fsub(a: u64, b: u64) -> u64 {
    if a >= b {
        a - b
    } else {
        P - (b - a)
    }
}

#[inline]
pub(super) fn fmul(a: u64, b: u64) -> u64 {
    ((a as u128 * b as u128) % P as u128) as u64
}

pub(super) fn fpow(mut base: u64, mut exp: u64) -> u64 {
    let mut result = 1u64;
    while exp > 0 {
        if exp & 1 == 1 {
            result = fmul(result, base);
        }
        exp >>= 1;
        base = fmul(base, base);
    }
    result
}

/// Modular inverse via Fermat's little theorem (P is prime).
#[inline]
pub(super) fn finv(a: u64) -> u64 {
    fpow(a, P - 2)
}

#[inline]
pub(super) fn fdiv(a: u64, b: u64) -> u64 {
    fmul(a, finv(b))
}

/// Convert a signed count to its representative in GF(P).
#[inline]
pub(super) fn count_to_field(count: i64) -> u64 {
    if count >= 0 {
        (count as u64) % P
    } else {
        P - ((-count) as u64 % P)
    }
}

/// Deterministic hash of a digest, used as the mapping seed and for cell
/// verification.  Must be the same function used during encoding and peeling.
#[inline]
pub(super) fn digest_hash(d: u64) -> u64 {
    let mut h = DefaultHasher::new();
    d.hash(&mut h);
    h.finish()
}

// Serialised size of one coded symbol: sym (8 B) + hash (8 B) + count (8 B).
pub(super) const CELL_BYTES: usize = 3 * mem::size_of::<u64>();

// ---------------------------------------------------------------------------
// Multiparty coded symbol
// ---------------------------------------------------------------------------

/// A single coded symbol cell that accumulates element contributions using
/// field arithmetic rather than XOR.
#[derive(Clone, Default)]
pub(super) struct MCell {
    /// Field sum of (digest % P) for each element inserted (+) or removed (−).
    pub(super) sym: u64,
    /// Field sum of (digest_hash(digest) % P) for each element.
    pub(super) hash: u64,
    /// Signed count: +1 per insertion, −1 per removal.
    pub(super) count: i64,
}

impl MCell {
    /// A cell is pure when it encodes exactly |count| copies of a single
    /// element.  We verify by recovering the candidate element via field
    /// division and checking consistency with the hash accumulator.
    pub(super) fn is_pure(&self) -> bool {
        if self.count == 0 {
            return false;
        }
        let kf = count_to_field(self.count);
        let e_cand = fdiv(self.sym, kf);
        let h_cand = digest_hash(e_cand) % P;
        fmul(h_cand, kf) == self.hash
    }

    /// Recover the field-reduced digest from a pure cell.
    pub(super) fn recover_digest_f(&self) -> u64 {
        fdiv(self.sym, count_to_field(self.count))
    }
}

// ---------------------------------------------------------------------------
// Multiparty sketch
// ---------------------------------------------------------------------------

pub(super) struct MSketch {
    pub(super) cells: Vec<MCell>,
}

impl MSketch {
    pub(super) fn new(size: usize) -> Self {
        Self {
            cells: vec![MCell::default(); size],
        }
    }

    /// Insert (+1) or remove (−1) one element from the sketch.
    pub(super) fn encode_element(&mut self, digest: u64, sign: i64) {
        let e_f = digest % P;
        let h_f = digest_hash(digest) % P;
        let size = self.cells.len();
        let mut mapping = SymbolMapping::new(digest_hash(digest));
        loop {
            let idx = mapping.next().unwrap();
            if idx >= size {
                break;
            }
            if sign > 0 {
                self.cells[idx].sym = fadd(self.cells[idx].sym, e_f);
                self.cells[idx].hash = fadd(self.cells[idx].hash, h_f);
                self.cells[idx].count += 1;
            } else {
                self.cells[idx].sym = fsub(self.cells[idx].sym, e_f);
                self.cells[idx].hash = fsub(self.cells[idx].hash, h_f);
                self.cells[idx].count -= 1;
            }
        }
    }

    /// In-place field addition: self += other (aggregation step).
    pub(super) fn add_sketch(&mut self, other: &MSketch) {
        for (a, b) in self.cells.iter_mut().zip(&other.cells) {
            a.sym = fadd(a.sym, b.sym);
            a.hash = fadd(a.hash, b.hash);
            a.count += b.count;
        }
    }

    /// In-place field subtraction: self −= other (diff step).
    pub(super) fn sub_sketch(&mut self, other: &MSketch) {
        for (a, b) in self.cells.iter_mut().zip(&other.cells) {
            a.sym = fsub(a.sym, b.sym);
            a.hash = fsub(a.hash, b.hash);
            a.count -= b.count;
        }
    }

    /// Greedy peeling decoder.
    ///
    /// Returns the field-reduced digests of "remote-only" elements
    /// (those with count > 0 after diff) if the sketch fully decodes,
    /// or `None` if the sketch is too small.
    pub(super) fn try_decode(&self) -> Option<Vec<u64>> {
        let mut cells = self.cells.clone();
        let size = cells.len();
        let mut remote_only = Vec::new();
        let mut changed = true;

        while changed {
            changed = false;
            for i in 0..size {
                if cells[i].count == 0 || !cells[i].is_pure() {
                    continue;
                }

                let e_f = cells[i].recover_digest_f();
                if cells[i].count > 0 {
                    remote_only.push(e_f);
                }

                // Peel: subtract this element's total contribution from every
                // cell it maps to.  For a pure cell the deltas equal the cell
                // values themselves.
                let sym_d = cells[i].sym;
                let hash_d = cells[i].hash;
                let cnt_d = cells[i].count;

                // Use the field-reduced digest as the mapping seed; for
                // digests < P (essentially all u64 values) this equals the
                // original digest so the mapping matches encoding exactly.
                let mut mapping = SymbolMapping::new(digest_hash(e_f));
                loop {
                    let idx = mapping.next().unwrap();
                    if idx >= size {
                        break;
                    }
                    cells[idx].sym = fsub(cells[idx].sym, sym_d);
                    cells[idx].hash = fsub(cells[idx].hash, hash_d);
                    cells[idx].count -= cnt_d;
                }

                changed = true;
            }
        }

        if cells.iter().all(|c| c.count == 0) {
            Some(remote_only)
        } else {
            None
        }
    }
}

pub(super) fn build_sketch(replica: &Replica, size: usize) -> MSketch {
    let mut sketch = MSketch::new(size);
    for elem in &replica.set {
        sketch.encode_element(elem.digest, 1);
    }
    sketch
}

// ---------------------------------------------------------------------------
// Shared RIBLT loop — used by both TopologyAwareRiblt and TopologyAwareCbfRiblt
// ---------------------------------------------------------------------------

pub(super) struct RibltLoopResult {
    pub(super) final_sketch_size: usize,
    pub(super) encode_time: Duration,
    pub(super) decode_time: Duration,
    pub(super) remote_only_f: Vec<u64>,
}

/// Rateless RIBLT encode-aggregate-diff-decode loop.
///
/// Starts at `initial_sketch_size` and grows by `cells_per_increment` on each
/// failed decode, up to a safety cap of 200× the increment.
///
/// `initial_encode_time` is folded into the first encode measurement so that
/// callers with a pre-round (e.g. CBF) can include that cost in the total.
pub(super) fn run_riblt_loop(
    replicas: &[Replica],
    replica_id: usize,
    initial_sketch_size: usize,
    cells_per_increment: usize,
    initial_encode_time: Duration,
) -> RibltLoopResult {
    let max_sketch_size = cells_per_increment * 200;
    let mut sketch_size = initial_sketch_size;
    let mut final_sketch_size = sketch_size;

    let mut encode_time = initial_encode_time;
    let mut decode_time = Duration::ZERO;
    let mut remote_only_f: Vec<u64> = Vec::new();

    loop {
        let enc_start = Instant::now();

        let sketches: Vec<MSketch> = replicas
            .iter()
            .map(|r| build_sketch(r, sketch_size))
            .collect();

        let mut total = MSketch::new(sketch_size);
        for s in &sketches {
            total.add_sketch(s);
        }

        encode_time += enc_start.elapsed();

        let dec_start = Instant::now();

        let mut diff = MSketch::new(sketch_size);
        diff.add_sketch(&total);
        diff.sub_sketch(&sketches[replica_id]);

        match diff.try_decode() {
            Some(digests) => {
                decode_time += dec_start.elapsed();
                final_sketch_size = sketch_size;
                remote_only_f = digests;
                break;
            }
            None => {
                decode_time += dec_start.elapsed();
                sketch_size += cells_per_increment;
                if sketch_size > max_sketch_size {
                    final_sketch_size = sketch_size - cells_per_increment;
                    break;
                }
            }
        }
    }

    RibltLoopResult {
        final_sketch_size,
        encode_time,
        decode_time,
        remote_only_f,
    }
}

/// Insert into `next_set` every element whose field-reduced digest appears in
/// `remote_only_f`.  Elements are looked up across all replicas.
pub(super) fn recover_elements(
    remote_only_f: &[u64],
    replicas: &[Replica],
    next_set: &mut HashSet<Element>,
) {
    for d_f in remote_only_f {
        if let Some(element) = replicas
            .iter()
            .flat_map(|r| r.set.iter())
            .find(|e| e.digest % P == *d_f)
            .cloned()
        {
            next_set.insert(element);
        }
    }
}

// ---------------------------------------------------------------------------
// Protocol
// ---------------------------------------------------------------------------

/// Topology-aware RIBLT reconciliation.
///
/// Instead of pairwise exchanges, each replica broadcasts its sketch toward
/// a network aggregator.  The aggregator computes:
///
///   total = ∑ IBLT(Sⱼ)   for all j
///
/// and each replica i recovers its missing elements by decoding:
///
///   diff_i = total − IBLT(Sᵢ)
///
/// Finite-field arithmetic ensures that elements shared by an even number of
/// replicas are not cancelled by XOR, which is the standard parity problem
/// with bitwise XOR in multiparty settings.
///
/// The sketch is extended rateless-style until decoding succeeds, matching
/// the pairwise RIBLT's approach of transmitting incrementally.
#[derive(Clone, Debug)]
pub struct TopologyAwareRibltProtocol {
    cells_per_increment: usize,
}

impl Default for TopologyAwareRibltProtocol {
    fn default() -> Self {
        Self::new()
    }
}

impl TopologyAwareRibltProtocol {
    pub fn new() -> Self {
        Self {
            cells_per_increment: 1024,
        }
    }

    pub fn with_cells_per_increment(cells_per_increment: usize) -> Self {
        Self { cells_per_increment }
    }
}

impl Protocol for TopologyAwareRibltProtocol {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::TopologyAwareRiblt
    }

    /// Execute one protocol step for `replica_id`.
    ///
    /// In a real deployment this corresponds to a full aggregation round:
    ///   1. Each node encodes its set into a sketch and forwards it toward
    ///      the aggregator (one sketch per network edge, upward).
    ///   2. The aggregator sums all sketches and broadcasts the result back
    ///      (one sketch per network edge, downward).
    ///   3. Each node decodes `total − own` to learn its missing elements.
    ///
    /// Because the simulator has a global view, steps 1–3 are executed
    /// directly without routing.  Byte accounting reflects the number of
    /// sketch exchanges implied by the topology.
    fn step_replica(
        &self,
        replica_id: usize,
        replicas: &[Replica],
        topology: &Topology,
    ) -> ProtocolStepResult {
        let mut next_set = replicas[replica_id].snapshot_set();

        let RibltLoopResult {
            final_sketch_size,
            encode_time,
            decode_time,
            remote_only_f,
        } = run_riblt_loop(
            replicas,
            replica_id,
            self.cells_per_increment,
            self.cells_per_increment,
            Duration::ZERO,
        );

        // state_bytes = 0: elements are encoded inside the sketch (metadata),
        // not transmitted as separate state, consistent with pairwise Riblt.
        recover_elements(&remote_only_f, replicas, &mut next_set);

        // Bytes model: each node exchanges one sketch per adjacent edge in both
        // directions (upload toward aggregator + download of total sketch).
        // `metadata_bytes` is recorded by the engine as both sent and received,
        // so supplying num_neighbors × sketch_bytes correctly captures one
        // sketch per direction per edge.
        let num_neighbors = topology.neighbors(replica_id).len();
        let metadata_bytes = num_neighbors * final_sketch_size * CELL_BYTES;

        ProtocolStepResult {
            next_set,
            metrics: ProtocolMetrics {
                state_bytes: 0,
                metadata_bytes,
                encode_time,
                decode_time,
                false_matches: 0,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn make_element(digest: u64, payload_byte: u8) -> Element {
        Element::new(digest, vec![payload_byte; 4])
    }

    fn make_replica(id: usize, digests: &[u64]) -> Replica {
        let set = digests
            .iter()
            .map(|&d| make_element(d, d as u8))
            .collect::<HashSet<_>>();
        Replica::new(id, set)
    }

    #[test]
    fn topology_aware_star_converges_in_one_step() {
        let protocol = TopologyAwareRibltProtocol::new();
        let topology = Topology::star(4);

        let replicas = vec![
            make_replica(0, &[1, 2, 3]),
            make_replica(1, &[2, 3, 4]),
            make_replica(2, &[3, 4, 5]),
            make_replica(3, &[4, 5, 6]),
        ];

        for replica_id in 0..4 {
            let result = protocol.step_replica(replica_id, &replicas, &topology);
            let digests: HashSet<u64> = result.next_set.iter().map(|e| e.digest).collect();
            for d in 1u64..=6 {
                assert!(digests.contains(&d), "replica {replica_id} missing {d}");
            }
            assert_eq!(digests.len(), 6);
        }
    }

    #[test]
    fn topology_aware_tree_converges_in_one_step() {
        let protocol = TopologyAwareRibltProtocol::new();
        let topology = Topology::tree(4);

        let replicas = vec![
            make_replica(0, &[1, 2]),
            make_replica(1, &[2, 3]),
            make_replica(2, &[3, 4]),
            make_replica(3, &[4, 5]),
        ];

        for replica_id in 0..4 {
            let result = protocol.step_replica(replica_id, &replicas, &topology);
            let digests: HashSet<u64> = result.next_set.iter().map(|e| e.digest).collect();
            for d in 1u64..=5 {
                assert!(digests.contains(&d), "replica {replica_id} missing {d}");
            }
        }
    }

    #[test]
    fn topology_aware_chord_converges_in_one_step() {
        let protocol = TopologyAwareRibltProtocol::new();
        let topology = Topology::chord(4);

        let replicas = vec![
            make_replica(0, &[10, 20]),
            make_replica(1, &[20, 30]),
            make_replica(2, &[30, 40]),
            make_replica(3, &[40, 10]),
        ];

        for replica_id in 0..4 {
            let result = protocol.step_replica(replica_id, &replicas, &topology);
            let digests: HashSet<u64> = result.next_set.iter().map(|e| e.digest).collect();
            for d in [10u64, 20, 30, 40] {
                assert!(digests.contains(&d), "replica {replica_id} missing {d}");
            }
        }
    }

    #[test]
    fn topology_aware_identical_replicas_unchanged() {
        let protocol = TopologyAwareRibltProtocol::new();
        let topology = Topology::star(2);

        let replicas = vec![
            make_replica(0, &[1, 2, 3]),
            make_replica(1, &[1, 2, 3]),
        ];

        let before = replicas[0].snapshot_set();
        let after = protocol.step_replica(0, &replicas, &topology).next_set;
        assert_eq!(before, after);
    }

    #[test]
    fn topology_aware_keeps_existing_local_elements() {
        let protocol = TopologyAwareRibltProtocol::new();
        let topology = Topology::star(3);

        let replicas = vec![
            make_replica(0, &[1, 2]),
            make_replica(1, &[2, 3]),
            make_replica(2, &[3, 4]),
        ];

        let result = protocol.step_replica(0, &replicas, &topology);
        let digests: HashSet<u64> = result.next_set.iter().map(|e| e.digest).collect();
        assert!(digests.contains(&1));
        assert!(digests.contains(&2));
        assert!(digests.contains(&3));
        assert!(digests.contains(&4));
    }

    #[test]
    fn topology_aware_reports_metadata_bytes() {
        let protocol = TopologyAwareRibltProtocol::new();
        let topology = Topology::star(3);

        let replicas = vec![
            make_replica(0, &[1, 2, 3]),
            make_replica(1, &[2, 3, 4]),
            make_replica(2, &[3, 4, 5]),
        ];

        let result = protocol.step_replica(0, &replicas, &topology);
        assert!(result.metrics.metadata_bytes > 0);
    }
}
