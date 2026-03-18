use std::collections::HashSet;
use std::time::{Duration, Instant};

use crate::simulator::algorithms::multiparty_sketch::{build_sketch, MSketch, CELL_BYTES, P};
use crate::simulator::protocols::{Protocol, ProtocolKind, ProtocolMetrics, ProtocolStepResult};
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

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
    use crate::simulator::protocols::test_helpers::{make_element, make_replica};

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
