use std::time::{Duration, Instant};

use crate::simulator::algorithms::counting_bloom::CountingBf;
use crate::simulator::protocols::{Protocol, ProtocolKind, ProtocolMetrics, ProtocolStepResult};
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

use crate::simulator::algorithms::multiparty_sketch::CELL_BYTES;
use super::topology_aware_riblt::{recover_elements, run_riblt_loop, RibltLoopResult};

// ---------------------------------------------------------------------------
// Protocol
// ---------------------------------------------------------------------------

/// Topology-aware RIBLT with a CBF pre-round to estimate the effective diff size.
///
/// Identical correctness to `TopologyAwareRiblt`.  Adds one CBF aggregation
/// round before the RIBLT loop so that the initial sketch size can be set
/// closer to the true effective diff size, reducing the number of rateless
/// growth iterations at the cost of one extra CBF communication round.
#[derive(Clone, Debug)]
pub struct TopologyAwareCbfRibltProtocol {
    cells_per_increment: usize,
    cbf_bits_per_elem: usize,
    cbf_num_hashes: usize,
}

impl Default for TopologyAwareCbfRibltProtocol {
    fn default() -> Self {
        Self::new()
    }
}

impl TopologyAwareCbfRibltProtocol {
    pub fn new() -> Self {
        Self {
            cells_per_increment: 1024,
            cbf_bits_per_elem: 8,
            cbf_num_hashes: 3,
        }
    }
}

fn round_up_to_multiple(value: usize, multiple: usize) -> usize {
    ((value + multiple - 1) / multiple) * multiple
}

impl Protocol for TopologyAwareCbfRibltProtocol {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::TopologyAwareCbfRiblt
    }

    fn step_replica(
        &self,
        replica_id: usize,
        replicas: &[Replica],
        topology: &Topology,
    ) -> ProtocolStepResult {
        let mut next_set = replicas[replica_id].snapshot_set();

        // ------------------------------------------------------------------
        // CBF pre-round: estimate effective diff size
        // ------------------------------------------------------------------
        let enc_start = Instant::now();

        let local_size = replicas[replica_id].set.len();
        let cbf_num_cells = local_size.max(64) * self.cbf_bits_per_elem / 32;
        let cbf_num_cells = cbf_num_cells.max(64);

        // Build one CBF per replica.
        let cbfs: Vec<CountingBf> = replicas
            .iter()
            .map(|r| {
                let mut cbf = CountingBf::new(cbf_num_cells, self.cbf_num_hashes);
                for elem in &r.set {
                    cbf.insert(elem.digest);
                }
                cbf
            })
            .collect();

        // Aggregate total CBF.
        let mut total_cbf = CountingBf::new(cbf_num_cells, self.cbf_num_hashes);
        for c in &cbfs {
            total_cbf.add(c);
        }

        // diff_cbf_i = total − cbf_i
        let mut diff_cbf = CountingBf::new(cbf_num_cells, self.cbf_num_hashes);
        diff_cbf.add(&total_cbf);
        diff_cbf.sub(&cbfs[replica_id]);

        // Count elements in Si that appear in the diff (shared-element noise).
        let mut shared_count = 0usize;
        for elem in &replicas[replica_id].set {
            if diff_cbf.query(elem.digest) >= 1 {
                shared_count += 1;
            }
        }

        let encode_time_cbf = enc_start.elapsed();

        // ------------------------------------------------------------------
        // Sizing: set initial sketch size based on CBF estimate
        // ------------------------------------------------------------------
        let effective_diff_estimate = shared_count + local_size;
        let raw_initial = (effective_diff_estimate * 3 + 1) / 2; // ceil(* 1.5)
        let initial_sketch_size = round_up_to_multiple(raw_initial, self.cells_per_increment)
            .max(self.cells_per_increment);

        // ------------------------------------------------------------------
        // RIBLT phase — CBF encode time folded in via initial_encode_time
        // ------------------------------------------------------------------
        let RibltLoopResult {
            final_sketch_size,
            encode_time,
            decode_time,
            remote_only_f,
        } = run_riblt_loop(
            replicas,
            replica_id,
            initial_sketch_size,
            self.cells_per_increment,
            encode_time_cbf,
        );

        // state_bytes = 0: elements are encoded inside the sketch (metadata),
        // not transmitted as separate state.
        recover_elements(&remote_only_f, replicas, &mut next_set);

        // Byte accounting: RIBLT sketch bytes + CBF bytes per neighbor edge.
        let num_neighbors = topology.neighbors(replica_id).len();
        let cbf_bytes = total_cbf.byte_len();
        let metadata_bytes = num_neighbors * (cbf_bytes + final_sketch_size * CELL_BYTES);

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
    fn star_converges_in_one_step() {
        let protocol = TopologyAwareCbfRibltProtocol::new();
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
    fn tree_converges_in_one_step() {
        let protocol = TopologyAwareCbfRibltProtocol::new();
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
    fn chord_converges_in_one_step() {
        let protocol = TopologyAwareCbfRibltProtocol::new();
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
    fn identical_replicas_unchanged() {
        let protocol = TopologyAwareCbfRibltProtocol::new();
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
    fn keeps_existing_local_elements() {
        let protocol = TopologyAwareCbfRibltProtocol::new();
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
    fn metadata_bytes_exceeds_pure_riblt() {
        let protocol = TopologyAwareCbfRibltProtocol::new();
        let topology = Topology::star(3);

        let replicas = vec![
            make_replica(0, &[1, 2, 3]),
            make_replica(1, &[2, 3, 4]),
            make_replica(2, &[3, 4, 5]),
        ];

        let result = protocol.step_replica(0, &replicas, &topology);
        let num_neighbors = topology.neighbors(0).len();
        // The minimum pure RIBLT cost would be num_neighbors * cells_per_increment * CELL_BYTES.
        // With CBF overhead, metadata_bytes must be strictly greater.
        let min_pure_riblt = num_neighbors * 1024 * CELL_BYTES;
        assert!(
            result.metrics.metadata_bytes > min_pure_riblt,
            "expected CBF overhead: metadata_bytes={} <= min_pure_riblt={}",
            result.metrics.metadata_bytes,
            min_pure_riblt
        );
    }
}
