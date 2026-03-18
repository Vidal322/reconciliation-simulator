use std::collections::HashSet;
use std::mem;
use std::time::Duration;

use crate::simulator::algorithms::bloom::BloomFilter;
use crate::simulator::algorithms::riblt::RatelessIBLT;
use crate::simulator::protocols::{Protocol, ProtocolKind, ProtocolMetrics, ProtocolStepResult};
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

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

    fn bloom_metadata_size(bloom: &BloomFilter<u64>) -> usize {
        bloom.byte_len() + mem::size_of::<usize>() + mem::size_of::<u64>()
    }
}

impl Protocol for StaticBfIbltProtocol {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::StaticBfIblt
    }

    fn step_replica(
        &self,
        replica_id: usize,
        replicas: &[Replica],
        topology: &Topology,
    ) -> ProtocolStepResult {
        let mut next_set = replicas[replica_id].snapshot_set();
        let local_digests = replicas[replica_id]
            .set
            .iter()
            .map(|e| e.digest)
            .collect::<Vec<_>>();

        let mut state_bytes = 0usize;
        let mut metadata_bytes = 0usize;
        let mut encode_time = Duration::ZERO;
        let mut decode_time = Duration::ZERO;
        let mut false_matches = 0usize;

        for &neighbor_id in topology.neighbors(replica_id) {
            let remote_elements = &replicas[neighbor_id].set;
            let remote_digests = remote_elements.iter().map(|e| e.digest).collect::<Vec<_>>();

            let mut bloom = BloomFilter::new(local_digests.len().max(1), self.false_positive_rate);
            for digest in &local_digests {
                bloom.timed_insert(digest);
            }

            metadata_bytes += Self::bloom_metadata_size(&bloom);
            encode_time += bloom.t_enc();

            let mut confirmed_remote_only = Vec::new();
            let mut candidate_remote_positive = Vec::new();

            for digest in remote_digests {
                if bloom.timed_contains(&digest) {
                    candidate_remote_positive.push(digest);
                } else {
                    confirmed_remote_only.push(digest);
                }
            }

            decode_time += bloom.t_dec();

            let mut recovered_remote_only = confirmed_remote_only;

            if !candidate_remote_positive.is_empty() {
                let mut local_riblt = RatelessIBLT::riblt_from(local_digests.clone());
                let mut remote_riblt = RatelessIBLT::riblt_from(candidate_remote_positive);

                let sketch_len = local_riblt.find_all_differences(&mut remote_riblt);

                metadata_bytes += sketch_len * mem::size_of::<u64>();
                encode_time += local_riblt.t_enc();
                decode_time += local_riblt.t_dec();

                let riblt_remote_only = local_riblt.get_remote_only_symbols();
                false_matches += riblt_remote_only.len();
                recovered_remote_only.extend(riblt_remote_only);
            }

            for digest in recovered_remote_only {
                if let Some(element) = remote_elements.iter().find(|e| e.digest == digest).cloned()
                {
                    if next_set.insert(element.clone()) {
                        state_bytes += element.wire_size();
                    }
                }
            }
        }

        ProtocolStepResult {
            next_set,
            metrics: ProtocolMetrics {
                state_bytes,
                metadata_bytes,
                encode_time,
                decode_time,
                false_matches,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulator::protocols::test_helpers::{make_element, make_replica};

    #[test]
    fn bf_iblt_step_replica_learns_missing_neighbor_elements() {
        let protocol = StaticBfIbltProtocol::new();
        let topology = Topology::star(2);

        let replicas = vec![make_replica(0, &[1, 2, 3]), make_replica(1, &[2, 3, 4])];

        let result = protocol.step_replica(0, &replicas, &topology);
        let digests = result
            .next_set
            .iter()
            .map(|e| e.digest)
            .collect::<HashSet<_>>();

        assert!(digests.contains(&1));
        assert!(digests.contains(&2));
        assert!(digests.contains(&3));
        assert!(digests.contains(&4));
        assert_eq!(digests.len(), 4);
        assert!(result.metrics.metadata_bytes > 0);
        assert!(result.metrics.state_bytes > 0);
    }

    #[test]
    fn bf_iblt_step_replica_keeps_existing_local_elements() {
        let protocol = StaticBfIbltProtocol::new();
        let topology = Topology::star(2);

        let replicas = vec![make_replica(0, &[10, 20]), make_replica(1, &[20, 30])];

        let result = protocol.step_replica(0, &replicas, &topology);
        let digests = result
            .next_set
            .iter()
            .map(|e| e.digest)
            .collect::<HashSet<_>>();

        assert!(digests.contains(&10));
        assert!(digests.contains(&20));
        assert!(digests.contains(&30));
    }

    #[test]
    fn bf_iblt_step_replica_with_identical_neighbors_is_unchanged() {
        let protocol = StaticBfIbltProtocol::new();
        let topology = Topology::star(2);

        let replicas = vec![make_replica(0, &[5, 6, 7]), make_replica(1, &[5, 6, 7])];

        let before = replicas[0].snapshot_set();
        let after = protocol.step_replica(0, &replicas, &topology).next_set;

        assert_eq!(before, after);
    }
}
