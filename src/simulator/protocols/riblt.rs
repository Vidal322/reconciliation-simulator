use std::collections::HashSet;
use std::time::Duration;

use crate::simulator::algorithms::riblt::RatelessIBLT;
use crate::simulator::protocols::{Protocol, ProtocolKind, ProtocolMetrics, ProtocolStepResult};
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

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

        let mut metadata_bytes = 0usize;
        let mut encode_time = Duration::ZERO;
        let mut decode_time = Duration::ZERO;

        for &neighbor_id in topology.neighbors(replica_id) {
            let remote_digests = replicas[neighbor_id]
                .set
                .iter()
                .map(|e| e.digest)
                .collect::<Vec<_>>();

            let mut local_riblt = RatelessIBLT::riblt_from(local_digests.clone());
            let mut remote_riblt = RatelessIBLT::riblt_from(remote_digests);

            let sketch_len = local_riblt.find_all_differences(&mut remote_riblt);

            metadata_bytes += sketch_len * std::mem::size_of::<u64>();
            encode_time += local_riblt.t_enc();
            decode_time += local_riblt.t_dec();

            let remote_only = local_riblt.get_remote_only_symbols();

            for digest in remote_only {
                if let Some(element) = replicas[neighbor_id]
                    .set
                    .iter()
                    .find(|e| e.digest == digest)
                    .cloned()
                {
                    next_set.insert(element);
                }
            }
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use crate::simulator::protocols::test_helpers::{make_element, make_replica};

    #[test]
    fn riblt_step_replica_learns_missing_neighbor_elements() {
        let protocol = RibltProtocol::new();
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
    }

    #[test]
    fn riblt_step_replica_keeps_existing_local_elements() {
        let protocol = RibltProtocol::new();
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
    fn riblt_step_replica_with_identical_neighbors_is_unchanged() {
        let protocol = RibltProtocol::new();
        let topology = Topology::star(2);

        let replicas = vec![make_replica(0, &[5, 6, 7]), make_replica(1, &[5, 6, 7])];

        let before = replicas[0].snapshot_set();
        let after = protocol.step_replica(0, &replicas, &topology).next_set;

        assert_eq!(before, after);
    }
}
