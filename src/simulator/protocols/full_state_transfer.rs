use std::time::Duration;

use crate::simulator::protocols::{Protocol, ProtocolKind, ProtocolMetrics, ProtocolStepResult};
use crate::simulator::replica::Replica;
use crate::simulator::topology::Topology;

#[derive(Clone, Debug, Default)]
pub struct FullStateTransfer;

impl FullStateTransfer {
    pub fn new() -> Self {
        Self
    }
}

impl Protocol for FullStateTransfer {
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::FullStateTransfer
    }

    fn step_replica(
        &self,
        replica_id: usize,
        replicas: &[Replica],
        topology: &Topology,
    ) -> ProtocolStepResult {
        let mut next_set = replicas[replica_id].snapshot_set();
        let mut state_bytes = 0usize;

        for &neighbor_id in topology.neighbors(replica_id) {
            // Charge for the full neighbor set — full state transfer sends every
            // element regardless of whether the receiver already has it.
            for element in &replicas[neighbor_id].set {
                state_bytes += std::mem::size_of::<u64>() + element.payload.len();
                next_set.insert(element.clone());
            }
        }

        ProtocolStepResult {
            next_set,
            metrics: ProtocolMetrics {
                state_bytes,
                metadata_bytes: 0,
                encode_time: Duration::ZERO,
                decode_time: Duration::ZERO,
                false_matches: 0,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulator::replica::{Element, Replica};
    use crate::simulator::topology::Topology;
    use std::collections::HashSet;

    fn make_element(digest: u64, payload_byte: u8, payload_len: usize) -> Element {
        Element::new(digest, vec![payload_byte; payload_len])
    }

    fn make_replica(id: usize, elements: Vec<Element>) -> Replica {
        let set = elements.into_iter().collect::<HashSet<_>>();
        Replica::new(id, set)
    }

    #[test]
    fn full_state_transfer_learns_missing_neighbor_elements() {
        let protocol = FullStateTransfer::new();
        let topology = Topology::star(2);

        let replicas = vec![
            make_replica(
                0,
                vec![
                    make_element(1, 1, 4),
                    make_element(2, 2, 4),
                    make_element(3, 3, 4),
                ],
            ),
            make_replica(
                1,
                vec![
                    make_element(2, 2, 4),
                    make_element(3, 3, 4),
                    make_element(4, 4, 4),
                ],
            ),
        ];

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
    }

    #[test]
    fn full_state_transfer_counts_state_bytes_for_new_elements() {
        let protocol = FullStateTransfer::new();
        let topology = Topology::star(2);

        let replicas = vec![
            make_replica(0, vec![make_element(1, 1, 4)]),
            make_replica(
                1,
                vec![
                    make_element(1, 1, 4),
                    make_element(2, 2, 10),
                    make_element(3, 3, 6),
                ],
            ),
        ];

        let result = protocol.step_replica(0, &replicas, &topology);

        // All 3 elements in neighbor's set are transmitted, not just the 2 new ones.
        let expected = (std::mem::size_of::<u64>() + 4)
            + (std::mem::size_of::<u64>() + 10)
            + (std::mem::size_of::<u64>() + 6);

        assert_eq!(result.metrics.state_bytes, expected);
        assert_eq!(result.metrics.metadata_bytes, 0);
    }

    #[test]
    fn full_state_transfer_with_identical_neighbors_is_unchanged() {
        let protocol = FullStateTransfer::new();
        let topology = Topology::star(2);

        let shared = vec![
            make_element(5, 5, 4),
            make_element(6, 6, 4),
            make_element(7, 7, 4),
        ];

        let replicas = vec![make_replica(0, shared.clone()), make_replica(1, shared)];

        let before = replicas[0].snapshot_set();
        let after = protocol.step_replica(0, &replicas, &topology).next_set;

        assert_eq!(before, after);
    }

    #[test]
    fn full_state_transfer_keeps_existing_local_elements() {
        let protocol = FullStateTransfer::new();
        let topology = Topology::star(2);

        let replicas = vec![
            make_replica(0, vec![make_element(10, 10, 4), make_element(20, 20, 4)]),
            make_replica(1, vec![make_element(20, 20, 4), make_element(30, 30, 4)]),
        ];

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
}
