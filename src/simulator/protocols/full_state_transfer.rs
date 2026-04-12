use crate::simulator::network::Network;
use crate::simulator::protocols::messages::ProtocolMsg;
use crate::simulator::protocols::{LocalMetrics, Protocol, ProtocolStepResult, ProtocolKind};
use crate::simulator::replica::{Element, Replica};
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

    fn send_phase(
        &mut self,
        replica_id: usize,
        local: &Replica,
        topology: &Topology,
        network: &mut Network<ProtocolMsg>,
    ) {
        let payload: Vec<Element> = local.set.iter().cloned().collect();
        for &neighbor_id in topology.neighbors(replica_id) {
            network.send(
                replica_id,
                neighbor_id,
                ProtocolMsg::Elements(payload.clone()),
            );
        }
    }

    fn recv_phase(
        &mut self,
        _replica_id: usize,
        local: &Replica,
        _topology: &Topology,
        inbox: Vec<(usize, ProtocolMsg)>,
        _network: &mut Network<ProtocolMsg>,
    ) -> ProtocolStepResult {
        let mut next_set = local.snapshot_set();
        for (_from, msg) in inbox {
            if let ProtocolMsg::Elements(els) = msg {
                for element in els {
                    next_set.insert(element);
                }
            }
        }

        ProtocolStepResult {
            next_set,
            metrics: LocalMetrics::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulator::protocols::test_helpers::make_element;
    use std::collections::HashSet;

    fn make_replica(id: usize, elements: Vec<Element>) -> Replica {
        let set = elements.into_iter().collect::<HashSet<_>>();
        Replica::new(id, set)
    }

    #[test]
    fn protocol2_full_state_transfer_bills_full_neighbour_set() {
        let mut protocol = FullStateTransfer::new();
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
        let mut network: Network<ProtocolMsg> = Network::from_topology(&topology);

        for (id, replica) in replicas.iter().enumerate() {
            protocol.send_phase(id, replica, &topology, &mut network);
        }

        let expected_node0 = (std::mem::size_of::<u64>() + 4) as u64;
        let expected_node1 = ((std::mem::size_of::<u64>() + 4)
            + (std::mem::size_of::<u64>() + 10)
            + (std::mem::size_of::<u64>() + 6)) as u64;

        let stats = network.stats();
        assert_eq!(stats.per_node_state[0], expected_node0);
        assert_eq!(stats.per_node_state[1], expected_node1);
        assert_eq!(stats.bytes_state, expected_node0 + expected_node1);
        assert_eq!(stats.bytes_metadata, 0);
    }

    #[test]
    fn protocol2_full_state_transfer_recv_merges_neighbours() {
        let mut protocol = FullStateTransfer::new();
        let topology = Topology::star(2);
        let replicas = vec![
            make_replica(0, vec![make_element(1, 1, 4), make_element(2, 2, 4)]),
            make_replica(1, vec![make_element(2, 2, 4), make_element(3, 3, 4)]),
        ];
        let mut network: Network<ProtocolMsg> = Network::from_topology(&topology);

        for (id, replica) in replicas.iter().enumerate() {
            protocol.send_phase(id, replica, &topology, &mut network);
        }

        let inbox_0 = network.drain_inbox(0);
        let result_0 = protocol.recv_phase(0, &replicas[0], &topology, inbox_0, &mut network);

        let digests = result_0
            .next_set
            .iter()
            .map(|e| e.digest)
            .collect::<HashSet<_>>();
        assert!(digests.contains(&1));
        assert!(digests.contains(&2));
        assert!(digests.contains(&3));
        assert_eq!(digests.len(), 3);
    }
}
