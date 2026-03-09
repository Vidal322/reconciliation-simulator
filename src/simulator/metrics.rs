use std::collections::HashMap;
use std::time::Duration;

use crate::simulator::replica::Replica;

#[derive(Clone, Debug, Default)]
pub struct NodeMetrics {
    pub replica_id: usize,
    pub bytes_sent: usize,
    pub bytes_received: usize,
    pub encode_time: Duration,
    pub decode_time: Duration,
    pub elements_added: usize,
}

#[derive(Clone, Debug, Default)]
pub struct MetricsSnapshot {
    pub rounds: usize,
    pub total_bytes_sent: usize,
    pub total_bytes_received: usize,
    pub total_encode_time: Duration,
    pub total_decode_time: Duration,
    pub total_elements_added: usize,
    pub per_node: Vec<NodeMetrics>,
}

#[derive(Clone, Debug, Default)]
pub struct MetricsCollector {
    rounds: usize,
    total_bytes_sent: usize,
    total_bytes_received: usize,
    total_encode_time: Duration,
    total_decode_time: Duration,
    total_elements_added: usize,
    per_node: HashMap<usize, NodeMetrics>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_rounds(&mut self, rounds: usize) {
        self.rounds = rounds;
    }

    pub fn record_replica(&mut self, replica: &Replica) {
        let entry = self
            .per_node
            .entry(replica.id)
            .or_insert_with(|| NodeMetrics {
                replica_id: replica.id,
                ..Default::default()
            });

        entry.bytes_sent = replica.stats.bytes_sent;
        entry.bytes_received = replica.stats.bytes_received;
        entry.encode_time = replica.stats.encode_time;
        entry.decode_time = replica.stats.decode_time;
        entry.elements_added = replica.stats.elements_added;
    }

    pub fn record_replicas(&mut self, replicas: &[Replica]) {
        self.per_node.clear();

        for replica in replicas {
            self.record_replica(replica);
        }

        self.recompute_totals();
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let mut per_node = self.per_node.values().cloned().collect::<Vec<_>>();
        per_node.sort_unstable_by_key(|m| m.replica_id);

        MetricsSnapshot {
            rounds: self.rounds,
            total_bytes_sent: self.total_bytes_sent,
            total_bytes_received: self.total_bytes_received,
            total_encode_time: self.total_encode_time,
            total_decode_time: self.total_decode_time,
            total_elements_added: self.total_elements_added,
            per_node,
        }
    }

    fn recompute_totals(&mut self) {
        self.total_bytes_sent = 0;
        self.total_bytes_received = 0;
        self.total_encode_time = Duration::default();
        self.total_decode_time = Duration::default();
        self.total_elements_added = 0;

        for node in self.per_node.values() {
            self.total_bytes_sent += node.bytes_sent;
            self.total_bytes_received += node.bytes_received;
            self.total_encode_time += node.encode_time;
            self.total_decode_time += node.decode_time;
            self.total_elements_added += node.elements_added;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulator::replica::{Replica, ReplicaStats};
    use std::collections::HashSet;
    use std::time::Duration;

    fn make_replica(
        id: usize,
        bytes_sent: usize,
        bytes_received: usize,
        encode_ms: u64,
        decode_ms: u64,
        elements_added: usize,
    ) -> Replica {
        let mut replica = Replica::new(id, HashSet::new());
        replica.stats = ReplicaStats {
            bytes_sent,
            bytes_received,
            encode_time: Duration::from_millis(encode_ms),
            decode_time: Duration::from_millis(decode_ms),
            elements_added,
        };
        replica
    }

    #[test]
    fn records_single_replica_correctly() {
        let replica = make_replica(3, 100, 50, 7, 11, 4);

        let mut collector = MetricsCollector::new();
        collector.set_rounds(2);
        collector.record_replica(&replica);

        let snapshot = collector.snapshot();

        assert_eq!(snapshot.rounds, 2);
        assert_eq!(snapshot.total_bytes_sent, 0);
        assert_eq!(snapshot.total_bytes_received, 0);
        assert_eq!(snapshot.total_elements_added, 0);

        assert_eq!(snapshot.per_node.len(), 1);
        let node = &snapshot.per_node[0];
        assert_eq!(node.replica_id, 3);
        assert_eq!(node.bytes_sent, 100);
        assert_eq!(node.bytes_received, 50);
        assert_eq!(node.encode_time, Duration::from_millis(7));
        assert_eq!(node.decode_time, Duration::from_millis(11));
        assert_eq!(node.elements_added, 4);
    }

    #[test]
    fn records_multiple_replicas_and_recomputes_totals() {
        let r1 = make_replica(0, 100, 40, 5, 8, 2);
        let r2 = make_replica(1, 200, 60, 7, 9, 3);
        let r3 = make_replica(2, 300, 80, 11, 13, 5);

        let mut collector = MetricsCollector::new();
        collector.set_rounds(4);
        collector.record_replicas(&[r1, r2, r3]);

        let snapshot = collector.snapshot();

        assert_eq!(snapshot.rounds, 4);
        assert_eq!(snapshot.total_bytes_sent, 600);
        assert_eq!(snapshot.total_bytes_received, 180);
        assert_eq!(snapshot.total_encode_time, Duration::from_millis(23));
        assert_eq!(snapshot.total_decode_time, Duration::from_millis(30));
        assert_eq!(snapshot.total_elements_added, 10);
        assert_eq!(snapshot.per_node.len(), 3);
    }

    #[test]
    fn per_node_snapshot_is_sorted_by_replica_id() {
        let r1 = make_replica(5, 10, 10, 1, 1, 1);
        let r2 = make_replica(2, 20, 20, 2, 2, 2);
        let r3 = make_replica(9, 30, 30, 3, 3, 3);

        let mut collector = MetricsCollector::new();
        collector.record_replicas(&[r1, r2, r3]);

        let snapshot = collector.snapshot();
        let ids: Vec<usize> = snapshot.per_node.iter().map(|n| n.replica_id).collect();

        assert_eq!(ids, vec![2, 5, 9]);
    }

    #[test]
    fn record_replicas_replaces_previous_state() {
        let r1 = make_replica(0, 100, 50, 5, 5, 1);
        let r2 = make_replica(1, 200, 60, 6, 6, 2);

        let mut collector = MetricsCollector::new();
        collector.record_replicas(&[r1, r2]);

        let snapshot1 = collector.snapshot();
        assert_eq!(snapshot1.total_bytes_sent, 300);
        assert_eq!(snapshot1.per_node.len(), 2);

        let r3 = make_replica(7, 10, 20, 1, 2, 3);
        collector.record_replicas(&[r3]);

        let snapshot2 = collector.snapshot();
        assert_eq!(snapshot2.total_bytes_sent, 10);
        assert_eq!(snapshot2.total_bytes_received, 20);
        assert_eq!(snapshot2.per_node.len(), 1);
        assert_eq!(snapshot2.per_node[0].replica_id, 7);
    }
}
