use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::simulator::replica::Replica;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NodeMetrics {
    pub replica_id: usize,
    pub state_bytes_sent: usize,
    pub state_bytes_received: usize,
    pub metadata_bytes_sent: usize,
    pub metadata_bytes_received: usize,
    pub encode_time: Duration,
    pub decode_time: Duration,
    pub elements_added: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub rounds: usize,
    pub total_state_bytes_sent: usize,
    pub total_state_bytes_received: usize,
    pub total_metadata_bytes_sent: usize,
    pub total_metadata_bytes_received: usize,
    pub total_encode_time: Duration,
    pub total_decode_time: Duration,
    pub total_elements_added: usize,
    pub per_node: Vec<NodeMetrics>,
}

#[derive(Clone, Debug, Default)]
pub struct MetricsCollector {
    rounds: usize,
    total_state_bytes_sent: usize,
    total_state_bytes_received: usize,
    total_metadata_bytes_sent: usize,
    total_metadata_bytes_received: usize,
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

        entry.state_bytes_sent = replica.stats.state_bytes_sent;
        entry.state_bytes_received = replica.stats.state_bytes_received;
        entry.metadata_bytes_sent = replica.stats.metadata_bytes_sent;
        entry.metadata_bytes_received = replica.stats.metadata_bytes_received;
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
            total_state_bytes_sent: self.total_state_bytes_sent,
            total_state_bytes_received: self.total_state_bytes_received,
            total_metadata_bytes_sent: self.total_metadata_bytes_sent,
            total_metadata_bytes_received: self.total_metadata_bytes_received,
            total_encode_time: self.total_encode_time,
            total_decode_time: self.total_decode_time,
            total_elements_added: self.total_elements_added,
            per_node,
        }
    }

    fn recompute_totals(&mut self) {
        self.total_state_bytes_sent = 0;
        self.total_state_bytes_received = 0;
        self.total_metadata_bytes_sent = 0;
        self.total_metadata_bytes_received = 0;
        self.total_encode_time = Duration::default();
        self.total_decode_time = Duration::default();
        self.total_elements_added = 0;

        for node in self.per_node.values() {
            self.total_state_bytes_sent += node.state_bytes_sent;
            self.total_state_bytes_received += node.state_bytes_received;
            self.total_metadata_bytes_sent += node.metadata_bytes_sent;
            self.total_metadata_bytes_received += node.metadata_bytes_received;
            self.total_encode_time += node.encode_time;
            self.total_decode_time += node.decode_time;
            self.total_elements_added += node.elements_added;
        }
    }
}
