use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::simulator::replica::Replica;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NodeMetrics {
    pub replica_id: usize,
    pub state_bytes_sent: usize,
    pub metadata_bytes_sent: usize,
    pub encode_time: Duration,
    pub decode_time: Duration,
    pub elements_added: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub rounds: usize,
    pub total_state_bytes_sent: usize,
    pub total_metadata_bytes_sent: usize,
    pub total_encode_time: Duration,
    pub total_decode_time: Duration,
    pub total_elements_added: usize,
    pub per_node: Vec<NodeMetrics>,
}

impl MetricsSnapshot {
    /// Sum of state and metadata bytes sent across all replicas.
    /// This is the scalar the eval fitness function consumes.
    pub fn total_bytes_sent(&self) -> usize {
        self.total_state_bytes_sent + self.total_metadata_bytes_sent
    }

    /// Build a snapshot directly from replica state. `Replica.stats` is
    /// the source of truth for per-replica counters; this is a pure
    /// projection of that source.
    pub fn from_replicas(replicas: &[Replica], rounds: usize) -> Self {
        let mut per_node: Vec<NodeMetrics> = replicas
            .iter()
            .map(|r| NodeMetrics {
                replica_id: r.id,
                state_bytes_sent: r.stats.state_bytes_sent,
                metadata_bytes_sent: r.stats.metadata_bytes_sent,
                encode_time: r.stats.encode_time,
                decode_time: r.stats.decode_time,
                elements_added: r.stats.elements_added,
            })
            .collect();
        per_node.sort_unstable_by_key(|m| m.replica_id);

        let total_state_bytes_sent = per_node.iter().map(|n| n.state_bytes_sent).sum();
        let total_metadata_bytes_sent = per_node.iter().map(|n| n.metadata_bytes_sent).sum();
        let total_encode_time = per_node.iter().map(|n| n.encode_time).sum();
        let total_decode_time = per_node.iter().map(|n| n.decode_time).sum();
        let total_elements_added = per_node.iter().map(|n| n.elements_added).sum();

        Self {
            rounds,
            total_state_bytes_sent,
            total_metadata_bytes_sent,
            total_encode_time,
            total_decode_time,
            total_elements_added,
            per_node,
        }
    }
}
