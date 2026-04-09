use std::collections::VecDeque;

use crate::simulator::topology::Topology;
/// Wire-size contract for anything sent through `Network`.
/// Split into `state_bytes` (payload: actual set elements being transferred)
/// and `metadata_bytes` (sketches, filters, coefficients, headers).
pub trait WireSized {
    fn state_bytes(&self) -> u64;
    fn metadata_bytes(&self) -> u64;
}

#[derive(Debug, Clone, Default)]
pub struct NetworkStats {
    pub bytes_state: u64,
    pub bytes_metadata: u64,
    pub per_node_state: Vec<u64>,
    pub per_node_metadata: Vec<u64>,
}

/// A simulated point-to-point network.
///
/// Each node has a per-node inbox.  Messages are delivered by calling
/// send, which checks if `from` and `to` are topology neighbours
/// and then places `msg` in `to`'s inbox.
///
pub struct Network<Msg: WireSized> {
    inboxes: Vec<VecDeque<(usize, Msg)>>,
    neighbors: Vec<Vec<usize>>,
    bytes_state: u64,
    bytes_metadata: u64,
    per_node_state: Vec<u64>,
    per_node_metadata: Vec<u64>,
}

impl<Msg: WireSized> Network<Msg> {
    /// Build a `Network` mirroring the adjacency of `topology`.
    pub fn from_topology(topology: &Topology) -> Self {
        let n = topology.node_count();
        let neighbors = (0..n).map(|id| topology.neighbors(id).to_vec()).collect();
        Self {
            inboxes: (0..n).map(|_| VecDeque::new()).collect(),
            neighbors,
            bytes_state: 0,
            bytes_metadata: 0,
            per_node_state: vec![0; n],
            per_node_metadata: vec![0; n],
        }
    }

    /// Deliver `msg` from `from` to `to`.
    /// Panics if `to` is not a neighbour of `from`
    pub fn send(&mut self, from: usize, to: usize, msg: Msg) {
        assert!(
            self.neighbors[from].contains(&to),
            "node {from} attempted to send to non-neighbour {to}"
        );
        // count data
        let s = msg.state_bytes();
        let m = msg.metadata_bytes();

        self.bytes_state += s;
        self.bytes_metadata += m;
        self.per_node_state[from] += s;
        self.per_node_metadata[from] += m;

        self.inboxes[to].push_back((from, msg));
    }

    /// Drain and return all pending messages for `node`.
    pub fn drain_inbox(&mut self, node: usize) -> Vec<(usize, Msg)> {
        self.inboxes[node].drain(..).collect()
    }

    /// Record metadata bytes attributed to a node that learned them
    /// by decoding (rather than emitting). Used by interactive
    /// protocols where the wire cost depends on the receiver's local
    /// state — e.g. RIBLT, where the symbol count is determined
    /// during the joint decode and is not knowable at send time.
    ///
    /// This is the only way to bill bytes outside `send()`. The method
    /// lives on `Network` so the harness retains exclusive control of
    /// the byte counters; protocols cannot bypass it.
    pub fn record_decoded_metadata(&mut self, node: usize, bytes: u64) {
        self.bytes_metadata += bytes;
        self.per_node_metadata[node] += bytes;
    }

    pub fn stats(&self) -> NetworkStats {
        NetworkStats {
            bytes_state: self.bytes_state,
            bytes_metadata: self.bytes_metadata,
            per_node_state: self.per_node_state.clone(),
            per_node_metadata: self.per_node_metadata.clone(),
        }
    }

    /// Clear every inbox and reset metrics
    pub fn reset(&mut self) {
        for inbox in &mut self.inboxes {
            inbox.clear();
        }
        self.bytes_state = 0;
        self.bytes_metadata = 0;
        self.per_node_state.iter_mut().for_each(|v| *v = 0);
        self.per_node_metadata.iter_mut().for_each(|v| *v = 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulator::topology::Topology;

    struct Dummy {
        s: u64,
        m: u64,
    }
    impl WireSized for Dummy {
        fn state_bytes(&self) -> u64 {
            self.s
        }
        fn metadata_bytes(&self) -> u64 {
            self.m
        }
    }

    #[test]
    fn send_counts_bytes_on_sender_side() {
        // Star with 3 nodes: 0 is centre, 1 and 2 are leaves.
        let topo = Topology::star(3);
        let mut net: Network<Dummy> = Network::from_topology(&topo);

        net.send(0, 1, Dummy { s: 100, m: 7 });
        net.send(0, 2, Dummy { s: 50, m: 3 });

        let st = net.stats();
        assert_eq!(st.bytes_state, 150);
        assert_eq!(st.bytes_metadata, 10);
        assert_eq!(st.per_node_state[0], 150);
        assert_eq!(st.per_node_metadata[0], 10);
        assert_eq!(st.per_node_state[1], 0);
        assert_eq!(st.per_node_state[2], 0);
    }

    #[test]
    fn reset_clears_counters_and_inboxes() {
        let topo = Topology::star(2);
        let mut net: Network<Dummy> = Network::from_topology(&topo);
        net.send(0, 1, Dummy { s: 42, m: 9 });
        net.reset();
        let st = net.stats();
        assert_eq!(st.bytes_state, 0);
        assert_eq!(st.bytes_metadata, 0);
        assert_eq!(st.per_node_state[0], 0);
        assert!(net.drain_inbox(1).is_empty());
    }

    #[test]
    #[should_panic(expected = "non-neighbour")]
    fn send_to_non_neighbour_panics() {
        // Tree with 3 nodes: 0 is root, 1 and 2 are its children.
        // Nodes 1 and 2 are siblings — not directly connected.
        let topo = Topology::tree(3);
        let mut net: Network<Dummy> = Network::from_topology(&topo);
        net.send(1, 2, Dummy { s: 1, m: 1 });
    }
}
