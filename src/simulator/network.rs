use std::collections::VecDeque;

use crate::simulator::topology::Topology;

/// A simulated point-to-point network.
///
/// Each node has a per-node inbox.  Messages are delivered by calling
/// send, which checks if `from` and `to` are topology neighbours
/// and then places `msg` in `to`'s inbox.
///
pub struct Network<Msg> {
    inboxes: Vec<VecDeque<(usize, Msg)>>,
    neighbors: Vec<Vec<usize>>,
}

impl<Msg> Network<Msg> {
    /// Build a `Network` mirroring the adjacency of `topology`.
    pub fn from_topology(topology: &Topology) -> Self {
        let n = topology.node_count();
        let neighbors = (0..n).map(|id| topology.neighbors(id).to_vec()).collect();
        Self {
            inboxes: (0..n).map(|_| VecDeque::new()).collect(),
            neighbors,
        }
    }

    /// Deliver `msg` from `from` to `to`.
    ///
    /// Panics if `to` is not a neighbour of `from`
    pub fn send(&mut self, from: usize, to: usize, msg: Msg) {
        assert!(
            self.neighbors[from].contains(&to),
            "node {from} attempted to send to non-neighbour {to}"
        );
        self.inboxes[to].push_back((from, msg));
    }

    /// Drain and return all pending messages for `node`.
    pub fn drain_inbox(&mut self, node: usize) -> Vec<(usize, Msg)> {
        self.inboxes[node].drain(..).collect()
    }

    /// Clear every inbox
    pub fn reset(&mut self) {
        for inbox in &mut self.inboxes {
            inbox.clear();
        }
    }
}
