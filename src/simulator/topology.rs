use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TopologyKind {
    Star,
    Tree,
    Chord,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Topology {
    pub kind: TopologyKind,
    pub num_nodes: usize,
    adjacency: Vec<Vec<usize>>,
}

impl Topology {
    pub fn build(kind: TopologyKind, num_nodes: usize) -> Self {
        match kind {
            TopologyKind::Star => Self::star(num_nodes),
            TopologyKind::Tree => Self::tree(num_nodes),
            TopologyKind::Chord => Self::chord(num_nodes),
        }
    }

    pub fn star(num_nodes: usize) -> Self {
        let mut adjacency = vec![Vec::new(); num_nodes];

        if num_nodes > 1 {
            for node in 1..num_nodes {
                adjacency[0].push(node);
                adjacency[node].push(0);
            }
        }

        Self {
            kind: TopologyKind::Star,
            num_nodes,
            adjacency,
        }
    }

    pub fn tree(num_nodes: usize) -> Self {
        let mut adjacency = vec![Vec::new(); num_nodes];

        for node in 1..num_nodes {
            let parent = (node - 1) / 2;
            adjacency[node].push(parent);
            adjacency[parent].push(node);
        }

        Self {
            kind: TopologyKind::Tree,
            num_nodes,
            adjacency,
        }
    }

    pub fn chord(num_nodes: usize) -> Self {
        let mut adjacency = vec![Vec::new(); num_nodes];

        if num_nodes == 0 {
            return Self {
                kind: TopologyKind::Chord,
                num_nodes,
                adjacency,
            };
        }

        for node in 0..num_nodes {
            for power in 0.. {
                let offset = 1usize << power;
                if offset >= num_nodes {
                    break;
                }

                let neighbor = (node + offset) % num_nodes;
                Self::add_undirected_edge(&mut adjacency, node, neighbor);
            }
        }

        Self {
            kind: TopologyKind::Chord,
            num_nodes,
            adjacency,
        }
    }

    pub fn neighbors(&self, node: usize) -> &[usize] {
        &self.adjacency[node]
    }

    pub fn is_neighbor(&self, a: usize, b: usize) -> bool {
        self.adjacency[a].contains(&b)
    }

    pub fn node_count(&self) -> usize {
        self.num_nodes
    }

    pub fn edge_count(&self) -> usize {
        self.adjacency.iter().map(|n| n.len()).sum::<usize>() / 2
    }

    pub fn edges(&self) -> Vec<(usize, usize)> {
        let mut edges = Vec::new();

        for a in 0..self.num_nodes {
            for &b in &self.adjacency[a] {
                if a < b {
                    edges.push((a, b));
                }
            }
        }

        edges
    }

    fn add_undirected_edge(adjacency: &mut [Vec<usize>], a: usize, b: usize) {
        if a == b {
            return;
        }

        if !adjacency[a].contains(&b) {
            adjacency[a].push(b);
        }

        if !adjacency[b].contains(&a) {
            adjacency[b].push(a);
        }
    }

    pub fn is_root(&self, node: usize) -> bool {
        match self.kind {
            TopologyKind::Tree => node == 0,
            _ => false,
        }
    }

    pub fn parent(&self, node: usize) -> Option<usize> {
        match self.kind {
            TopologyKind::Tree => {
                if node == 0 || node >= self.num_nodes {
                    None
                } else {
                    Some((node - 1) / 2)
                }
            }
            _ => None,
        }
    }

    pub fn children(&self, node: usize) -> Vec<usize> {
        match self.kind {
            TopologyKind::Tree => {
                if node >= self.num_nodes {
                    return Vec::new();
                }

                let left = 2 * node + 1;
                let right = 2 * node + 2;

                let mut children = Vec::new();

                if left < self.num_nodes {
                    children.push(left);
                }
                if right < self.num_nodes {
                    children.push(right);
                }

                children
            }
            _ => Vec::new(),
        }
    }
}
