use std::collections::HashSet;
use std::time::Duration;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Element {
    pub digest: u64,
    pub payload: Vec<u8>,
}

impl Element {
    pub fn new(digest: u64, payload: Vec<u8>) -> Self {
        Self { digest, payload }
    }

    pub fn payload_len(&self) -> usize {
        self.payload.len()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Default)]
pub enum ReplicaPhase {
    #[default]
    Idle,
    Active,
    Converged,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Default)]
pub struct ReplicaStats {
    pub bytes_sent: usize,
    pub bytes_received: usize,
    pub encode_time: Duration,
    pub decode_time: Duration,
    pub elements_added: usize,
}

impl ReplicaStats {
    pub fn record_bytes_sent(&mut self, bytes: usize) {
        self.bytes_sent += bytes;
    }

    pub fn record_bytes_received(&mut self, bytes: usize) {
        self.bytes_received += bytes;
    }

    pub fn record_encode_time(&mut self, duration: Duration) {
        self.encode_time += duration;
    }

    pub fn record_decode_time(&mut self, duration: Duration) {
        self.decode_time += duration;
    }

    pub fn record_elements_added(&mut self, count: usize) {
        self.elements_added += count;
    }
}
#[derive(Debug)]
pub struct Replica {
    pub id: usize,
    pub set: HashSet<Element>,
    pub phase: ReplicaPhase,
    pub stats: ReplicaStats,
}

impl Replica {
    pub fn new(id: usize, set: HashSet<Element>) -> Self {
        Self {
            id,
            set,
            phase: ReplicaPhase::default(),
            stats: ReplicaStats::default(),
        }
    }

    pub fn snapshot_set(&self) -> HashSet<Element> {
        self.set.clone()
    }

    pub fn set_phase(&mut self, phase: ReplicaPhase) {
        self.phase = phase;
    }

    pub fn replace_set(&mut self, next_set: HashSet<Element>) {
        let added = next_set.difference(&self.set).count();
        self.stats.record_elements_added(added);
        self.set = next_set;
    }

    pub fn len(&self) -> usize {
        self.set.len()
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    pub fn contains(&self, element: &Element) -> bool {
        self.set.contains(element)
    }

    pub fn insert(&mut self, element: Element) -> bool {
        let inserted = self.set.insert(element);

        if inserted {
            self.stats.record_elements_added(1);
        }
        inserted
    }

    pub fn extend<I>(&mut self, elements: I) -> usize
    where
        I: IntoIterator<Item = Element>,
    {
        let mut added = 0;
        for element in elements {
            if self.set.insert(element) {
                added += 1;
            }
        }

        if added > 0 {
            self.stats.record_elements_added(added)
        }

        added
    }
}
