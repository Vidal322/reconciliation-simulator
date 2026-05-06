use std::collections::HashSet;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Element {
    pub digest: u64,
    pub payload: Vec<u8>,
}

impl Element {
    pub fn new(digest: u64, payload: Vec<u8>) -> Self {
        Self { digest, payload }
    }

    /// Serialised byte size: 8-byte digest + payload bytes.
    pub fn wire_size(&self) -> usize {
        std::mem::size_of::<u64>() + self.payload.len()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ReplicaPhase {
    #[default]
    Idle,
    Active,
    Converged,
}

#[derive(Clone, Debug, Default)]
pub struct ReplicaStats {
    pub state_bytes_sent: usize,
    pub metadata_bytes_sent: usize,
    pub encode_time: Duration,
    pub decode_time: Duration,
    pub elements_added: usize,
}

impl ReplicaStats {
    pub fn record_state_bytes_sent(&mut self, bytes: usize) {
        self.state_bytes_sent += bytes;
    }

    pub fn record_metadata_bytes_sent(&mut self, bytes: usize) {
        self.metadata_bytes_sent += bytes;
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

/// Narrow, borrowed view of a replica passed to protocol methods.
/// Exposes only what reconciliation logic legitimately needs: the
/// replica's identity and its current set. Engine-internal fields
/// (`stats`, `phase`) are invisible to protocols.
pub struct ReplicaView<'a> {
    pub id: usize,
    pub set: &'a HashSet<Element>,
}

impl<'a> ReplicaView<'a> {
    pub fn snapshot_set(&self) -> HashSet<Element> {
        self.set.clone()
    }
}

#[derive(Clone, Debug)]
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
            phase: ReplicaPhase::Idle,
            stats: ReplicaStats::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.set.len()
    }

    pub fn view(&self) -> ReplicaView<'_> {
        ReplicaView { id: self.id, set: &self.set }
    }

    pub fn set_phase(&mut self, phase: ReplicaPhase) {
        self.phase = phase;
    }
}
