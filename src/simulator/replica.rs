use std::collections::HashSet;
use std::time::Duration;

/// A set element. Carries the 8-byte `digest` (its identity) and the
/// `payload_len` — the size of the application payload it stands for. The
/// payload *content* is never inspected by the simulator (only its length
/// feeds `wire_size`), so storing the length instead of the bytes keeps the
/// byte accounting identical while making `Element` a 16-byte `Copy` value
/// with no per-element heap allocation. That matters at scale: at n=64 every
/// replica converges to the full union, so the set storage is
/// O(replicas x union) — holding real payload Vecs there exhausted memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Element {
    pub digest: u64,
    pub payload_len: usize,
}

impl Element {
    pub fn new(digest: u64, payload_len: usize) -> Self {
        Self { digest, payload_len }
    }

    /// Serialised byte size: 8-byte digest + payload bytes.
    pub fn wire_size(&self) -> usize {
        std::mem::size_of::<u64>() + self.payload_len
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
