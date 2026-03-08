use std::collections::HashSet;
use std::time::Duration;

pub struct Element {
    pub digest: u64,
    pub payload: Vec<u8>
}

impl Element {
    pub fn new(digest: u64, payload:Vec<u8>) -> Self {
        Self { digest, payload }
    }
    
    pub fn payload_len(&self) -> usize {
        self.payload.len()    
    }
}

#[derive(Copy, Eq, Default)]
pub enum ReplicaPhase {
    #[default]
    Idle,
    Active,
    Converged,
    Failed,
}

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

pub struct Replica {
    pub id: usize,
    pub set: HashSet<Element>,
    pub phase: ReplicaPhase,
    pub stats: ReplicaStats
}

impl Replica {
        pub fn new(id: usize, set: HashSet<Element>):
            Self {
                id, 
                set,
                ReplicaPhase::default()
                ReplicaStats::default()
            }

}


