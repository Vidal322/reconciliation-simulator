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

