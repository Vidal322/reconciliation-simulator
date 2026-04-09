use crate::simulator::network::WireSized;
use crate::simulator::replica::Element;

use std::mem;

pub enum ProtocolMsg {
    Elements(Vec<Element>),

    RibltSketch {
        symbols: usize,
        digests: Vec<u64>,
        elements: Vec<Element>,
    },

    BloomFilter {
        bit_len: usize,
        digests: Vec<u64>,
        false_positive_rate: f64,
    },

    RatelessBloom {
        byte_len: usize,
        digests: Vec<u64>,
        bloom_bits: usize,
    },
}

impl WireSized for ProtocolMsg {
    fn state_bytes(&self) -> u64 {
        match self {
            // Only Elements carries real payload across the wire
            ProtocolMsg::Elements(els) => els.iter().map(|e| e.wire_size() as u64).sum(),
            // Sketches and filters are pure metadata — no elements in them.
            ProtocolMsg::RibltSketch { .. }
            | ProtocolMsg::BloomFilter { .. }
            | ProtocolMsg::RatelessBloom { .. } => 0,
        }
    }

    fn metadata_bytes(&self) -> u64 {
        match self {
            // Elements are pure payload — zero metadata.
            ProtocolMsg::Elements(_) => 0,
            //  A RIBLT sketch is a sequence of coded symbols. Each coded symbol is roughly a
            //  u64.  That's pure metadata — no elements cross the wire in this message
            //  state = 0
            //  metadata = symbols * 8
            ProtocolMsg::RibltSketch { symbols, .. } => {
                (*symbols as u64) * (mem::size_of::<u64>() as u64)
            }
            //A Bloom filter is a bit array. Therefore only metadata is sent.
            //The bit array rounded up to bytes + length header + seed.
            ProtocolMsg::BloomFilter { bit_len, .. } => {
                let bloom_bytes = (bit_len + 7) / 8;
                (bloom_bytes + mem::size_of::<usize>() + mem::size_of::<u64>()) as u64
            }
            //The rateless Bloom has its own size_of() method that returns the byte count.
            // The variant just stores that number; metadata_bytes returns it.
            ProtocolMsg::RatelessBloom { byte_len, .. } => *byte_len as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulator::protocols::test_helpers::make_element;

    #[test]
    fn elements_variant_bills_only_state_bytes() {
        let msg = ProtocolMsg::Elements(vec![make_element(1, 1, 4), make_element(2, 2, 10)]);
        // wire_size = size_of::<u64>() + payload_len
        let expected = (mem::size_of::<u64>() + 4 + mem::size_of::<u64>() + 10) as u64;
        assert_eq!(msg.state_bytes(), expected);
        assert_eq!(msg.metadata_bytes(), 0);
    }

    #[test]
    fn riblt_sketch_bills_symbols_as_metadata() {
        let msg = ProtocolMsg::RibltSketch {
            symbols: 12,
            digests: vec![],
            elements: vec![],
        };
        assert_eq!(msg.state_bytes(), 0);
        assert_eq!(msg.metadata_bytes(), 12 * mem::size_of::<u64>() as u64);
    }

    #[test]
    fn bloom_filter_bills_bits_plus_header() {
        let msg = ProtocolMsg::BloomFilter {
            bit_len: 128,
            digests: vec![],
            false_positive_rate: 0.01,
        };
        let expected_bytes = 16 + mem::size_of::<usize>() + mem::size_of::<u64>();
        assert_eq!(msg.metadata_bytes(), expected_bytes as u64);
        assert_eq!(msg.state_bytes(), 0);
    }

    #[test]
    fn rateless_bloom_bills_byte_len_as_metadata() {
        let msg = ProtocolMsg::RatelessBloom {
            byte_len: 77,
            digests: vec![],
            bloom_bits: 128,
        };
        assert_eq!(msg.metadata_bytes(), 77);
        assert_eq!(msg.state_bytes(), 0);
    }
}
