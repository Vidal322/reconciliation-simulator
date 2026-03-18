// Deterministic seeds required for cross-replica aggregation.
// Using RandomState would produce different seeds per process.
const CBF_SEED_0: u64 = 0x517cc1b727220a95;
const CBF_SEED_1: u64 = 0x6c62272e07bb0142;

/// Counting Bloom filter with u32 counters (safe for N ≤ 64 replicas).
///
/// Uses double hashing from a pre-computed digest value, so the caller is
/// responsible for providing a stable digest (e.g. `element.digest`).
pub struct CountingBf {
    cells: Vec<u32>,
    num_hashes: usize,
}

impl CountingBf {
    pub fn new(num_cells: usize, num_hashes: usize) -> Self {
        Self {
            cells: vec![0u32; num_cells],
            num_hashes,
        }
    }

    fn slots(&self, digest: u64) -> impl Iterator<Item = usize> + '_ {
        let h0 = digest.wrapping_mul(CBF_SEED_0);
        let h1 = digest.wrapping_mul(CBF_SEED_1);
        let num_cells = self.cells.len();
        (0..self.num_hashes).map(move |i| {
            (h0.wrapping_add((i as u64).wrapping_mul(h1))) as usize % num_cells
        })
    }

    /// Increment all k slots for the given digest.
    pub fn insert(&mut self, digest: u64) {
        for slot in self.slots(digest).collect::<Vec<_>>() {
            self.cells[slot] = self.cells[slot].saturating_add(1);
        }
    }

    /// Cell-wise addition: self += other (aggregation).
    pub fn add(&mut self, other: &CountingBf) {
        for (a, b) in self.cells.iter_mut().zip(&other.cells) {
            *a = a.saturating_add(*b);
        }
    }

    /// Cell-wise saturating subtraction: self -= other (diff).
    pub fn sub(&mut self, other: &CountingBf) {
        for (a, b) in self.cells.iter_mut().zip(&other.cells) {
            *a = a.saturating_sub(*b);
        }
    }

    /// Minimum of the k slots — standard CBF membership query.
    pub fn query(&self, digest: u64) -> u32 {
        self.slots(digest)
            .map(|slot| self.cells[slot])
            .min()
            .unwrap_or(0)
    }

    /// Serialised byte length (4 bytes per u32 counter).
    pub fn byte_len(&self) -> usize {
        self.cells.len() * 4
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_query_found() {
        let mut cbf = CountingBf::new(64, 3);
        cbf.insert(42);
        assert!(cbf.query(42) >= 1);
    }

    #[test]
    fn query_absent_is_zero() {
        let cbf = CountingBf::new(64, 3);
        assert_eq!(cbf.query(99), 0);
    }

    #[test]
    fn add_aggregates_counts() {
        let mut a = CountingBf::new(64, 3);
        let mut b = CountingBf::new(64, 3);
        a.insert(7);
        b.insert(7);
        a.add(&b);
        assert!(a.query(7) >= 2);
    }

    #[test]
    fn sub_restores_zero() {
        let mut a = CountingBf::new(64, 3);
        let mut b = CountingBf::new(64, 3);
        a.insert(7);
        b.insert(7);
        a.add(&b);
        a.sub(&b);
        a.sub(&{
            let mut tmp = CountingBf::new(64, 3);
            tmp.insert(7);
            tmp
        });
        assert_eq!(a.query(7), 0);
    }

    #[test]
    fn byte_len_correct() {
        let cbf = CountingBf::new(128, 3);
        assert_eq!(cbf.byte_len(), 128 * 4);
    }
}
