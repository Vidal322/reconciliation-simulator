use std::{
    cmp::max,
    f64::consts::LN_2,
    hash::{BuildHasher, DefaultHasher, Hash, Hasher},
    marker::PhantomData,
    time::{Duration, Instant},
};

use bitvec::{bitvec, slice::BitSlice, vec::BitVec};

/// Two fixed seeds for the Bloom filter's pair of hash functions. Using a
/// deterministic, seeded `BuildHasher` instead of `std`'s `RandomState`
/// makes a filter's false-positive pattern a pure function of its contents
/// and these constants — never of per-process hash randomization. This
/// removes the run-to-run byte-count jitter (the ~2.3% "noise floor") so
/// that reconciliation cost is exactly reproducible for a fixed input,
/// which is the precondition for trusting small fitness deltas during the
/// search. The two seeds differ so the double-hashing scheme `h0 + i*h1`
/// draws on two independent hash streams; any two distinct seeds work.
const BLOOM_SEED_0: u64 = 0x9E37_79B9_7F4A_7C15;
const BLOOM_SEED_1: u64 = 0xD1B5_4A32_D192_ED03;

/// Deterministic, seeded `BuildHasher`. Pre-loads its seed into a
/// `DefaultHasher` so distinct seeds yield independent hash streams while
/// staying stable across processes and runs (unlike `RandomState`, which
/// reseeds from process entropy on every construction).
#[derive(Clone, Debug)]
pub struct SeededState(u64);

impl SeededState {
    #[inline]
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }
}

impl BuildHasher for SeededState {
    type Hasher = DefaultHasher;

    #[inline]
    fn build_hasher(&self) -> DefaultHasher {
        let mut h = DefaultHasher::new();
        h.write_u64(self.0);
        h
    }
}

/// splitmix64 finalizer — spreads a counter into a well-distributed u64 so
/// that consecutive slice indices yield uncorrelated seeds.
#[inline]
fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Deterministic pair of seeded hashers for slice `index`. A rateless Bloom
/// filter relies on each slice using *independent* hash functions so that
/// successive slices catch different false positives; with `RandomState`
/// that independence came from per-instance randomness. Deriving the two
/// seeds from the slice index reproduces that independence while keeping the
/// whole construction reproducible across runs. Slice 0 is the canonical
/// pair used by single-shot (non-rateless) filters.
#[inline]
pub fn seeded_hashers(index: u64) -> [SeededState; 2] {
    [
        SeededState::new(splitmix64(BLOOM_SEED_0 ^ (index << 1))),
        SeededState::new(splitmix64(BLOOM_SEED_1 ^ ((index << 1) | 1))),
    ]
}

#[inline]
fn default_bloom_hashers() -> [SeededState; 2] {
    seeded_hashers(0)
}

#[derive(Debug)]
pub struct BloomFilter<T: ?Sized> {
    base: BitVec,
    hashers: [SeededState; 2],
    hashes: u64,
    _marker: PhantomData<T>,
    t_enc: Duration,
    t_dec: Duration,
}

impl<T> BloomFilter<T>
where
    T: ?Sized,
{
    #[inline]
    #[must_use]
    pub fn new(capacity: usize, fpr: f64) -> Self {
        assert!(
            (0.0..1.0).contains(&fpr) && fpr > 0.0,
            "false positive rate should be in (0, 1)"
        );

        let m = (-(capacity as f64) * fpr.ln() / (LN_2 * LN_2)).ceil() as usize;
        let k = (-fpr.ln() / LN_2).ceil() as u64;

        Self {
            base: bitvec![0; max(m, 1)],
            hashers: default_bloom_hashers(),
            hashes: k,
            _marker: PhantomData,
            t_enc: Duration::ZERO,
            t_dec: Duration::ZERO,
        }
    }

    #[inline]
    #[must_use]
    pub fn from_raw_parts(m: usize, k: u64) -> Self {
        assert!(m > 0 && k > 0, "m and k should be positive");

        Self {
            base: bitvec![0; max(m, 1)],
            hashers: default_bloom_hashers(),
            hashes: k,
            _marker: PhantomData,
            t_enc: Duration::ZERO,
            t_dec: Duration::ZERO,
        }
    }

    #[inline]
    #[must_use]
    pub fn from_raw_parts_with_hashers(m: usize, k: u64, hashers: [SeededState; 2]) -> Self {
        assert!(m > 0 && k > 0, "m and k should be positive");

        Self {
            base: bitvec![0; m],
            hashers,
            hashes: k,
            _marker: PhantomData,
            t_enc: Duration::ZERO,
            t_dec: Duration::ZERO,
        }
    }

    #[inline]
    pub fn bitslice(&self) -> &BitSlice {
        &self.base
    }

    #[inline]
    pub fn hashers(&self) -> [SeededState; 2] {
        self.hashers.clone()
    }

    #[inline]
    pub fn bit_len(&self) -> usize {
        self.base.len()
    }

    #[inline]
    pub fn byte_len(&self) -> usize {
        self.base.len().div_ceil(8)
    }

    #[inline]
    pub fn hashes(&self) -> u64 {
        self.hashes
    }

    #[inline]
    pub fn t_enc(&self) -> Duration {
        self.t_enc
    }

    #[inline]
    pub fn t_dec(&self) -> Duration {
        self.t_dec
    }
}

impl<T> BloomFilter<T>
where
    T: ?Sized + Hash,
{
    #[inline]
    pub fn timed_contains(&mut self, value: &T) -> bool {
        let start = Instant::now();
        let contains = self.contains(value);
        self.t_dec += start.elapsed();
        contains
    }

    #[inline]
    pub fn contains(&self, value: &T) -> bool {
        let h = (
            self.hashers[0].hash_one(value),
            self.hashers[1].hash_one(value),
        );

        (0..self.hashes).all(|i| {
            let bit =
                usize::try_from(h.0.wrapping_add(i.wrapping_mul(h.1))).unwrap() % self.base.len();
            self.base[bit]
        })
    }

    #[inline]
    pub fn timed_insert(&mut self, value: &T) {
        let start = Instant::now();
        self.insert(value);
        self.t_enc += start.elapsed();
    }

    #[inline]
    pub fn insert(&mut self, value: &T) {
        let h = (
            self.hashers[0].hash_one(value),
            self.hashers[1].hash_one(value),
        );

        (0..self.hashes).for_each(|i| {
            let bit =
                usize::try_from(h.0.wrapping_add(i.wrapping_mul(h.1))).unwrap() % self.base.len();
            self.base.set(bit, true);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom_filter_membership_works() {
        let mut bloom = BloomFilter::new(100, 0.01);

        assert!(!bloom.contains("1"));
        assert!(!bloom.contains("2"));

        bloom.insert("1");

        assert!(bloom.contains("1"));
    }

    #[test]
    fn bloom_filter_raw_parts_have_expected_sizes() {
        let bloom: BloomFilter<u64> = BloomFilter::from_raw_parts(128, 3);

        assert_eq!(bloom.bit_len(), 128);
        assert_eq!(bloom.byte_len(), 16);
        assert_eq!(bloom.hashes(), 3);
    }
}
