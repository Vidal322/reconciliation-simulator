use super::bloom::BloomFilter;
use std::{
    cmp::max,
    hash::{Hash, RandomState},
    mem,
    time::{Duration, Instant},
};

pub mod angle_heuristic;
pub mod bayesian_cost;
pub mod bayesian_similarity;
pub mod expected_cost;

pub trait StoppingStrategyFactory<T: Hash> {
    type Strategy: StoppingStrategy<T>;

    fn create(&self, elements: Vec<T>, sample_size: usize) -> Self::Strategy;
    fn print_name(&self) -> String;
    fn print_params(&self) -> String;
}

pub trait StoppingStrategy<T: Hash> {
    fn on_extend(&mut self, bf: &mut RatelessBF<T>);
    fn should_stop(&mut self, bf: &mut RatelessBF<T>) -> Option<(Vec<T>, Vec<T>)>;
}

#[derive(Debug)]
pub struct RatelessBF<T: Hash> {
    bloom_filters: Vec<BloomFilter<T>>,
    data: Vec<T>,
    m: usize,
    t_enc: Duration,
    t_dec: Duration,
}

impl<T> RatelessBF<T>
where
    T: Hash,
{
    #[inline]
    #[must_use]
    pub fn new(data: Vec<T>, m: usize) -> Self {
        Self {
            bloom_filters: Vec::new(),
            data,
            m: max(m, 1),
            t_enc: Duration::ZERO,
            t_dec: Duration::ZERO,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.bloom_filters.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bloom_filters.is_empty()
    }

    #[inline]
    pub fn data(&self) -> &[T] {
        &self.data
    }

    #[inline]
    pub fn filters(&self) -> &[BloomFilter<T>] {
        &self.bloom_filters
    }

    #[inline]
    pub fn bits_per_filter(&self) -> usize {
        self.m
    }

    pub fn extend(&mut self) {
        let mut filter = BloomFilter::from_raw_parts(self.m, 1);
        self.data.iter().for_each(|d| filter.insert(d));
        self.bloom_filters.push(filter);
    }

    pub fn extend_with_hashers(&mut self, hashers: [RandomState; 2]) {
        let mut filter = BloomFilter::from_raw_parts_with_hashers(self.m, 1, hashers);
        self.data.iter().for_each(|d| filter.insert(d));
        self.bloom_filters.push(filter);
    }

    pub fn contains(&self, value: &T) -> bool {
        self.bloom_filters
            .iter()
            .all(|filter| filter.contains(value))
    }

    pub fn extend_until<S: StoppingStrategy<T>>(&mut self, mut strategy: S) -> (Vec<T>, Vec<T>) {
        loop {
            let start = Instant::now();
            self.extend();
            self.t_enc += start.elapsed();

            let start = Instant::now();
            strategy.on_extend(self);
            if let Some(partitioned_elements) = strategy.should_stop(self) {
                self.t_dec += start.elapsed();
                return partitioned_elements;
            }
            self.t_dec += start.elapsed();
        }
    }

    pub fn on_extend<S: StoppingStrategy<T>>(&mut self, mut strategy: S) {
        strategy.on_extend(self);
    }

    #[inline]
    pub fn size_of(&self) -> usize {
        if self.bloom_filters.is_empty() {
            return 0;
        }

        let standalone_bf = &self.bloom_filters[0];
        let standalone_bf_size = standalone_bf.bitslice().chunks(8).count();

        self.bloom_filters.len() * standalone_bf_size + mem::size_of::<u64>()
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

#[cfg(test)]
mod tests {
    use super::*;

    struct StopAfterN {
        n: usize,
    }

    impl StoppingStrategy<u64> for StopAfterN {
        fn on_extend(&mut self, _bf: &mut RatelessBF<u64>) {}

        fn should_stop(&mut self, bf: &mut RatelessBF<u64>) -> Option<(Vec<u64>, Vec<u64>)> {
            if bf.len() >= self.n {
                Some((bf.data().to_vec(), Vec::new()))
            } else {
                None
            }
        }
    }

    #[test]
    fn rateless_bf_extend_grows_number_of_filters() {
        let mut bf = RatelessBF::new(vec![1u64, 2, 3], 128);

        assert_eq!(bf.len(), 0);
        bf.extend();
        assert_eq!(bf.len(), 1);
        bf.extend();
        assert_eq!(bf.len(), 2);
    }

    #[test]
    fn rateless_bf_contains_requires_all_filters_to_match() {
        let mut bf = RatelessBF::new(vec![1u64, 2, 3], 128);
        bf.extend();
        bf.extend();

        assert!(bf.contains(&1));
    }

    #[test]
    fn rateless_bf_size_of_is_nonzero_after_extend() {
        let mut bf = RatelessBF::new(vec![1u64, 2, 3], 128);
        assert_eq!(bf.size_of(), 0);

        bf.extend();

        assert!(bf.size_of() > 0);
    }

    #[test]
    fn rateless_bf_extend_until_stops() {
        let mut bf = RatelessBF::new(vec![1u64, 2, 3], 128);
        let strategy = StopAfterN { n: 3 };

        let (positives, negatives) = bf.extend_until(strategy);

        assert_eq!(bf.len(), 3);
        assert_eq!(positives, vec![1, 2, 3]);
        assert!(negatives.is_empty());
    }
}
