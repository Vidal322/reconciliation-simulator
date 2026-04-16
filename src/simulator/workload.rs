use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use rand::SeedableRng;
use rand::distr::{Distribution, weighted::WeightedIndex};
use rand::rngs::StdRng;

use crate::simulator::replica::Element;

const PAYLOAD_PADDING: &'static str = "payloadPadding";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DivergencePattern {
    Uniform,
    Clustered,
}

#[derive(Clone, Debug)]
pub struct WorkloadConfig {
    pub num_replicas: usize,
    pub set_size: usize,
    pub payload_size: usize,
    pub digest_bits: usize,
    pub jaccard_similarity: f64,
    pub pattern: DivergencePattern,
    pub seed: u64,

    /// Number of candidate elements in the synthetic universe.
    /// This should be comfortably larger than `set_size`.
    pub universe_size: usize,

    /// Zipf exponent s. Larger values mean more skew.
    /// Typical values: 0.8 .. 1.4
    pub zipf_exponent: f64,

    /// Only used for clustered workloads.
    pub cluster_count: Option<usize>,

    /// Only used for clustered workloads.
    /// Expected divergence between replicas in different clusters.
    pub jaccard_inter: Option<f64>,

    /// Only used for clustered workloads.
    /// Expected divergence between replicas in the same cluster.
    pub jaccard_intra: Option<f64>,
}

#[derive(Clone, Debug)]
pub struct Workload {
    pub replica_sets: Vec<HashSet<Element>>,
    pub target_union: HashSet<Element>,
}

impl Workload {
    pub fn generate(config: &WorkloadConfig) -> Self {
        validate_config(config);

        let mut rng = StdRng::seed_from_u64(config.seed);
        let universe = build_universe(config);
        let zipf = build_zipf_sampler(config.universe_size, config.zipf_exponent);

        let replica_sets = match config.pattern {
            DivergencePattern::Uniform => generate_uniform(config, &universe, &zipf, &mut rng),
            DivergencePattern::Clustered => generate_clustered(config, &universe, &zipf, &mut rng),
        };

        let target_union = replica_sets
            .iter()
            .flat_map(|set| set.iter().cloned())
            .collect::<HashSet<_>>();

        Self {
            replica_sets,
            target_union,
        }
    }
}

fn validate_config(config: &WorkloadConfig) {
    assert!(config.num_replicas > 0, "num_replicas must be > 0");
    assert!(config.set_size > 0, "set_size must be > 0");
    assert!(
        (1..=64).contains(&config.digest_bits),
        "digest_bits must be in 1..=64"
    );
    assert!(
        (0.0..=1.0).contains(&config.jaccard_similarity),
        "jaccard_similarity must be in [0, 1]"
    );
    assert!(
        config.universe_size >= config.set_size,
        "universe_size must be >= set_size"
    );
    assert!(config.zipf_exponent > 0.0, "zipf_exponent must be > 0");

    if config.pattern == DivergencePattern::Clustered {
        let cluster_count = config
            .cluster_count
            .expect("cluster_count is required for clustered");
        let inter = config
            .jaccard_inter
            .expect("jaccard_inter is required for clustered");
        let intra = config
            .jaccard_intra
            .expect("jaccard_intra is required for clustered");

        assert!(cluster_count > 0, "cluster_count must be > 0");
        assert!(
            cluster_count <= config.num_replicas,
            "cluster_count must be <= num_replicas"
        );
        assert!(
            (0.0..=1.0).contains(&intra),
            "jaccard_intra must be in [0, 1]"
        );
        assert!(
            (0.0..=1.0).contains(&inter),
            "jaccard_inter must be in [0, 1]"
        );
        assert!(
            intra >= inter,
            "jaccard_intra must be >= jaccard_inter (same-cluster pairs are more similar)"
        );
    }
}

fn build_universe(config: &WorkloadConfig) -> Vec<Element> {
    (0..config.universe_size)
        .map(|id| make_element(id, config.payload_size, config.digest_bits))
        .collect()
}

fn make_element(id: usize, payload_size: usize, digest_bits: usize) -> Element {
    let digest = masked_digest(id as u64, digest_bits);
    let payload = deterministic_payload(id as u64, payload_size);
    Element::new(digest, payload)
}

fn deterministic_payload(id: u64, payload_size: usize) -> Vec<u8> {
    let mut seed_hasher = std::collections::hash_map::DefaultHasher::new();
    id.hash(&mut seed_hasher); // get
    PAYLOAD_PADDING.hash(&mut seed_hasher); // Domain Separator
    let seed = seed_hasher.finish();

    let mut rng = StdRng::seed_from_u64(seed);
    let mut payload = vec![0u8; payload_size];
    use rand::Rng;
    rng.fill_bytes(&mut payload);
    payload
}

fn masked_digest(value: u64, digest_bits: usize) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    let raw = hasher.finish();

    if digest_bits == 64 {
        raw
    } else {
        let mask = (1u64 << digest_bits) - 1;
        raw & mask
    }
}

fn build_zipf_sampler(universe_size: usize, exponent: f64) -> WeightedIndex<f64> {
    let weights = (1..=universe_size)
        .map(|rank| 1.0 / (rank as f64).powf(exponent))
        .collect::<Vec<_>>();

    WeightedIndex::new(weights).expect("failed to build zipf sampler")
}

pub fn jaccard_to_common_size(j: f64, set_size: usize) -> usize {
    (2.0 * set_size as f64 * j / (1.0 + j)).round() as usize
}

fn generate_uniform(
    config: &WorkloadConfig,
    universe: &[Element],
    zipf: &WeightedIndex<f64>,
    rng: &mut StdRng,
) -> Vec<HashSet<Element>> {
    let common_size = jaccard_to_common_size(config.jaccard_similarity, config.set_size);
    let unique_size = config.set_size - common_size;

    let common_set = sample_unique_zipf(universe, zipf, common_size, None, rng);

    let mut replica_sets = Vec::with_capacity(config.num_replicas);

    for _ in 0..config.num_replicas {
        let mut set = common_set.clone();
        let local_unique = sample_unique_zipf(universe, zipf, unique_size, Some(&set), rng);
        set.extend(local_unique);
        replica_sets.push(set);
    }

    replica_sets
}

fn generate_clustered(
    config: &WorkloadConfig,
    universe: &[Element],
    zipf: &WeightedIndex<f64>,
    rng: &mut StdRng,
) -> Vec<HashSet<Element>> {
    let cluster_count = config.cluster_count.unwrap();
    let inter = config.jaccard_inter.unwrap();
    let intra = config.jaccard_intra.unwrap();

    let global_common_size = jaccard_to_common_size(inter, config.set_size);
    let total_intra = jaccard_to_common_size(intra, config.set_size);
    let cluster_shared_size = total_intra.saturating_sub(global_common_size);

    let used = global_common_size + cluster_shared_size;
    let replica_unique_size = config.set_size.saturating_sub(used);

    let global_common = sample_unique_zipf(universe, zipf, global_common_size, None, rng);

    let cluster_bases = (0..cluster_count)
        .map(|_| {
            let mut base = global_common.clone();
            let cluster_shared =
                sample_unique_zipf(universe, zipf, cluster_shared_size, Some(&base), rng);
            base.extend(cluster_shared);
            base
        })
        .collect::<Vec<_>>();

    let mut replica_sets = Vec::with_capacity(config.num_replicas);

    for replica_id in 0..config.num_replicas {
        let cluster_id = replica_id % cluster_count;
        let mut set = cluster_bases[cluster_id].clone();
        let local_unique = sample_unique_zipf(universe, zipf, replica_unique_size, Some(&set), rng);
        set.extend(local_unique);
        replica_sets.push(set);
    }

    replica_sets
}

fn sample_unique_zipf(
    universe: &[Element],
    zipf: &WeightedIndex<f64>,
    count: usize,
    exclude: Option<&HashSet<Element>>,
    rng: &mut StdRng,
) -> HashSet<Element> {
    let mut result = HashSet::with_capacity(count);

    let excluded_count = exclude.map_or(0, |s| s.len());
    let available = universe.len().saturating_sub(excluded_count);

    assert!(
        count <= available,
        "cannot sample {} unique elements from only {} available",
        count,
        available
    );

    while result.len() < count {
        let idx = zipf.sample(rng);
        let candidate = universe[idx].clone();

        if let Some(excluded) = exclude {
            if excluded.contains(&candidate) {
                continue;
            }
        }

        result.insert(candidate);
    }

    result
}

// ZIPF tests

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::distr::Distribution;
    use rand::rngs::StdRng;

    fn sample_counts(universe_size: usize, exponent: f64, draws: usize) -> Vec<usize> {
        let sampler = build_zipf_sampler(universe_size, exponent);
        let mut rng = StdRng::seed_from_u64(42);
        let mut counts = vec![0usize; universe_size];

        for _ in 0..draws {
            let idx = sampler.sample(&mut rng);
            counts[idx] += 1;
        }

        counts
    }

    fn concentration_top_k(counts: &[usize], k: usize) -> f64 {
        let mut sorted = counts.to_vec();
        sorted.sort_unstable_by(|a, b| b.cmp(a));

        let top: usize = sorted.iter().take(k).sum();
        let total: usize = sorted.iter().sum();

        top as f64 / total as f64
    }

    #[test]
    fn zipf_head_is_more_frequent_than_tail() {
        let counts = sample_counts(10_000, 1.0, 200_000);

        assert!(counts[0] > counts[9], "rank 1 should exceed rank 10");
        assert!(counts[9] > counts[99], "rank 10 should exceed rank 100");
        assert!(counts[99] > counts[999], "rank 100 should exceed rank 1000");
    }

    #[test]
    fn zipf_ratios_are_reasonable_for_exponent_one() {
        let counts = sample_counts(10_000, 1.0, 400_000);

        let r1 = counts[0] as f64;
        let r10 = counts[9] as f64;
        let r100 = counts[99] as f64;
        let r1000 = counts[999] as f64;

        let ratio_1_10 = r1 / r10.max(1.0);
        let ratio_1_100 = r1 / r100.max(1.0);
        let ratio_1_1000 = r1 / r1000.max(1.0);

        assert!((8.0..=12.5).contains(&ratio_1_10));
        assert!((70.0..=130.0).contains(&ratio_1_100));
        assert!((500.0..=1500.0).contains(&ratio_1_1000));
    }

    #[test]
    fn higher_exponent_increases_head_concentration() {
        let counts_low = sample_counts(10_000, 0.8, 300_000);
        let counts_high = sample_counts(10_000, 1.2, 300_000);

        let top10_low = concentration_top_k(&counts_low, 10);
        let top10_high = concentration_top_k(&counts_high, 10);

        assert!(
            top10_high > top10_low,
            "higher exponent should increase concentration"
        );
    }

    #[test]
    fn tail_is_not_empty() {
        let counts = sample_counts(10_000, 1.0, 300_000);

        assert!(counts[999] > 0);
        assert!(counts[4999] > 0);
        assert!(counts[9999] > 0);
    }
}
