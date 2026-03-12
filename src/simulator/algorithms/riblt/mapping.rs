use rand::{RngExt, SeedableRng, rngs::StdRng};

#[derive(Debug)]
pub struct SymbolMapping {
    rng: StdRng,
    last_mapped_idx: usize,
}

impl SymbolMapping {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            last_mapped_idx: 0,
        }
    }
}

impl Iterator for SymbolMapping {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let r: f64 = self.rng.random_range(0.0..1.0);

        let i = self.last_mapped_idx as f64;
        let u_sqrt_inv = 1.0 / (1.0 - r).sqrt();
        let diff = ((1.5 + i) * (u_sqrt_inv - 1.0)).ceil() as usize;

        let index_to_return = self.last_mapped_idx;
        self.last_mapped_idx += diff;

        Some(index_to_return)
    }
}
