use std::collections::HashSet;
use std::mem;
use std::time::Duration;

use crate::simulator::algorithms::rateless_bloom::bayesian_cost::RATELESS_SET_RECONCILIATION_OVERHEAD;
use crate::simulator::algorithms::rateless_bloom::expected_cost::ExpectedCostFactory;
use crate::simulator::algorithms::rateless_bloom::{RatelessBF, StoppingStrategyFactory};
use crate::simulator::algorithms::riblt::RatelessIBLT;
use crate::simulator::protocols::{Protocol, ProtocolKind, ProtocolMetrics, ProtocolStepResult};
use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::Topology;

/// Hybrid protocol:
/// 1. Use a Rateless Bloom Filter to cheaply eliminate remote elements that are
///    definitely not present locally.
/// 2. Use a Rateless IBLT only on the remaining ambiguous elements to recover the
///    exact remote-only differences.
#[derive(Clone, Debug)]
pub struct HybridRbfRibltProtocol {
    /// Requested number of Bloom bits per element, expressed as a ratio.
    /// The effective Bloom size is clamped to a minimum large enough for the
    /// expected-cost stopping rule to produce a nonzero threshold.
    m_ratio: f64,
}

impl Default for HybridRbfRibltProtocol {
    /// Creates the default hybrid protocol configuration.
    fn default() -> Self {
        Self::new()
    }
}

impl HybridRbfRibltProtocol {
    /// Creates a hybrid protocol with a default Bloom bit ratio.
    pub fn new() -> Self {
        Self { m_ratio: 0.5 }
    }

    /// Creates a hybrid protocol with a user-defined Bloom bit ratio.
    pub fn with_m_ratio(m_ratio: f64) -> Self {
        assert!(m_ratio > 0.0, "m_ratio must be positive");
        Self { m_ratio }
    }

    /// Computes the number of bits used by each Bloom slice.
    ///
    /// We enforce a minimum size so that the expected-cost stopping rule has a
    /// nonzero reconciliation threshold. Otherwise, for tiny test inputs, the
    /// threshold may become zero and the Bloom phase may never stop.
    fn bloom_bits_for(&self, n: usize) -> usize {
        let requested = ((n as f64) * self.m_ratio).ceil().max(1.0) as usize;
        let minimum_for_nonzero_threshold = RATELESS_SET_RECONCILIATION_OVERHEAD * 8;
        requested.max(minimum_for_nonzero_threshold)
    }

    /// Converts an effective Bloom bit count back into the ratio expected by the
    /// old `ExpectedCostFactory` API.
    ///
    /// This keeps the stopping strategy consistent with the actual Bloom slice
    /// size we are using in the protocol.
    fn effective_m_ratio(&self, bloom_bits: usize, n: usize) -> f64 {
        bloom_bits as f64 / n.max(1) as f64
    }

    /// Returns the size, in bytes, of transferring one full `Element`.
    fn element_transfer_size(element: &Element) -> usize {
        mem::size_of::<u64>() + element.payload.len()
    }
}

impl Protocol for HybridRbfRibltProtocol {
    /// Returns the simulator-facing kind of this protocol.
    fn kind(&self) -> ProtocolKind {
        ProtocolKind::HybridRbfRiblt
    }

    /// Executes one protocol step for a single replica.
    ///
    /// For each neighbor, this function:
    /// 1. Builds a Rateless Bloom Filter from the local digests.
    /// 2. Uses the `ExpectedCost` stopping strategy to partition the neighbor's
    ///    digests into:
    ///    - `remote_common`: still ambiguous / still possible positives
    ///    - `remote_definitely_missing`: ruled out by the Bloom phase
    /// 3. Immediately transfers the definitely-missing elements.
    /// 4. Runs Rateless IBLT on the ambiguous subset to recover the remaining
    ///    exact remote-only digests.
    /// 5. Recovers full `Element`s from the neighbor set and inserts them into
    ///    the next local state.
    /// 6. Aggregates metadata and timing metrics from both phases.
    fn step_replica(
        &self,
        replica_id: usize,
        replicas: &[Replica],
        topology: &Topology,
    ) -> ProtocolStepResult {
        let mut next_set = replicas[replica_id].snapshot_set();

        let local_digests = replicas[replica_id]
            .set
            .iter()
            .map(|e| e.digest)
            .collect::<Vec<_>>();

        let mut state_bytes = 0usize;
        let mut metadata_bytes = 0usize;
        let mut encode_time = Duration::ZERO;
        let mut decode_time = Duration::ZERO;
        let false_matches = 0usize;

        for &neighbor_id in topology.neighbors(replica_id) {
            let remote_elements = &replicas[neighbor_id].set;
            let remote_digests = remote_elements.iter().map(|e| e.digest).collect::<Vec<_>>();

            // Phase 1: Rateless Bloom Filter
            //
            // We pick a Bloom slice size large enough that the expected-cost
            // stopping rule has a nonzero threshold even on tiny workloads.
            let bloom_bits = self.bloom_bits_for(local_digests.len());
            let effective_m_ratio = self.effective_m_ratio(bloom_bits, local_digests.len());

            let mut local_filter = RatelessBF::new(local_digests.clone(), bloom_bits);

            let stopping_strategy = ExpectedCostFactory::new(effective_m_ratio)
                .create(remote_digests, local_digests.len());

            let (remote_common, remote_definitely_missing) =
                local_filter.extend_until(stopping_strategy);

            metadata_bytes += local_filter.size_of();
            encode_time += local_filter.t_enc();
            decode_time += local_filter.t_dec();

            // Elements ruled out by Bloom are definitely not present locally,
            // so they can be transferred immediately.
            for digest in remote_definitely_missing {
                if let Some(element) = remote_elements.iter().find(|e| e.digest == digest).cloned()
                {
                    if next_set.insert(element.clone()) {
                        state_bytes += Self::element_transfer_size(&element);
                    }
                }
            }

            // Phase 2: Rateless IBLT
            //
            // The Bloom phase leaves behind an ambiguous subset (`remote_common`).
            // We now reconcile that remaining uncertainty exactly with a Rateless IBLT.
            if !remote_common.is_empty() {
                let mut local_riblt = RatelessIBLT::riblt_from(local_digests.clone());
                let mut remote_riblt = RatelessIBLT::riblt_from(remote_common);

                let sketch_len = local_riblt.find_all_differences(&mut remote_riblt);

                metadata_bytes += sketch_len * mem::size_of::<u64>();
                encode_time += local_riblt.t_enc();
                decode_time += local_riblt.t_dec();

                let remote_only = local_riblt.get_remote_only_symbols();

                for digest in remote_only {
                    if let Some(element) =
                        remote_elements.iter().find(|e| e.digest == digest).cloned()
                    {
                        if next_set.insert(element.clone()) {
                            state_bytes += Self::element_transfer_size(&element);
                        }
                    }
                }
            }
        }

        ProtocolStepResult {
            next_set,
            metrics: ProtocolMetrics {
                state_bytes,
                metadata_bytes,
                encode_time,
                decode_time,
                false_matches,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_element(digest: u64, payload_byte: u8, payload_len: usize) -> Element {
        Element::new(digest, vec![payload_byte; payload_len])
    }

    fn make_replica(id: usize, digests: &[u64]) -> Replica {
        let set = digests
            .iter()
            .map(|&d| make_element(d, d as u8, 4))
            .collect::<HashSet<_>>();
        Replica::new(id, set)
    }

    #[test]
    fn hybrid_step_replica_learns_missing_neighbor_elements() {
        let protocol = HybridRbfRibltProtocol::new();
        let topology = Topology::star(2);

        let replicas = vec![make_replica(0, &[1, 2, 3]), make_replica(1, &[2, 3, 4])];

        let result = protocol.step_replica(0, &replicas, &topology);
        let digests = result
            .next_set
            .iter()
            .map(|e| e.digest)
            .collect::<HashSet<_>>();

        assert!(digests.contains(&1));
        assert!(digests.contains(&2));
        assert!(digests.contains(&3));
        assert!(digests.contains(&4));
        assert_eq!(digests.len(), 4);
        assert!(result.metrics.metadata_bytes > 0);
    }

    #[test]
    fn hybrid_step_replica_keeps_existing_local_elements() {
        let protocol = HybridRbfRibltProtocol::new();
        let topology = Topology::star(2);

        let replicas = vec![make_replica(0, &[10, 20]), make_replica(1, &[20, 30])];

        let result = protocol.step_replica(0, &replicas, &topology);
        let digests = result
            .next_set
            .iter()
            .map(|e| e.digest)
            .collect::<HashSet<_>>();

        assert!(digests.contains(&10));
        assert!(digests.contains(&20));
        assert!(digests.contains(&30));
    }

    #[test]
    fn hybrid_step_replica_with_identical_neighbors_is_unchanged() {
        let protocol = HybridRbfRibltProtocol::new();
        let topology = Topology::star(2);

        let replicas = vec![make_replica(0, &[5, 6, 7]), make_replica(1, &[5, 6, 7])];

        let before = replicas[0].snapshot_set();
        let after = protocol.step_replica(0, &replicas, &topology).next_set;

        assert_eq!(before, after);
    }
}
