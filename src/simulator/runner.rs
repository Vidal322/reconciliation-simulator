use crate::simulator::engine::{Simulation, SimulationConfig};
use crate::simulator::export::{RunSummaryRow, append_run_summary_csv};
use crate::simulator::protocols::ProtocolKind;
use crate::simulator::topology::TopologyKind;
use crate::simulator::workload::{DivergencePattern, WorkloadConfig};

#[derive(Clone, Debug)]
pub struct BatchRunnerConfig {
    pub output_path: String,
    pub protocols: Vec<ProtocolKind>,
    pub topologies: Vec<TopologyKind>,
    pub divergences: Vec<f64>,
    pub seeds: Vec<u64>,
    pub num_replicas: usize,
    pub set_size: usize,
    pub payload_size: usize,
    pub digest_bits: usize,
    pub pattern: DivergencePattern,
    pub round_cap: usize,
    pub universe_size: usize,
    pub zipf_exponent: f64,
    pub cluster_count: Option<usize>,
    pub inter_cluster_divergence: Option<f64>,
    pub intra_cluster_divergence: Option<f64>,
}

impl BatchRunnerConfig {
    pub fn baseline(output_path: impl Into<String>) -> Self {
        Self {
            output_path: output_path.into(),
            protocols: vec![ProtocolKind::FullStateTransfer, ProtocolKind::Riblt],
            topologies: vec![TopologyKind::Star, TopologyKind::Tree, TopologyKind::Chord],
            divergences: vec![0.001, 0.01, 0.05, 0.1, 0.2],
            seeds: (0u64..10u64).collect(),
            num_replicas: 8,
            set_size: 10_000,
            payload_size: 32,
            digest_bits: 64,
            pattern: DivergencePattern::Uniform,
            round_cap: 20,
            universe_size: 100_000,
            zipf_exponent: 1.0,
            cluster_count: None,
            inter_cluster_divergence: None,
            intra_cluster_divergence: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct BatchRunSummary {
    pub total_runs: usize,
    pub converged_runs: usize,
    pub non_converged_runs: usize,
}

pub fn run_batch(config: &BatchRunnerConfig) -> std::io::Result<BatchRunSummary> {
    let mut summary = BatchRunSummary::default();
    let mut run_index = 0usize;

    for &protocol in &config.protocols {
        for &topology in &config.topologies {
            for &divergence in &config.divergences {
                for &seed in &config.seeds {
                    let workload = WorkloadConfig {
                        num_replicas: config.num_replicas,
                        set_size: config.set_size,
                        payload_size: config.payload_size,
                        digest_bits: config.digest_bits,
                        divergence,
                        pattern: config.pattern,
                        seed,
                        universe_size: config.universe_size,
                        zipf_exponent: config.zipf_exponent,
                        cluster_count: config.cluster_count,
                        inter_cluster_divergence: config.inter_cluster_divergence,
                        intra_cluster_divergence: config.intra_cluster_divergence,
                    };

                    let sim_config = SimulationConfig {
                        round_cap: config.round_cap,
                        seed,
                        topology,
                        protocol,
                        workload,
                    };

                    let mut simulation = Simulation::new(sim_config.clone());
                    let result = simulation.run();

                    let row = RunSummaryRow::from_run(
                        &sim_config,
                        &result,
                        simulation.target_union().len(),
                    );

                    append_run_summary_csv(&config.output_path, &row)?;

                    run_index += 1;
                    summary.total_runs += 1;

                    if result.converged {
                        summary.converged_runs += 1;
                    } else {
                        summary.non_converged_runs += 1;
                    }

                    println!(
                        "[{}] protocol={:?}, topology={:?}, seed={}, divergence={}, rounds={}, converged={}",
                        run_index,
                        protocol,
                        topology,
                        seed,
                        divergence,
                        result.rounds,
                        result.converged
                    );
                }
            }
        }
    }

    Ok(summary)
}
