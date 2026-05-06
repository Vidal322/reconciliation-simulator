use std::fs;

use reconciliation_simulator::simulator::engine::{Simulation, SimulationConfig};
use reconciliation_simulator::simulator::protocols::ProtocolKind;
use reconciliation_simulator::simulator::topology::TopologyKind;
use reconciliation_simulator::simulator::workload::{DivergencePattern, WorkloadConfig};

use serde::{Deserialize, Serialize};

// --- TOML input config ---

#[derive(Deserialize)]
struct EvalConfig {
    protocol: ProtocolKind,
    topologies: Vec<TopologyKind>,
    seeds: Vec<u64>,
    jaccard_similarities: Vec<f64>,
    num_replicas: Vec<usize>,
    round_cap: usize,
    workload: EvalWorkload,
}

#[derive(Deserialize, Clone)]
struct EvalWorkload {
    set_size: usize,
    payload_size: usize,
    digest_bits: usize,
    pattern: DivergencePattern,
    universe_size: usize,
    zipf_exponent: f64,
    seed: u64,
    cluster_count: Option<usize>,
    jaccard_inter: Option<f64>,
    jaccard_intra: Option<f64>,
}

impl EvalWorkload {
    fn to_workload(&self, jaccard_similarity: f64, num_replicas: usize) -> WorkloadConfig {
        WorkloadConfig {
            num_replicas,
            set_size: self.set_size,
            payload_size: self.payload_size,
            digest_bits: self.digest_bits,
            jaccard_similarity,
            pattern: self.pattern,
            seed: self.seed,
            universe_size: self.universe_size,
            zipf_exponent: self.zipf_exponent,
            cluster_count: self.cluster_count,
            jaccard_inter: self.jaccard_inter,
            jaccard_intra: self.jaccard_intra,
        }
    }
}

// --- JSON output types ---

#[derive(Serialize)]
struct EvalOutput {
    runs: Vec<RunEntry>,
    summary: Summary,
}

#[derive(Serialize)]
struct RunEntry {
    topology: TopologyKind,
    jaccard_similarity: f64,
    num_replicas: usize,
    seed: u64,
    converged: bool,
    rounds: usize,
    total_bytes_sent: usize,
    state_bytes_sent: usize,
    metadata_bytes_sent: usize,
}

#[derive(Serialize)]
struct CellSummary {
    topology: TopologyKind,
    jaccard_similarity: f64,
    num_replicas: usize,
    mean_bytes: f64,
    std_bytes: f64,
    mean_rounds: f64,
    all_converged: bool,
}

#[derive(Serialize)]
struct Summary {
    by_cell: Vec<CellSummary>,
    fitness: f64,
    all_converged: bool,
}

fn create_run_entry(
    eval_config: &EvalConfig,
    &topo: &TopologyKind,
    jaccard_index: f64,
    num_replicas: usize,
    seed: u64,
) -> RunEntry {
    let sim_config = SimulationConfig {
        round_cap: eval_config.round_cap,
        seed,
        topology: topo,
        protocol: eval_config.protocol,
        workload: eval_config
            .workload
            .to_workload(jaccard_index, num_replicas),
    };
    let mut sim = Simulation::new(sim_config);
    let result = sim.run();

    RunEntry {
        topology: topo,
        jaccard_similarity: jaccard_index,
        num_replicas,
        seed,
        converged: result.converged,
        rounds: result.rounds,
        total_bytes_sent: result.metrics.total_bytes_sent(),
        state_bytes_sent: result.metrics.total_state_bytes_sent,
        metadata_bytes_sent: result.metrics.total_metadata_bytes_sent,
    }
}

fn build_cells(
    runs: &[RunEntry],
    topologies: &[TopologyKind],
    jaccard_similarities: &[f64],
    num_replicas: &[usize],
) -> Vec<CellSummary> {
    topologies
        .iter()
        .flat_map(|&topo| {
            jaccard_similarities
                .iter()
                .flat_map(move |&j| num_replicas.iter().map(move |&nr| (topo, j, nr)))
        })
        .map(|(topo, j, nr)| {
            let cell_runs: Vec<&RunEntry> = runs
                .iter()
                .filter(|r| {
                    r.num_replicas == nr
                        && r.topology == topo
                        && (r.jaccard_similarity - j).abs() < 1e-9
                })
                .collect();
            let n = cell_runs.len() as f64;
            let mean_bytes = cell_runs
                .iter()
                .map(|r| r.total_bytes_sent as f64)
                .sum::<f64>()
                / n;
            let mean_rounds = cell_runs.iter().map(|r| r.rounds as f64).sum::<f64>() / n;
            let variance = cell_runs
                .iter()
                .map(|r| {
                    let diff = r.total_bytes_sent as f64 - mean_bytes;
                    diff * diff
                })
                .sum::<f64>()
                / n;
            let std_bytes = variance.sqrt();
            CellSummary {
                topology: topo,
                jaccard_similarity: j,
                num_replicas: nr,
                mean_bytes,
                std_bytes,
                mean_rounds,
                all_converged: cell_runs.iter().all(|r| r.converged),
            }
        })
        .collect()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let config_path = parse_config_path(&args);
    let toml_str = fs::read_to_string(&config_path).unwrap_or_else(|e| {
        eprintln!("Failed to read {config_path}: {e}");
        std::process::exit(1);
    });
    let eval_config: EvalConfig = toml::from_str(&toml_str).unwrap_or_else(|e| {
        eprintln!("Failed to parse TOML: {e}");
        std::process::exit(1);
    });

    let mut runs = Vec::new();

    for &num_replicas in &eval_config.num_replicas {
        for topo in &eval_config.topologies {
            for &j in &eval_config.jaccard_similarities {
                for &seed in &eval_config.seeds {
                    runs.push(create_run_entry(&eval_config, topo, j, num_replicas, seed))
                }
            }
        }
    }

    let js = &eval_config.jaccard_similarities;
    let ns = &eval_config.num_replicas;
    let topos = &eval_config.topologies;

    let by_cell: Vec<CellSummary> = build_cells(&runs, topos, js, ns);
    let fitness: f64 = geometric_mean(by_cell.iter().map(|c| c.mean_bytes));
    let all_converged = by_cell.iter().all(|c| c.all_converged);

    let output = EvalOutput {
        runs,
        summary: Summary {
            by_cell,
            fitness,
            all_converged,
        },
    };

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}

fn geometric_mean(values: impl IntoIterator<Item = f64>) -> f64 {
    let mut n = 0usize;
    let mut log_sum = 0.0f64;
    for v in values {
        assert!(v > 0.0, "geometric_mean: non-positive value {v}");
        log_sum += v.ln();
        n += 1;
    }
    assert!(n > 0, "geometric_mean: empty input");
    (log_sum / n as f64).exp()
}

fn parse_config_path(args: &[String]) -> String {
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--config" {
            i += 1;
            if i < args.len() {
                return args[i].clone();
            }
            eprintln!("--config requires a path");
            std::process::exit(1);
        }
        i += 1;
    }
    eprintln!("Usage: eval --config <path.toml>");
    std::process::exit(1);
}
