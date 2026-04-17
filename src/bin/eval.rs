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
    round_cap: usize,
    workload: EvalWorkload,
}

#[derive(Deserialize, Clone)]
struct EvalWorkload {
    num_replicas: usize,
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
    fn to_workload(&self, jaccard_similarity: f64) -> WorkloadConfig {
        WorkloadConfig {
            num_replicas: self.num_replicas,
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

    for &topo in &eval_config.topologies {
        for &j in &eval_config.jaccard_similarities {
            for &seed in &eval_config.seeds {
                let sim_config = SimulationConfig {
                    round_cap: eval_config.round_cap,
                    seed,
                    topology: topo,
                    protocol: eval_config.protocol,
                    workload: eval_config.workload.to_workload(j),
                };
                let mut sim = Simulation::new(sim_config);
                let result = sim.run();
                let total = result.metrics.total_state_bytes_sent
                    + result.metrics.total_metadata_bytes_sent;

                runs.push(RunEntry {
                    topology: topo,
                    jaccard_similarity: j,
                    seed,
                    converged: result.converged,
                    rounds: result.rounds,
                    total_bytes_sent: total,
                    state_bytes_sent: result.metrics.total_state_bytes_sent,
                    metadata_bytes_sent: result.metrics.total_metadata_bytes_sent,
                });
            }
        }
    }

    let by_cell: Vec<CellSummary> = eval_config
        .topologies
        .iter()
        .flat_map(|&topo| {
            eval_config
                .jaccard_similarities
                .iter()
                .map(move |&j| (topo, j))
        })
        .map(|(topo, j)| {
            let cell_runs: Vec<&RunEntry> = runs
                .iter()
                .filter(|r| r.topology == topo && (r.jaccard_similarity - j).abs() < 1e-9)
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
                mean_bytes,
                std_bytes,
                mean_rounds,
                all_converged: cell_runs.iter().all(|r| r.converged),
            }
        })
        .collect();

    let fitness: f64 = by_cell.iter().map(|c| c.mean_bytes).sum();
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
