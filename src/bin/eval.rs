use std::fs;

use reconciliation_simulator::simulator::engine::{Simulation, SimulationConfig};
use reconciliation_simulator::simulator::protocols::ProtocolKind;
use reconciliation_simulator::simulator::topology::TopologyKind;
use reconciliation_simulator::simulator::workload::WorkloadConfig;

use serde::{Deserialize, Serialize};

// --- TOML input config ---

#[derive(Deserialize)]
struct EvalConfig {
    protocol: ProtocolKind,
    topologies: Vec<TopologyKind>,
    seeds: Vec<u64>,
    round_cap: usize,
    workload: WorkloadConfig,
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
    seed: u64,
    converged: bool,
    rounds: usize,
    total_bytes_sent: usize,
    state_bytes_sent: usize,
    metadata_bytes_sent: usize,
}

#[derive(Serialize)]
struct TopologySummary {
    topology: TopologyKind,
    mean_bytes: f64,
    std_bytes: f64,
    mean_rounds: f64,
    all_converged: bool,
}

#[derive(Serialize)]
struct Summary {
    by_topology: Vec<TopologySummary>,
    fitness: f64,
    all_converged: bool,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    // Expect: eval --config <path>
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
        for &seed in &eval_config.seeds {
            let sim_config = SimulationConfig {
                round_cap: eval_config.round_cap,
                seed: seed,
                topology: topo,
                protocol: eval_config.protocol,
                workload: eval_config.workload.clone(),
            };
            let mut sim = Simulation::new(sim_config);
            let result = sim.run();
            let total =
                result.metrics.total_state_bytes_sent + result.metrics.total_metadata_bytes_sent;

            runs.push(RunEntry {
                topology: topo,
                seed: seed,
                converged: result.converged,
                rounds: result.rounds,
                total_bytes_sent: total,
                state_bytes_sent: result.metrics.total_state_bytes_sent,
                metadata_bytes_sent: result.metrics.total_metadata_bytes_sent,
            });
        }
    }

    // Build summary
    let by_topology: Vec<TopologySummary> = eval_config
        .topologies
        .iter()
        .map(|&topo| {
            let topo_runs: Vec<&RunEntry> = runs.iter().filter(|r| r.topology == topo).collect();
            let n = topo_runs.len() as f64;
            let mean_bytes = topo_runs
                .iter()
                .map(|r| r.total_bytes_sent as f64)
                .sum::<f64>()
                / n;
            let mean_rounds = topo_runs.iter().map(|r| r.rounds as f64).sum::<f64>() / n;
            let variance = topo_runs
                .iter()
                .map(|r| {
                    let diff = r.total_bytes_sent as f64 - mean_bytes;
                    diff * diff
                })
                .sum::<f64>()
                / n;
            let std_bytes = variance.sqrt();
            TopologySummary {
                topology: topo,
                mean_bytes,
                std_bytes,
                mean_rounds,
                all_converged: topo_runs.iter().all(|r| r.converged),
            }
        })
        .collect();

    let fitness: f64 = by_topology.iter().map(|t| t.mean_bytes).sum();
    let all_converged = by_topology.iter().all(|t| t.all_converged);

    let output = EvalOutput {
        runs,
        summary: Summary {
            by_topology,
            fitness,
            all_converged,
        },
    };

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}

fn parse_config_path(args: &[String]) -> String {
    // Simple: eval --config <path>
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
