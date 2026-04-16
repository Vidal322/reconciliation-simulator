#![allow(unused)]
mod simulator;

use simulator::engine::{Simulation, SimulationConfig, SimulationResult};
use simulator::export::{RunSummaryRow, append_run_summary_csv};
use simulator::protocols::ProtocolKind;
use simulator::topology::TopologyKind;
use simulator::workload::{DivergencePattern, WorkloadConfig};

fn parse_protocol(name: &str) -> ProtocolKind {
    match name {
        "FullStateTransfer" => ProtocolKind::FullStateTransfer,
        "Riblt" => ProtocolKind::Riblt,
        "StaticBfIblt" => ProtocolKind::StaticBfIblt,
        "HybridRbfRiblt" => ProtocolKind::HybridRbfRiblt,
        "MultiReplicaV2" => ProtocolKind::MultiReplicaV2,
        _ => {
            eprintln!("Unknown protocol: {name}");
            eprintln!(
                "Available: FullStateTransfer, Riblt, StaticBfIblt, HybridRbfRiblt, MultiReplicaV2"
            );
            std::process::exit(1);
        }
    }
}

struct CliArgs {
    protocol_filter: Option<ProtocolKind>,
    json: bool,
}

fn parse_args() -> CliArgs {
    let args: Vec<String> = std::env::args().collect();
    let mut protocol_filter = None;
    let mut json = false;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--protocol" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("--protocol requires a value");
                    std::process::exit(1);
                }
                protocol_filter = Some(parse_protocol(&args[i]));
            }
            "--json" => {
                json = true;
            }
            other => {
                eprintln!("Unknown argument: {other}");
                std::process::exit(1);
            }
        }
        i += 1;
    }
    CliArgs {
        protocol_filter,
        json,
    }
}

fn print_json(results: &[(SimulationResult, usize, f64)]) {
    println!("{{");
    println!("  \"runs\": [");
    for (i, (result, target_union_size, jaccard_similarity)) in results.iter().enumerate() {
        let total_sent =
            result.metrics.total_state_bytes_sent + result.metrics.total_metadata_bytes_sent;
        let comma = if i + 1 < results.len() { "," } else { "" };
        println!("    {{");
        println!("      \"protocol\": \"{:?}\",", result.protocol);
        println!("      \"topology\": \"{:?}\",", result.topology);
        println!("      \"converged\": {},", result.converged);
        println!("      \"rounds\": {},", result.rounds);
        println!("      \"total_bytes_sent\": {},", total_sent);
        println!(
            "      \"total_state_bytes_sent\": {},",
            result.metrics.total_state_bytes_sent
        );
        println!(
            "      \"total_metadata_bytes_sent\": {},",
            result.metrics.total_metadata_bytes_sent
        );
        println!("      \"target_union_size\": {}", target_union_size);
        println!("      \"jaccard_similarity\": {}", jaccard_similarity);
        println!("    }}{comma}");
    }
    println!("  ]");
    println!("}}");
}

fn print_human(result: &SimulationResult, simulation: &Simulation) {
    println!("====================================================");
    println!("Protocol: {:?}", result.protocol);
    println!("Topology: {:?}", result.topology);
    println!("Converged: {}", result.converged);
    println!("Status: {:?}", result.status);
    println!("Rounds: {}", result.rounds);
    println!("Replicas: {}", result.num_replicas);
    println!("Target union size: {}", simulation.target_union().len());
    println!("Edges: {}", simulation.topology().edge_count());

    println!("\n--- Aggregate Metrics ---");
    println!(
        "State bytes sent:       {}",
        result.metrics.total_state_bytes_sent
    );
    println!(
        "State bytes received:   {}",
        result.metrics.total_state_bytes_received
    );
    println!(
        "Metadata bytes sent:    {}",
        result.metrics.total_metadata_bytes_sent
    );
    println!(
        "Metadata bytes received:{}",
        result.metrics.total_metadata_bytes_received
    );
    println!(
        "Encode time:            {:?}",
        result.metrics.total_encode_time
    );
    println!(
        "Decode time:            {:?}",
        result.metrics.total_decode_time
    );
    println!(
        "Elements added:         {}",
        result.metrics.total_elements_added
    );

    let total_sent =
        result.metrics.total_state_bytes_sent + result.metrics.total_metadata_bytes_sent;
    let total_received =
        result.metrics.total_state_bytes_received + result.metrics.total_metadata_bytes_received;

    println!("Total bytes sent:       {}", total_sent);
    println!("Total bytes received:   {}", total_received);

    println!("\n--- Sanity Expectations ---");
    match result.protocol {
        ProtocolKind::FullStateTransfer => {
            println!("Expected: high state bytes, near-zero metadata bytes.");
        }
        ProtocolKind::Riblt => {
            println!("Expected: near-zero state bytes, nonzero metadata bytes.");
        }
        ProtocolKind::StaticBfIblt => {
            println!("Expected: both metadata and some state bytes.");
        }
        ProtocolKind::HybridRbfRiblt => {
            println!(
                "Expected: metadata bytes lower than pure RIBLT in many cases, and some state bytes."
            );
        }
        ProtocolKind::MultiReplicaV2 => {
            println!("Agent-target protocol. Baseline: full state transfer.");
        }
    }

    println!("\n--- Per-node Metrics ---");
    for node in &result.metrics.per_node {
        println!(
            "Replica {} -> state_sent={}, state_recv={}, meta_sent={}, meta_recv={}, enc={:?}, dec={:?}, added={}",
            node.replica_id,
            node.state_bytes_sent,
            node.state_bytes_received,
            node.metadata_bytes_sent,
            node.metadata_bytes_received,
            node.encode_time,
            node.decode_time,
            node.elements_added
        );
    }

    println!("\n--- Final Replica Sizes ---");
    for replica in simulation.replicas() {
        println!(
            "Replica {} -> size={}, phase={:?}",
            replica.id,
            replica.len(),
            replica.phase
        );
    }

    println!();
}

fn main() {
    let cli = parse_args();

    let all_protocols = [
        ProtocolKind::FullStateTransfer,
        ProtocolKind::Riblt,
        ProtocolKind::StaticBfIblt,
        ProtocolKind::HybridRbfRiblt,
        ProtocolKind::MultiReplicaV2,
    ];

    let protocols: Vec<ProtocolKind> = match cli.protocol_filter {
        Some(p) => vec![p],
        None => all_protocols.to_vec(),
    };

    let topologies = [TopologyKind::Star, TopologyKind::Tree, TopologyKind::Chord];

    let mut json_results: Vec<(SimulationResult, usize, f64)> = Vec::new();

    for protocol in &protocols {
        for topology in topologies {
            let workload = WorkloadConfig {
                num_replicas: 8,
                set_size: 10_000,
                payload_size: 32,
                digest_bits: 64,
                jaccard_similarity: 0.818,
                pattern: DivergencePattern::Uniform,
                seed: 42,
                universe_size: 100_000,
                zipf_exponent: 1.0,
                cluster_count: None,
                jaccard_inter: None,
                jaccard_intra: None,
            };

            let config = SimulationConfig {
                round_cap: 20,
                seed: 42,
                topology,
                protocol: *protocol,
                workload,
            };

            let mut simulation = Simulation::new(config.clone());
            let result = simulation.run();

            let row = RunSummaryRow::from_run(&config, &result, simulation.target_union().len());
            append_run_summary_csv("results.csv", &row).expect("failed to write CSV");

            if cli.json {
                json_results.push((
                    result,
                    simulation.target_union().len(),
                    config.workload.jaccard_similarity,
                ));
            } else {
                print_human(&result, &simulation);
            }
        }
    }

    if cli.json {
        print_json(&json_results);
    }
}
