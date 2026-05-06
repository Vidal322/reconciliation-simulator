use reconciliation_simulator::simulator;

use simulator::engine::{Simulation, SimulationConfig, SimulationResult};
use simulator::export::{RunSummaryRow, append_run_summary_csv};
use simulator::protocols::ProtocolKind;
use simulator::topology::TopologyKind;
use simulator::workload::{DivergencePattern, WorkloadConfig};

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
        "State bytes sent:    {}",
        result.metrics.total_state_bytes_sent
    );
    println!(
        "Metadata bytes sent: {}",
        result.metrics.total_metadata_bytes_sent
    );
    println!(
        "Encode time:         {:?}",
        result.metrics.total_encode_time
    );
    println!(
        "Decode time:         {:?}",
        result.metrics.total_decode_time
    );
    println!(
        "Elements added:      {}",
        result.metrics.total_elements_added
    );

    let total_sent =
        result.metrics.total_state_bytes_sent + result.metrics.total_metadata_bytes_sent;

    println!("Total bytes sent:    {}", total_sent);

    println!("\n--- Per-node Metrics ---");
    for node in &result.metrics.per_node {
        println!(
            "Replica {} -> state_sent={}, meta_sent={}, enc={:?}, dec={:?}, added={}",
            node.replica_id,
            node.state_bytes_sent,
            node.metadata_bytes_sent,
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
    let protocols = [
        ProtocolKind::FullStateTransfer,
        ProtocolKind::Riblt,
        ProtocolKind::StaticBfIblt,
        ProtocolKind::HybridRbfRiblt,
        ProtocolKind::MultiReplica,
    ];

    let topologies = [TopologyKind::Star, TopologyKind::Tree, TopologyKind::Chord];

    for protocol in protocols {
        for topology in topologies {
            let workload = WorkloadConfig {
                num_replicas: 32,
                set_size: 10_000,
                payload_size: 32,
                digest_bits: 64,
                jaccard_similarity: 0.5,
                pattern: DivergencePattern::Uniform,
                seed: 42,
                universe_size: 200_000,
                zipf_exponent: 1.0,
                cluster_count: None,
                jaccard_inter: None,
                jaccard_intra: None,
            };

            let config = SimulationConfig {
                round_cap: 100,
                seed: 42,
                topology,
                protocol,
                workload,
            };

            let mut simulation = Simulation::new(config.clone());
            let result = simulation.run();

            let row = RunSummaryRow::from_run(&config, &result, simulation.target_union().len());
            append_run_summary_csv("results.csv", &row).expect("failed to write CSV");

            print_human(&result, &simulation);
        }
    }
}
