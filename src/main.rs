mod simulator;

use simulator::engine::{Simulation, SimulationConfig};
use simulator::export::{RunSummaryRow, append_run_summary_csv};
use simulator::protocols::ProtocolKind;
use simulator::topology::TopologyKind;
use simulator::workload::{DivergencePattern, WorkloadConfig};

fn main() {
    let protocols = [
        ProtocolKind::FullStateTransfer,
        ProtocolKind::Riblt,
        ProtocolKind::StaticBfIblt,
        ProtocolKind::HybridRbfRiblt,
        ProtocolKind::MultiReplicaV2,
    ];

    let topologies = [TopologyKind::Star, TopologyKind::Tree, TopologyKind::Chord];

    for protocol in protocols {
        for topology in topologies {
            let workload = WorkloadConfig {
                num_replicas: 8,
                set_size: 10_000,
                payload_size: 32,
                digest_bits: 64,
                divergence: 0.1,
                pattern: DivergencePattern::Uniform,
                seed: 42,
                universe_size: 100_000,
                zipf_exponent: 1.0,
                cluster_count: None,
                inter_cluster_divergence: None,
                intra_cluster_divergence: None,
            };

            let config = SimulationConfig {
                round_cap: 20,
                seed: 42,
                topology,
                protocol,
                workload,
            };

            let mut simulation = Simulation::new(config.clone());
            let result = simulation.run();

            let row = RunSummaryRow::from_run(&config, &result, simulation.target_union().len());
            append_run_summary_csv("results.csv", &row).expect("failed to write CSV");

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
            let total_received = result.metrics.total_state_bytes_received
                + result.metrics.total_metadata_bytes_received;

            println!("Total bytes sent:       {}", total_sent);
            println!("Total bytes received:   {}", total_received);

            println!("\n--- Sanity Expectations ---");
            match protocol {
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
    }
}
