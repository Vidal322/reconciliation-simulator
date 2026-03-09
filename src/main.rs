mod simulator;

use simulator::engine::{Simulation, SimulationConfig};
use simulator::protocols::ProtocolKind;
use simulator::topology::TopologyKind;
use simulator::workload::{DivergencePattern, WorkloadConfig};

fn main() {
    let topologies = [TopologyKind::Star, TopologyKind::Tree, TopologyKind::Chord];

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
            protocol: ProtocolKind::FullStateTransfer,
            workload,
        };

        let mut simulation = Simulation::new(config);
        let result = simulation.run();

        println!("=== {:?} ===", topology);
        println!("Converged: {}", result.converged);
        println!("Status: {:?}", result.status);
        println!("Rounds: {}", result.rounds);
        println!("Replicas: {}", result.num_replicas);
        println!("Target union size: {}", simulation.target_union().len());
        println!("Protocol: {:?}", result.protocol);
        println!("Edges: {}", simulation.topology().edge_count());

        println!("\n=== Metrics ===");
        println!("Recorded rounds: {}", result.metrics.rounds);
        println!("Total bytes sent: {}", result.metrics.total_bytes_sent);
        println!(
            "Total bytes received: {}",
            result.metrics.total_bytes_received
        );
        println!("Total encode time: {:?}", result.metrics.total_encode_time);
        println!("Total decode time: {:?}", result.metrics.total_decode_time);
        println!(
            "Total elements added: {}",
            result.metrics.total_elements_added
        );

        println!("\nPer-node metrics:");
        for node in &result.metrics.per_node {
            println!(
                "  Replica {} -> bytes_sent={}, bytes_received={}, encode_time={:?}, decode_time={:?}, elements_added={}",
                node.replica_id,
                node.bytes_sent,
                node.bytes_received,
                node.encode_time,
                node.decode_time,
                node.elements_added
            );
        }

        println!("\nFinal replica sizes:");
        for replica in simulation.replicas() {
            println!(
                "  Replica {} -> size={}, phase={:?}",
                replica.id,
                replica.len(),
                replica.phase
            );
        }

        println!();
    }
}
