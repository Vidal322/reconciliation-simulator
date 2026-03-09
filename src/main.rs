mod simulator;

use simulator::engine::Simulation;
use simulator::engine::SimulationConfig;
use simulator::topology::TopologyKind;
use simulator::workload::{DivergencePattern, WorkloadConfig};

fn main() {
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
        topology: TopologyKind::Tree,
        workload,
    };

    let mut simulation = Simulation::new(config);
    let result = simulation.run();

    println!("=== Simulation Result ===");
    println!("Converged: {}", result.converged);
    println!("Status: {:?}", result.status);
    println!("Rounds: {}", result.rounds);
    println!("Replicas: {}", result.num_replicas);
    println!("Topology: {:?}", result.topology);
    println!("Target union size: {}", simulation.target_union().len());
    println!("Edges: {}", simulation.topology().edge_count());

    println!("\n=== Final Replica State Sizes ===");
    for replica in simulation.replicas() {
        println!(
            "Replica {} -> size={}, phase={:?}",
            replica.id,
            replica.len(),
            replica.phase
        );
    }
}
