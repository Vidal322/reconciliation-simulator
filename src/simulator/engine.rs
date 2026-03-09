use crate::simulator::replica::{Element, Replica};
use crate::simulator::topology::{Topology, TopologyKind};
use crate::simulator::workload::WorkloadConfig;

use std::collections::HashSet;

pub struct SimulationConfig {
    pub round_cap: usize,
    pub seed: u64,
    pub topology: TopologyKind,
    pub workload: WorkloadConfig,
}

pub struct Simulation {
    config: SimulationConfig,
    replicas: Vec<Replica>,
    topology: Box<Topology>,
    //network: Network,
    //    metrics: MetricsCollector,
    target_union: HashSet<Element>,
    current_round: usize,
}
/*
impl Simulation {
    pub fn new(config: SimulationConfig) -> Self { ... }

    pub fn run(&mut self) -> SimulationResult { ... }

    pub fn step(&mut self) -> RoundOutcome { ... }

    fn has_converged(&self) -> bool { ... }
}
*/
