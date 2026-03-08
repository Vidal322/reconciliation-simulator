pub struct SimulationConfig {
    pub round_cap: usize,
    pub seed: u64,
    pub topology: TopologyKind,
    pub workload: WorkloadConfig,
}

pub struct Simulation {
    config: SimulationConfig,
    replicas: Vec<Replica>,
    topology: Box<dyn Topology>,
    network: Network,
    metrics: MetricsCollector,
    target_union: HashSet<Element>,
    current_round: usize,
}

impl Simulation {
    pub fn new(config: SimulationConfig) -> Self { // }

    pub fn run(&mut self) -> SimulationResult { // }

    pub fn step(&mut self) -> RoundOutcome { // }

    fn has_converged(&self) -> bool { // }
}
