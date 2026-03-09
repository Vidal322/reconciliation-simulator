use crate::simulator::metrics::{MetricsCollector, MetricsSnapshot};
use crate::simulator::protocols::{Protocol, ProtocolKind, full_state_transfer::FullStateTransfer};
use crate::simulator::replica::{Element, Replica, ReplicaPhase};
use crate::simulator::topology::{Topology, TopologyKind};
use crate::simulator::workload::{Workload, WorkloadConfig};

use std::collections::HashSet;

#[derive(Clone, Debug)]
pub struct SimulationConfig {
    pub round_cap: usize,
    pub seed: u64,
    pub topology: TopologyKind,
    pub workload: WorkloadConfig,
    pub protocol: ProtocolKind,
}

pub struct Simulation {
    config: SimulationConfig,
    replicas: Vec<Replica>,
    topology: Box<Topology>,
    protocol: Box<dyn Protocol>,
    metrics: MetricsCollector,
    target_union: HashSet<Element>,
    current_round: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunStatus {
    Converged,
    RoundCapReached,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoundOutcome {
    Continue,
    Converged,
    RoundCapReached,
}

#[derive(Clone, Debug)]
pub struct SimulationResult {
    pub status: RunStatus,
    pub rounds: usize,
    pub num_replicas: usize,
    pub topology: TopologyKind,
    pub protocol: ProtocolKind,
    pub converged: bool,
    pub metrics: MetricsSnapshot,
}

impl Simulation {
    pub fn new(config: SimulationConfig) -> Self {
        let workload = Workload::generate(&config.workload);

        let replicas = workload
            .replica_sets
            .into_iter()
            .enumerate()
            .map(|(id, set)| Replica::new(id, set))
            .collect::<Vec<_>>();

        let topology = Box::new(Topology::build(
            config.topology,
            config.workload.num_replicas,
        ));

        let protocol: Box<dyn Protocol> = match config.protocol {
            ProtocolKind::FullStateTransfer => Box::new(FullStateTransfer::new()),
            ProtocolKind::HybridRbfRiblt => {
                panic!("HybridRbfRiblt not implemented yet")
            }
            ProtocolKind::Riblt => {
                panic!("Riblt not implemented yet")
            }
            ProtocolKind::StaticBfIblt => {
                panic!("StaticBfIblt not implemented yet")
            }
        };

        Self {
            config,
            replicas,
            topology,
            protocol,
            metrics: MetricsCollector::new(),
            target_union: workload.target_union,
            current_round: 0,
        }
    }

    pub fn run(&mut self) -> SimulationResult {
        if self.has_converged() {
            self.mark_converged();
            let metrics = self.finalize_metrics();

            return SimulationResult {
                status: RunStatus::Converged,
                rounds: self.current_round,
                num_replicas: self.replicas.len(),
                topology: self.config.topology,
                protocol: self.config.protocol,
                converged: true,
                metrics,
            };
        }

        loop {
            match self.step() {
                RoundOutcome::Continue => {}
                RoundOutcome::Converged => {
                    self.mark_converged();
                    let metrics = self.finalize_metrics();

                    return SimulationResult {
                        status: RunStatus::Converged,
                        rounds: self.current_round,
                        num_replicas: self.replicas.len(),
                        topology: self.config.topology,
                        protocol: self.config.protocol,
                        converged: true,
                        metrics,
                    };
                }
                RoundOutcome::RoundCapReached => {
                    let metrics = self.finalize_metrics();

                    return SimulationResult {
                        status: RunStatus::RoundCapReached,
                        rounds: self.current_round,
                        num_replicas: self.replicas.len(),
                        topology: self.config.topology,
                        protocol: self.config.protocol,
                        converged: false,
                        metrics,
                    };
                }
            }
        }
    }

    pub fn step(&mut self) -> RoundOutcome {
        if self.current_round >= self.config.round_cap {
            return RoundOutcome::RoundCapReached;
        }

        self.current_round += 1;
        self.mark_active();

        let next_sets = (0..self.replicas.len())
            .map(|replica_id| {
                self.protocol
                    .next_set(replica_id, &self.replicas, &self.topology)
            })
            .collect::<Vec<_>>();

        for (replica, next_set) in self.replicas.iter_mut().zip(next_sets) {
            replica.replace_set(next_set);
        }

        if self.has_converged() {
            RoundOutcome::Converged
        } else if self.current_round >= self.config.round_cap {
            RoundOutcome::RoundCapReached
        } else {
            RoundOutcome::Continue
        }
    }

    fn has_converged(&self) -> bool {
        self.replicas
            .iter()
            .all(|replica| replica.set == self.target_union)
    }

    fn finalize_metrics(&mut self) -> MetricsSnapshot {
        self.metrics.set_rounds(self.current_round);
        self.metrics.record_replicas(&self.replicas);
        self.metrics.snapshot()
    }

    fn mark_active(&mut self) {
        for replica in &mut self.replicas {
            replica.set_phase(ReplicaPhase::Active);
        }
    }

    fn mark_converged(&mut self) {
        for replica in &mut self.replicas {
            replica.set_phase(ReplicaPhase::Converged);
        }
    }

    pub fn replicas(&self) -> &[Replica] {
        &self.replicas
    }

    pub fn topology(&self) -> &Topology {
        &self.topology
    }

    pub fn protocol(&self) -> ProtocolKind {
        self.protocol.kind()
    }

    pub fn current_round(&self) -> usize {
        self.current_round
    }

    pub fn target_union(&self) -> &HashSet<Element> {
        &self.target_union
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulator::protocols::ProtocolKind;
    use crate::simulator::topology::TopologyKind;
    use crate::simulator::workload::{DivergencePattern, WorkloadConfig};

    fn make_config(topology: TopologyKind, round_cap: usize) -> SimulationConfig {
        SimulationConfig {
            round_cap,
            seed: 42,
            topology,
            protocol: ProtocolKind::FullStateTransfer,
            workload: WorkloadConfig {
                num_replicas: 8,
                set_size: 1_000,
                payload_size: 32,
                digest_bits: 64,
                divergence: 0.1,
                pattern: DivergencePattern::Uniform,
                seed: 42,
                universe_size: 10_000,
                zipf_exponent: 1.0,
                cluster_count: None,
                inter_cluster_divergence: None,
                intra_cluster_divergence: None,
            },
        }
    }

    fn assert_all_replicas_match_target(simulation: &Simulation) {
        let target = simulation.target_union();

        for replica in simulation.replicas() {
            assert_eq!(
                &replica.set, target,
                "replica {} did not converge to target union",
                replica.id
            );
        }
    }

    #[test]
    fn full_state_transfer_converges_on_star() {
        let config = make_config(TopologyKind::Star, 20);
        let mut simulation = Simulation::new(config);

        let result = simulation.run();

        assert!(result.converged);
        assert_eq!(result.status, RunStatus::Converged);
        assert_eq!(result.topology, TopologyKind::Star);
        assert_eq!(result.protocol, ProtocolKind::FullStateTransfer);
        assert_all_replicas_match_target(&simulation);

        for replica in simulation.replicas() {
            assert_eq!(replica.phase, ReplicaPhase::Converged);
        }
    }

    #[test]
    fn full_state_transfer_converges_on_tree() {
        let config = make_config(TopologyKind::Tree, 20);
        let mut simulation = Simulation::new(config);

        let result = simulation.run();

        assert!(result.converged);
        assert_eq!(result.status, RunStatus::Converged);
        assert_eq!(result.topology, TopologyKind::Tree);
        assert_eq!(result.protocol, ProtocolKind::FullStateTransfer);
        assert_all_replicas_match_target(&simulation);

        for replica in simulation.replicas() {
            assert_eq!(replica.phase, ReplicaPhase::Converged);
        }
    }

    #[test]
    fn full_state_transfer_converges_on_chord() {
        let config = make_config(TopologyKind::Chord, 20);
        let mut simulation = Simulation::new(config);

        let result = simulation.run();

        assert!(result.converged);
        assert_eq!(result.status, RunStatus::Converged);
        assert_eq!(result.topology, TopologyKind::Chord);
        assert_eq!(result.protocol, ProtocolKind::FullStateTransfer);
        assert_all_replicas_match_target(&simulation);

        for replica in simulation.replicas() {
            assert_eq!(replica.phase, ReplicaPhase::Converged);
        }
    }

    #[test]
    fn round_cap_reached_when_cap_is_too_low() {
        let config = make_config(TopologyKind::Tree, 0);
        let mut simulation = Simulation::new(config);

        let result = simulation.run();

        assert!(!result.converged);
        assert_eq!(result.status, RunStatus::RoundCapReached);
        assert_eq!(result.rounds, 0);
    }

    #[test]
    fn metrics_rounds_match_result_rounds() {
        let config = make_config(TopologyKind::Star, 20);
        let mut simulation = Simulation::new(config);

        let result = simulation.run();

        assert_eq!(result.metrics.rounds, result.rounds);
        assert_eq!(result.metrics.per_node.len(), result.num_replicas);
    }

    #[test]
    fn elements_added_is_recorded_for_at_least_one_replica() {
        let config = make_config(TopologyKind::Tree, 20);
        let mut simulation = Simulation::new(config);

        let result = simulation.run();

        let total_added: usize = result
            .metrics
            .per_node
            .iter()
            .map(|m| m.elements_added)
            .sum();

        assert!(
            total_added > 0,
            "expected at least one replica to record added elements"
        );
    }

    #[test]
    fn step_increments_round_when_not_finished() {
        let config = make_config(TopologyKind::Tree, 20);
        let mut simulation = Simulation::new(config);

        assert_eq!(simulation.current_round(), 0);

        let outcome = simulation.step();

        assert_ne!(outcome, RoundOutcome::RoundCapReached);
        assert_eq!(simulation.current_round(), 1);
    }
}
