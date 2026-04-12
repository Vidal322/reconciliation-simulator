use crate::simulator::metrics::{MetricsCollector, MetricsSnapshot};

use crate::simulator::protocols::bf_iblt::StaticBfIbltProtocol;
use crate::simulator::protocols::full_state_transfer::FullStateTransfer;
use crate::simulator::protocols::hybrid_rbf_riblt::HybridRbfRibltProtocol;
use crate::simulator::protocols::riblt::RibltProtocol;

use crate::simulator::protocols::ProtocolKind;
use crate::simulator::replica::{Element, Replica, ReplicaPhase};
use crate::simulator::topology::{Topology, TopologyKind};
use crate::simulator::workload::{Workload, WorkloadConfig};

use crate::simulator::network::Network;
use crate::simulator::protocols::messages::ProtocolMsg;
use crate::simulator::protocols::{LocalMetrics, Protocol};

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
    network: Network<ProtocolMsg>,
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

        let network = Network::from_topology(&topology);

        let protocol: Box<dyn Protocol> = match config.protocol {
            ProtocolKind::FullStateTransfer => Box::new(FullStateTransfer::new()),
            ProtocolKind::Riblt => Box::new(RibltProtocol::new()),
            ProtocolKind::HybridRbfRiblt => Box::new(HybridRbfRibltProtocol::new()),
            ProtocolKind::StaticBfIblt => Box::new(StaticBfIbltProtocol::new()),
        };

        Self {
            config,
            replicas,
            topology,
            protocol,
            network,
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

        self.network.reset();

        // Phase 1: every replica emits its outbound messages.
        for replica_id in 0..self.replicas.len() {
            self.protocol.send_phase(
                replica_id,
                &self.replicas[replica_id],
                &self.topology,
                &mut self.network,
            );
        }

        // Phase 2: every replica drains its inbox and reconciles.
        let recv_results: Vec<(HashSet<Element>, LocalMetrics)> = (0..self.replicas.len())
            .map(|replica_id| {
                let inbox = self.network.drain_inbox(replica_id);
                let result = self.protocol.recv_phase(
                    replica_id,
                    &self.replicas[replica_id],
                    &self.topology,
                    inbox,
                    &mut self.network,
                );
                (result.next_set, result.metrics)
            })
            .collect();

        // Snapshot AFTER recv_phase so decoded metadata is included.
        let stats = self.network.stats();

        // Apply results and bill bytes from Network::stats().
        for (replica_id, (replica, (next_set, local_metrics))) in
            self.replicas.iter_mut().zip(recv_results).enumerate()
        {
            let sent_state = stats.per_node_state[replica_id] as usize;
            let sent_meta = stats.per_node_metadata[replica_id] as usize;
            replica.stats.record_state_bytes_sent(sent_state);
            replica.stats.record_metadata_bytes_sent(sent_meta);
            replica.stats.record_encode_time(local_metrics.encode_time);
            replica.stats.record_decode_time(local_metrics.decode_time);

            let added = next_set.difference(&replica.set).count();
            replica.stats.record_elements_added(added);
            replica.set = next_set;
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
