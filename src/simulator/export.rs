use std::fs::{OpenOptions, metadata};
use std::io::{Result, Write};
use std::path::Path;

use crate::simulator::engine::{SimulationConfig, SimulationResult};
use crate::simulator::workload::DivergencePattern;

#[derive(Clone, Debug)]
pub struct RunSummaryRow {
    pub protocol: String,
    pub topology: String,
    pub num_replicas: usize,
    pub set_size: usize,
    pub payload_size: usize,
    pub digest_bits: usize,
    pub jaccard_similarity: f64,
    pub pattern: String,
    pub seed: u64,
    pub round_cap: usize,
    pub rounds: usize,
    pub converged: bool,
    pub target_union_size: usize,
    pub total_state_bytes_sent: usize,
    pub total_metadata_bytes_sent: usize,
    pub total_encode_time_ns: u128,
    pub total_decode_time_ns: u128,
    pub total_elements_added: usize,
}

impl RunSummaryRow {
    pub fn from_run(
        config: &SimulationConfig,
        result: &SimulationResult,
        target_union_size: usize,
    ) -> Self {
        Self {
            protocol: format!("{:?}", result.protocol),
            topology: format!("{:?}", result.topology),
            num_replicas: config.workload.num_replicas,
            set_size: config.workload.set_size,
            payload_size: config.workload.payload_size,
            digest_bits: config.workload.digest_bits,
            jaccard_similarity: config.workload.jaccard_similarity,
            pattern: divergence_pattern_to_string(config.workload.pattern),
            seed: config.seed,
            round_cap: config.round_cap,
            rounds: result.rounds,
            converged: result.converged,
            target_union_size,
            total_state_bytes_sent: result.metrics.total_state_bytes_sent,
            total_metadata_bytes_sent: result.metrics.total_metadata_bytes_sent,
            total_encode_time_ns: result.metrics.total_encode_time.as_nanos(),
            total_decode_time_ns: result.metrics.total_decode_time.as_nanos(),
            total_elements_added: result.metrics.total_elements_added,
        }
    }

    pub fn csv_header() -> &'static str {
        "protocol,topology,num_replicas,set_size,payload_size,digest_bits,jaccard_similarity,pattern,seed,round_cap,rounds,converged,target_union_size,total_state_bytes_sent,total_metadata_bytes_sent,total_encode_time_ns,total_decode_time_ns,total_elements_added"
    }

    pub fn to_csv_row(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            self.protocol,
            self.topology,
            self.num_replicas,
            self.set_size,
            self.payload_size,
            self.digest_bits,
            self.jaccard_similarity,
            self.pattern,
            self.seed,
            self.round_cap,
            self.rounds,
            self.converged,
            self.target_union_size,
            self.total_state_bytes_sent,
            self.total_metadata_bytes_sent,
            self.total_encode_time_ns,
            self.total_decode_time_ns,
            self.total_elements_added
        )
    }
}

pub fn append_run_summary_csv<P: AsRef<Path>>(path: P, row: &RunSummaryRow) -> Result<()> {
    let path = path.as_ref();

    let file_exists = path.exists();
    let file_is_empty = if file_exists {
        metadata(path)?.len() == 0
    } else {
        true
    };

    let mut file = OpenOptions::new().create(true).append(true).open(path)?;

    if file_is_empty {
        writeln!(file, "{}", RunSummaryRow::csv_header())?;
    }

    writeln!(file, "{}", row.to_csv_row())?;
    Ok(())
}

fn divergence_pattern_to_string(pattern: DivergencePattern) -> String {
    match pattern {
        DivergencePattern::Uniform => "Uniform".to_string(),
        DivergencePattern::Clustered => "Clustered".to_string(),
    }
}
