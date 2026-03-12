mod simulator;

use simulator::runner::{BatchRunnerConfig, run_batch};

fn main() {
    let config = BatchRunnerConfig::baseline("results.csv");

    match run_batch(&config) {
        Ok(summary) => {
            println!();
            println!("Finished batch run.");
            println!("Total runs: {}", summary.total_runs);
            println!("Converged runs: {}", summary.converged_runs);
            println!("Non-converged runs: {}", summary.non_converged_runs);
        }
        Err(err) => {
            eprintln!("Batch run failed: {}", err);
            std::process::exit(1);
        }
    }
}
