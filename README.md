# Reconciliation Simulator

A simulator for evaluating multi-party set-reconciliation protocols under
controlled workload and topology conditions. Built for a master's thesis on
bandwidth-efficient reconciliation; designed to produce publishable,
reproducible measurements.

The repository also hosts an autonomous agent (`bandwidth-researcher`) that
iteratively improves a protocol (`MultiReplica`) against fixed baselines.

---

## Architecture

One library crate, two binaries:

- **Library** (`src/lib.rs` → `simulator` module): workload generation,
  topology construction, protocol trait, simulation engine, metrics.
- **`main.rs`**: human-facing diagnostic runner. Executes every
  `(protocol × topology)` pair on a fixed workload and prints verbose per-run
  output.
- **`src/bin/eval.rs`**: agent-facing evaluation binary. Reads a TOML config,
  runs a single protocol across topologies × seeds, emits structured JSON to
  stdout. No file side effects.

Both binaries share the simulator library — the split keeps the agent's
evaluation interface clean (valid JSON, parameterised config) and the human
interface rich (diagnostic tables, CSV export).

---

## Components

### Workload (`src/simulator/workload.rs`)

Generates replica sets with controlled pairwise similarity. The key parameter
is **Jaccard similarity** `J(A,B) = |A ∩ B| / |A ∪ B|`; the workload derives
set overlap from `J` via `c = 2·n·J / (1 + J)`.

Supports two patterns:
- `Uniform` — every pair of replicas has the same `J`.
- `Clustered` — replicas are grouped into clusters with higher intra-cluster
  similarity (`jaccard_intra`) and lower inter-cluster similarity
  (`jaccard_inter`).

Element sampling uses a Zipf distribution so some digests are popular and
some are rare — closer to real workloads than uniform sampling.

### Topology (`src/simulator/topology.rs`)

Three fixed topologies:
- `Star` — n-1 edges, diameter 2. One hub, n-1 leaves.
- `Tree` — n-1 edges, diameter O(log n). A balanced binary tree.
- `Chord` — O(n log n) edges, diameter O(log n). Each node connects to peers
  at exponentially spaced offsets.

### Engine (`src/simulator/engine.rs`)

The simulation loop. One **round** = every replica runs `send_phase` in
parallel, then every replica runs `recv_phase` on its inbox. Terminates when
every replica holds the target union, or when `round_cap` is reached.

### Network (`src/simulator/network.rs`)

The billing model. `SendView::send()` and `RecvView::record_decoded_metadata()`
are the only operations that add bytes to a replica's metrics. There is **no
free data channel** — every field of every `ProtocolMsg` is billed by
`WireSized`. This is what makes cross-protocol bandwidth comparisons fair.

### Metrics (`src/simulator/metrics.rs`)

Per-node and aggregate counters: state bytes sent/received, metadata bytes
sent/received, encode/decode time, elements added. `SimulationResult`
exposes both per-node breakdowns and totals.

### Protocols (`src/simulator/protocols/`)

Each protocol implements `Protocol` with `send_phase` and `recv_phase`:

- `full_state_transfer.rs` — naive baseline; every replica ships its entire
  set to every neighbour. Upper bound on bandwidth.
- `riblt.rs` — RIBLT sketches for set difference. Transmits O(|diff|)
  symbols regardless of set size.
- `bf_iblt.rs` — Static Bloom filter + IBLT. Bloom filter partitions digests
  into "definitely absent" and "ambiguous", IBLT resolves the ambiguous
  subset.
- `hybrid_rbf_riblt.rs` — Rateless Bloom + RIBLT. Reference for the
  bandwidth-efficient frontier.
- `multi_replica.rs` — **the agent's target**. Starts as a full-state-transfer
  scaffold; the bandwidth-researcher agent iteratively improves it.

### Algorithms (`src/simulator/algorithms/`)

Reusable building blocks, independent of any specific protocol:

- `bloom.rs` — standard Bloom filter.
- `rateless_bloom/` — rateless (growable) Bloom filter with Bayesian stopping
  strategies.
- `riblt/` — rateless IBLT for set difference.
- `multiparty_sketch.rs` — cross-party sketches.
- `bayesian_estimation.rs` — set-difference estimators.

---

## Running

### Full diagnostic run (all protocols, all topologies)

```bash
cargo run --release
```

Prints verbose per-run output and appends rows to `results.csv` (gitignored).

### Single evaluation via the agent path

```bash
cargo run --release --bin eval -- --config agent.toml
```

Emits one JSON document on stdout with per-run entries and a summary
(`mean_bytes`, `std_bytes`, `mean_rounds` per `(topology, jaccard_similarity)`
cell; scalar `fitness` = sum of cell `mean_bytes`).

### Tests

```bash
cargo test
```

### Runtime

Wall-clock time for one `eval` invocation of `FullStateTransfer` (3 seeds
per cell, binary pre-built, measured on the current host). `MultiReplica`
currently is the FST scaffold, so these are representative of the agent's
per-iteration cost before it improves the protocol.

| Replicas | Star   | Tree    | Chord   | Total (3 topologies) |
|----------|--------|---------|---------|----------------------|
| 16       |  0.8 s |  2.5 s  |  1.6 s  |   ~4.9 s             |
| 32       |  2.1 s |  8.5 s  |  9.5 s  |  ~20.1 s             |
| 64       |  6.1 s | 34.0 s  | 37.0 s  |  ~77.1 s             |

`cargo test` adds ~1.3 s when the build is warm. At the default `n=32`
workload, one agent iteration (test + eval across 3 topologies × 3 seeds)
costs ~21 s. Scaling is roughly O(n²): FST sends every element to every
neighbour. Tree and Chord dominate because they have more edges and larger
diameter than Star. Sketch-based protocols are faster — once the agent
moves `MultiReplica` away from the scaffold, iteration time should drop.

#### Estimate: full evaluation matrix

A thesis-scale sweep (3 seeds × 3 replica counts × 3 topologies × 3
similarity levels = 81 simulations per iteration) costs roughly:

| Component               | Time    |
|-------------------------|---------|
| `cargo test`            |  ~1 s   |
| n=16, all topos, 3 Js   |  ~14 s  |
| n=32, all topos, 3 Js   |  ~59 s  |
| n=64, all topos, 3 Js   | ~227 s  |
| **Total per iteration** | **~5 min** |

The J factor is empirical: at n=32 Tree, FST takes 12.1 s / 8.9 s / 5.2 s
at J = 0.25 / 0.5 / 0.75 — a 2.9× multiplier over a single J level,
because lower similarity means a larger union and more rounds to converge.
A night of unattended iteration (~8 h) therefore buys ~95 agent attempts
at the full matrix; a full day (\~24 h) buys \~290. Sketch-based protocols
should cut this substantially once `MultiReplica` moves off the scaffold.

---

## The `bandwidth-researcher` agent

An autonomous agent that iteratively improves `src/simulator/protocols/multi_replica.rs`
to reduce total bandwidth while maintaining convergence across all three
topologies.

### Configuration

- **Agent instructions**: `.claude/agents/bandwidth-researcher.md` — loop
  definition, fitness function, baselines, ideas to explore.
- **Evaluation config**: `agent.toml` — workload, topologies, seeds,
  `round_cap`.
- **Experiment log**: `experiments.tsv` — running log of attempts; the
  agent's memory across iterations.

### Loop

Each iteration:
1. Read `multi_replica.rs` and `experiments.tsv`.
2. Hypothesise a change that reduces total bytes sent.
3. Edit `multi_replica.rs`.
4. Commit: `git add src/simulator/protocols/multi_replica.rs && git commit`.
5. Evaluate: `cargo test && cargo run --release --bin eval -- --config agent.toml > /tmp/result.json`.
6. Parse `summary.fitness` and `summary.all_converged`; flag noisy
   improvements where any topology's `std_bytes` exceeds the gain.
7. Record one row in `experiments.tsv`.
8. Decide: if `all_converged` and fitness improved, keep the commit;
   otherwise `git revert --no-edit HEAD`.

### Running the agent

The agent is a Claude Code subagent. Invoke it from a Claude Code session:

```
/agents bandwidth-researcher
```

Or reference it by name when asking Claude to perform the research loop.

### Fitness

```
fitness = mean_bytes(Star) + mean_bytes(Tree) + mean_bytes(Chord)
```

Means are across seeds = `[42, 43, 44]`. Lower is better. A run where
`all_converged` is false has fitness = 999999999.

---

## Workload configuration (`agent.toml`)

```toml
protocol = "MultiReplica"
topologies = ["Star", "Tree", "Chord"]
seeds = [42, 43, 44]
round_cap = 100

[workload]
num_replicas = 32
set_size = 10_000
payload_size = 32
digest_bits = 64
jaccard_similarity = 0.5
pattern = "Uniform"
universe_size = 200_000
zipf_exponent = 1.0
seed = 42
```

- `seeds` vs `[workload].seed` — outer `seeds` controls simulation randomness
  (one run per seed); inner `workload.seed` seeds workload generation. Kept
  separate so multi-seed evaluation varies protocol randomness while holding
  the workload fixed.
- `jaccard_similarity = 0.5` — each replica has ~33% unique elements vs any
  other replica.
- `universe_size = 200_000` — must exceed `common_size + n·unique` to avoid
  sampling exhaustion.

---

## Project layout

```
src/
├── lib.rs                      # exposes `simulator` as a library
├── main.rs                     # human-facing diagnostic runner
├── bin/
│   └── eval.rs                 # agent-facing evaluation binary
└── simulator/
    ├── mod.rs
    ├── engine.rs               # simulation loop
    ├── network.rs              # billing model
    ├── metrics.rs              # per-node + aggregate counters
    ├── replica.rs              # Element, Replica
    ├── topology.rs             # Star, Tree, Chord
    ├── workload.rs             # Jaccard-based replica generation
    ├── export.rs               # CSV row writer
    ├── algorithms/             # reusable algorithmic primitives
    │   ├── bloom.rs
    │   ├── rateless_bloom/
    │   ├── riblt/
    │   ├── multiparty_sketch.rs
    │   └── bayesian_estimation.rs
    └── protocols/              # reconciliation protocols
        ├── mod.rs              # Protocol trait, ProtocolKind
        ├── messages.rs         # ProtocolMsg, WireSized
        ├── full_state_transfer.rs
        ├── riblt.rs
        ├── bf_iblt.rs
        ├── hybrid_rbf_riblt.rs
        └── multi_replica.rs    # agent's target

agent.toml                      # eval binary config
experiments.tsv                 # agent's experiment log
.claude/agents/
└── bandwidth-researcher.md     # agent instructions
docs/
└── agent-setup-log.md          # thesis changelog (gitignored)
```

---

## Thesis context

See `docs/agent-setup-log.md` for the running changelog of experimental
methodology decisions (scaling choices, Jaccard derivation, round caps,
invariants). Each entry records what changed, why it changed, and what
invariant it preserves or introduces — intended to be cited in the thesis
methodology chapter.
