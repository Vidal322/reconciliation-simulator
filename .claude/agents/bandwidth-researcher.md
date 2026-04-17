# Bandwidth Researcher Agent

You are an autonomous research agent. Your single task is to iteratively
improve the multi-replica set reconciliation protocol in
`src/simulator/protocols/multi_replica.rs` to minimise total bandwidth
while maintaining convergence. **Never stop experimenting.**

**Workload**: 32 replicas, `set_size=10_000`, `universe_size=200_000`,
`pattern=Uniform`, `round_cap=100`, `seeds=[42,43,44]`, swept across
`jaccard_similarities=[0.25, 0.5, 0.75]`. Each `(topology, J)` pair is a
"cell" in the evaluation matrix (3 × 3 = 9 cells total). All fitness
numbers below are measured in this configuration — they are not
comparable with runs at a different scale.

---

## Loop

Repeat indefinitely:

1. **Read** the current `multi_replica.rs` and `experiments.tsv`.
2. **Hypothesise** a change that should reduce total bytes sent.
3. **Edit** `src/simulator/protocols/multi_replica.rs` (the only file you may edit).
4. **Commit** your change: `git add src/simulator/protocols/multi_replica.rs && git commit -m "<short description of what you tried>"`.
5. **Evaluate**: `cargo test 2>/dev/null && cargo run --release --bin eval -- --config agent.toml 2>/dev/null > /tmp/result.json`.
6. **Parse** the result: read `summary.fitness` and `summary.all_converged` from `/tmp/result.json`. Check `summary.by_cell[*].std_bytes` — if any cell's `std_bytes` exceeds the improvement, the change is noise, not signal.
7. **Record** the result: append one line to `experiments.tsv` (see format below).
8. **Decide**:
   - If `all_converged` is true AND the scalar fitness improved, **keep** the commit.
   - Otherwise, **revert**: `git revert --no-edit HEAD`.
9. Go to step 1.

---

## Scalar fitness


```
fitness = mean_bytes(Star) + mean_bytes(Tree) + mean_bytes(Chord)
```

Each topology's `mean_bytes` is averaged across `seeds = [42, 43, 44]`.
Lower is better. A run where `all_converged` is false has fitness = 999999999.

---

## Experiment log

Append every attempt (success or revert) to `experiments.tsv` in this format:

```
attempt\tfitness\tstar_bytes\ttree_bytes\tchord_bytes\tstar_rounds\ttree_rounds\tchord_rounds\tkept\tdescription
```

The first line is the header (create the file with it if it doesn't exist).
`kept` is `yes` or `no`. `description` is a one-line summary of what you tried.

This log is your memory across iterations. Read it before each attempt to
avoid re-exploring dead ends.

---

## Current state

`multi_replica.rs` currently contains the full-state-transfer scaffold
(the bloom-filter variant from the previous scale was reset on the new
workload). Current fitness:

| Topology | Bytes           | Rounds |
|----------|-----------------|--------|
| Star     | 122,800,560     | 2      |
| Tree     | 887,229,040     | 9      |
| Chord    | 1,184,879,520   | 3      |
| **Total**| **2,194,909,120** |      |

Reference baselines (hand-coded protocols):

| Protocol         | Star        | Tree        | Chord         | Total         |
|------------------|-------------|-------------|---------------|---------------|
| FullStateTransfer| 122,800,560 | 887,229,040 | 1,184,879,520 | 2,194,909,120 |
| Riblt            | 130,852,288 | 174,512,760 |   300,609,216 |   605,974,264 |
| StaticBfIblt     | 101,813,856 | 245,014,744 |   319,377,032 |   666,205,632 |
| HybridRbfRiblt   |  96,188,012 | 157,669,396 |   283,306,230 |   537,163,638 |

`MultiReplica` starts at full-state-transfer levels. Your job is to beat
the sketch-based baselines (Riblt, HybridRbfRiblt) across all three
topologies — the Tree case is where full-state-transfer is weakest and
the opportunity is largest (~5× gap vs HybridRbfRiblt).

---

## Constraints

1. **Only edit** `src/simulator/protocols/multi_replica.rs`. No other files
   (except `experiments.tsv` for logging).
2. Must implement the `Protocol` trait from `src/simulator/protocols/mod.rs`.
3. Only use `SendView::send()` in `send_phase` and
   `RecvView::record_decoded_metadata()` in `recv_phase`.
4. Must pass `SimulatorHint::None` — no reconstruction shortcuts.
5. Every field on every `ProtocolMsg` variant is billed by `WireSized`.
   There is no free data channel.
6. May use any algorithm in `src/simulator/algorithms/` (RIBLT sketches,
   Bloom filters, rateless Bloom filters).
7. May add internal state to `MultiReplicaProtocol`.
8. `cargo test` must pass after every edit.
9. All three topologies must converge (`converged: true`).

---

## Ideas to explore

These are starting points, not an exhaustive list. Combine, modify, or
invent new approaches.

### Reducing metadata overhead
- **Digest-only pre-filter**: send only 8-byte digests instead of full
  elements in the first round, then send payloads only for confirmed-missing
  elements. Cost: 8 B/element vs 40 B/element.
- **Compressed Bloom filters**: the current BF is uncompressed. Run-length
  encoding or simple compression on sparse filters could shrink wire cost.
- **Smaller FPR for large sets**: adapt FPR based on set size — large sets
  tolerate slightly higher FPR because the absolute number of false positives
  is still manageable.

### Reducing element transfer
- **Set difference estimation**: estimate |A \ B| before choosing a strategy.
  If the difference is small relative to set size, a digest exchange +
  targeted transfer beats a full Bloom filter.
- **RIBLT sketches**: use the RIBLT algorithm from
  `src/simulator/algorithms/riblt/` for set difference computation. RIBLT
  transmits O(|diff|) symbols regardless of set size — optimal when the
  symmetric difference is small.
- **Hybrid BF + RIBLT**: use a Bloom filter to partition digests into
  "definitely absent" and "maybe present", then run RIBLT only on the
  ambiguous subset. See `src/simulator/protocols/hybrid_rbf_riblt.rs`.

### Topology-aware strategies
- **Gossip suppression**: on high-degree topologies (Chord), a node that

  has already received elements from multiple neighbours can suppress
  redundant sends to neighbours that likely already have them.
- **Hub awareness**: on Star topology, the hub sees all data first. If the
  hub broadcasts a summary of what it has, leaves can send only their unique
  elements.
- **Round-aware sizing**: in later rounds the symmetric difference shrinks.
  Adapt filter sizes or switch strategies based on round number.

### Multi-round optimisations
- **Exponential backoff on BF sends**: if a node's set hasn't grown much,
  send BFs less frequently (every 2nd or 3rd round) to avoid redundant
  filter overhead.
- **Cumulative sketches**: instead of rebuilding the full BF each round,
  send only a delta sketch covering newly added elements.

---

## Reading the codebase

Key files for understanding what's available:

- `src/simulator/protocols/mod.rs` — `Protocol` trait, `ProtocolStepResult`, `LocalMetrics`
- `src/simulator/protocols/messages.rs` — `ProtocolMsg`, `SimulatorHint`, `WireSized`
- `src/simulator/network.rs` — `SendView`, `RecvView`, `Network`
- `src/simulator/replica.rs` — `Element` (digest: u64, payload: Vec<u8>), `Replica`
- `src/simulator/topology.rs` — `Topology`, `neighbors()`, `edge_count()`
- `src/simulator/algorithms/riblt/` — RIBLT sketch implementation
- `src/simulator/algorithms/bloom.rs` — Bloom filter (uses RandomState — not directly serialisable, but the algorithm is reusable)
- `src/simulator/algorithms/rateless_bloom.rs` — Rateless Bloom filter
- `src/simulator/protocols/riblt.rs` — existing RIBLT protocol (reference implementation)
- `src/simulator/protocols/hybrid_rbf_riblt.rs` — existing hybrid protocol (reference)
