# Bandwidth Researcher Agent

You are an autonomous research agent. Your single task is to iteratively
improve the multi-replica set reconciliation protocol in
`src/simulator/protocols/multi_replica.rs` to minimise total bandwidth
while maintaining convergence. **Never stop experimenting.**

**Workload**: `set_size=10_000`, `universe_size=500_000`,
`pattern=Uniform`, `round_cap=100`, `seeds=[42,43,44]`, swept across
`num_replicas=[16, 64]` and `jaccard_similarities=[0.25, 0.5, 0.75]`.
Each `(topology, J, n)` triple is a "cell" in the evaluation matrix
(3 × 3 × 2 = 18 cells total). All fitness numbers below are measured
in this configuration — they are not comparable with runs at a
different scale.

---

## Loop

**Before you start, read `hints.md` in the repo root.** It distils
lessons from prior agent runs at smaller scales: which architectures
worked, which optimisations helped, and — critically — which dead-end
strategies have already been tried and *must not be retried*. Re-read
it whenever you're stuck; it is your shortest path to a working
multi-scale protocol.

Repeat indefinitely:

1. **Read** `hints.md`, the current `multi_replica.rs`, and `experiments.tsv`.
2. **Hypothesise** a change that should reduce total bytes sent. Cross-check
   against `hints.md` — if you're about to retry something the "do NOT retry"
   list rules out, pick a different hypothesis.
3. **Edit** `src/simulator/protocols/multi_replica.rs` (the only file you may edit).
4. **Commit** your change: `git add src/simulator/protocols/multi_replica.rs && git commit -m "<short description of what you tried>"`.
5. **Evaluate**: `cargo test 2>/dev/null && cargo run --release --bin eval -- --config agent.toml 2>/dev/null > /tmp/result.json`.
6. **Parse** the result: read `summary.fitness` and `summary.all_converged` from `/tmp/result.json`. Check `summary.by_cell[*].std_bytes` — if any cell's `std_bytes` exceeds the improvement, the change is noise, not signal.
7. **Record** the result: **ALWAYS** append one line to `experiments.tsv`
   (see format below). **You MUST log every attempt — both successes AND
   failures.** Failed attempts with `kept: no` are essential memory to
   avoid re-exploring dead ends. If build/tests fail, log fitness as
   `1e30` (a sentinel clearly out of band of any realistic value).
8. **Decide**:
   - If `all_converged` is true AND the scalar fitness improved, **keep** the commit.
   - Otherwise, **revert**: `git revert --no-edit HEAD`.
9. Go to step 1.

---

## Scalar fitness

```
fitness = geometric_mean(mean_bytes across all cells)
        = exp( (1/N) · Σ ln(mean_bytes[cell]) )
```

Where a cell = one `(topology, jaccard_similarity, num_replicas)` triple,
and each cell's `mean_bytes` is averaged across `seeds = [42, 43, 44]`.
Lower is better. Treat `all_converged == false` as fitness = infinity
(reject the attempt).

**Why geometric mean, not sum.** Summing would make large-n / low-J /
high-edge-count cells dominate the fitness by orders of magnitude — a
10% improvement on Chord@n=64@J=0.25 would outweigh a 90% improvement
on Star@n=16@J=0.75. Geometric mean puts every cell on equal log-scale
footing: an X% reduction in *any* cell reduces fitness by roughly X/N%,
regardless of that cell's absolute byte count. This forces the agent to
improve broadly rather than exploiting the worst cell.

The numeric value is in byte-like units (it's `exp` of average log-bytes),
so "fitness 500M" means the typical cell transfers ~500M bytes.

---

## Experiment log

Append every attempt (success or revert) to `experiments.tsv` in this format:

```
attempt\tfitness\tall_converged\tkept\tdescription
```

The first line is the header (create the file with it if it doesn't exist).
`kept` is `yes` or `no`. `description` is a one-line summary of what you tried.

This log is your memory across iterations. Read it before each attempt to
avoid re-exploring dead ends.

---

## Current state

`multi_replica.rs` is the full-state-transfer scaffold, so its fitness
equals the FST baseline.

**Target to beat: `FullStateTransfer` fitness = 459,237,474 bytes**
(measured 2026-04-18 on the current config: `universe_size=500_000`,
`num_replicas=[16, 64]`, `J=[0.25, 0.5, 0.75]`, `seeds=[42, 43, 44]`,
all 18 cells converged).

The largest cells in absolute bytes are n=64 Tree/Chord at low J
(~3–5 GB each). Log-space, every cell contributes equally — but those
large cells also have the most slack versus a sketch-based approach,
so reductions there tend to be both easy and high-impact.

To re-measure the baseline after any config change:

```bash
sed 's/^protocol = "MultiReplica"/protocol = "FullStateTransfer"/' agent.toml \
  > /tmp/baseline-FST.toml
./target/release/eval --config /tmp/baseline-FST.toml \
  | jq '.summary.fitness'
```

---

## Constraints

1. **Only edit** `src/simulator/protocols/multi_replica.rs`. No other files
   (except `experiments.tsv` for logging).
2. Must implement the `Protocol` trait from `src/simulator/protocols/mod.rs`.
   The trait gives you `send_phase(&self, local: ReplicaView<'_>, topology,
   outbox: &mut Outbox<'_>, carry)` and `recv_phase(&self, local, topology,
   inbox: &mut [(usize, ProtocolMsg)]) -> ProtocolStepResult`.
3. To emit messages, call `Outbox::send_elements / send_riblt / send_bloom /
   send_rateless_bloom / send_bloom_riblt / send_rateless_bloom_riblt`.
   The `Outbox` is engine-bound to the current replica; you cannot supply
   a different sender.
4. To read incoming messages, use the `ProtocolMsg` accessors —
   `take_elements()`, `as_riblt()`, `as_bloom()`, `as_rateless_bloom()`,
   `as_bloom_riblt()`, `as_rateless_bloom_riblt()`. There is no way to
   pattern-match the inner enum from outside `network.rs`.
5. Every field on every `ProtocolMsg` variant is billed by `WireSized`
   (`state_bytes` at send time, `metadata_bytes` after recv). There is no
   free data channel and no reconstruction shortcut.
6. The set returned in `ProtocolStepResult.next_set` must satisfy
   `next_set ⊇ local.snapshot_set()` — protocols may only add elements,
   never remove them. The engine asserts this.
7. May use any algorithm in `src/simulator/algorithms/` (RIBLT sketches,
   Bloom filters, rateless Bloom filters, multiparty sketches).
8. May add internal state to `MultiReplicaProtocol`, but `Protocol`'s
   methods take `&self`, not `&mut self` — per-replica state must live
   inside the carry that flows through `recv_phase` → engine → `send_phase`,
   not on `self`. `self` is shared across all replicas.
9. `cargo test` must pass after every edit.
10. All cells in the matrix must converge (`converged: true`).

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

- `hints.md` — distilled strategy from prior runs (read this first)
- `src/simulator/protocols/mod.rs` — `Protocol` trait, `ProtocolStepResult`, `LocalMetrics`, `PendingElements` (the carry type)
- `src/simulator/network.rs` — `Outbox` (send path), `ProtocolMsg` and its accessors, `WireSized`, sealed message wrappers (`RibltMsg`, `BloomMsg`, `RatelessBloomMsg`, …)
- `src/simulator/replica.rs` — `Element` (digest: u64, payload: Vec<u8>), `Replica`, `ReplicaView` (the borrowed view passed to protocols)
- `src/simulator/topology.rs` — `Topology`, `neighbors()`, `node_count()`, `kind`
- `src/simulator/algorithms/riblt/` — RIBLT sketch implementation
- `src/simulator/algorithms/bloom.rs` — Bloom filter (uses RandomState — not directly serialisable, but the algorithm is reusable)
- `src/simulator/algorithms/rateless_bloom/` — Rateless Bloom filter and stopping strategies
- `src/simulator/algorithms/multiparty_sketch.rs` — finite-field coded sketches that can be added/subtracted along tree paths
- `src/simulator/protocols/riblt.rs` — existing RIBLT protocol (reference implementation; uses the carry pattern)
- `src/simulator/protocols/hybrid_rbf_riblt.rs` — existing hybrid protocol (reference)
- `src/simulator/protocols/full_state_transfer.rs` — minimal reference; current `multi_replica.rs` is a copy of this
