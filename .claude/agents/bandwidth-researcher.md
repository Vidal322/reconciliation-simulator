# Bandwidth Researcher Agent

You are an autonomous research agent. Your single task is to iteratively
improve the multi-replica set reconciliation protocol in
`src/simulator/protocols/multi_replica_v2.rs` to minimise total bandwidth
while maintaining convergence. **Never stop experimenting.**

---

## Loop

Repeat indefinitely:

1. **Read** the current `multi_replica_v2.rs` and `experiments.tsv`.
2. **Hypothesise** a change that should reduce total bytes sent.
3. **Edit** `src/simulator/protocols/multi_replica_v2.rs` (the only file you may edit).
4. **Commit** your change: `git add src/simulator/protocols/multi_replica_v2.rs && git commit -m "<short description of what you tried>"`.
5. **Evaluate**: `cargo test 2>/dev/null && cargo run --release -- --protocol MultiReplicaV2 --json 2>/dev/null`.
6. **Record** the result: append one line to `experiments.tsv` (see format below).
7. **Decide**:
   - If all three topologies converge AND the scalar fitness improved, **keep** the commit.
   - Otherwise, **revert**: `git revert --no-edit HEAD`.
8. Go to step 1.

---

## Scalar fitness

```
fitness = total_bytes_sent(Star) + total_bytes_sent(Tree) + total_bytes_sent(Chord)
```

Lower is better. A run that fails to converge on any topology has fitness = 999999999.

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

The protocol currently implements Bloom-filter reconciliation with BF-skip,
direct broadcast for small deltas, and piggybacking. Current fitness:

| Topology | Bytes       | Rounds |
|----------|-------------|--------|
| Star     | 2,771,833   | 6      |
| Tree     | 3,586,083   | 11     |
| Chord    | 6,518,775   | 6      |
| **Total**| **12,876,691** |     |

Reference baselines (hand-coded protocols):

| Protocol         | Star      | Tree      | Chord     | Total      |
|------------------|-----------|-----------|-----------|------------|
| FullStateTransfer| 13,174,280| 37,327,720| 39,176,000| 89,678,000 |
| Riblt            | 3,430,856 | 4,221,624 | 5,569,152 | 13,221,632 |
| StaticBfIblt     | 3,491,780 | 6,858,058 | 6,944,520 | 17,294,358 |
| HybridRbfRiblt   | 2,918,142 | 4,193,403 | 5,622,130 | 12,733,675 |

You are already beating Riblt and StaticBfIblt on Star and Tree. The main
opportunity is **Chord** (6.5M vs Riblt's 5.6M).

---

## Constraints

1. **Only edit** `src/simulator/protocols/multi_replica_v2.rs`. No other files
   (except `experiments.tsv` for logging).
2. Must implement the `Protocol` trait from `src/simulator/protocols/mod.rs`.
3. Only use `SendView::send()` in `send_phase` and
   `RecvView::record_decoded_metadata()` in `recv_phase`.
4. Must pass `SimulatorHint::None` — no reconstruction shortcuts.
5. Every field on every `ProtocolMsg` variant is billed by `WireSized`.
   There is no free data channel.
6. May use any algorithm in `src/simulator/algorithms/` (RIBLT sketches,
   Bloom filters, rateless Bloom filters).
7. May add internal state to `MultiReplicaV2Protocol`.
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
