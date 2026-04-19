# Strategy Hints for Bandwidth Researcher Agent

## Workload context

This is a **multi-scale evaluation matrix**: 3 topologies × 3 Jaccard
similarities × 2 node counts × 3 seeds = **54 simulation runs** per
experiment. The fitness is a **geometric mean** across 18 cells, so every
cell contributes equally in log-space.

| N  | J=0.25 (high divergence) | J=0.5 | J=0.75 (high similarity) |
|----|--------------------------|-------|--------------------------|
| 16 | ~3,333 unique/replica    | ~2,000| ~1,143 unique/replica    |
| 64 | ~3,333 unique/replica    | ~2,000| ~1,143 unique/replica    |

Key implications:
- **The protocol must work across ALL scales.** A strategy tuned for 16
  nodes must also work at 64, and vice versa.
- **J=0.25 means very large diffs** (~3,333 unique per replica). BF
  pre-filtering is highly valuable here.
- **J=0.75 means small diffs** (~1,143 unique). RIBLT alone may suffice.
- **64-node Chord** has power-of-2 links at distances 1,2,4,8,16,32 →
  6 neighbors per node, diameter ~6 hops.
- **64-node Tree** has depth ~6.
- **Do NOT hardcode node counts or topology sizes.** Use `topology.node_count()`,
  `topology.neighbors(id).len()`, etc.

---

## What we learned from prior runs (8-node and 32-node)

### Architecture that works across scales

**Star/Tree: single BF+RIBLT sketch + eager forwarding**
1. Round 1: each node sends a BF+RIBLT hybrid sketch to all neighbors
2. Sketch decoding identifies which elements each neighbor is missing
3. Send missing elements, then eager-forward all newly received elements
   to other neighbors (with `sent_to` dedup to prevent resending)
4. Star converges in 3-4 rounds, Tree in ~2×depth rounds

**Chord: pairwise BF+RIBLT with sequential distance-doubling**
1. Instead of sketching all neighbors every round, sketch only the pair
   at distance 2^power each round (power cycles 0, 1, 2, ..., log2(N)-1)
2. **Ascending distance order is essential**: reconcile nearby first so
   their elements are available when reconciling distant neighbors
3. No eager forwarding on Chord — it causes cascade storms at ≥32 nodes

### Optimizations that helped

- **Asymmetric sketching**: only lower-id node sends the sketch. The
  higher-id node decodes both directions of the diff. Halves metadata.
- **BF-encoded requests**: instead of sending a list of 64-bit digests
  to request missing elements, encode them as a Bloom filter (~10
  bits/element vs 64 bits/element). Use low FPR (0.1%).
- **m_ratio = 1.0**: controls BF size (bits per element). Tested 0.5,
  0.7, 1.0, 1.3 — the optimum is around 1.0 for 50% Jaccard. May
  need adaptation for J=0.25 and J=0.75.
- **Neighbor-has tracking**: skip eager-forwarding to neighbors known
  to already have the element (from sketch decode info).
- **Hub dedup (Star)**: when the hub needs an element, request it from
  only one leaf, not all leaves that have it.

### What does NOT work — do NOT retry

- **Hash-based forwarder selection on Chord**: at 32 nodes, the
  designated forwarder may not have the element yet → fails to converge.
  Will be worse at 64 nodes.
- **Eager forwarding on Chord**: cascade storms at ≥32 nodes. State
  bytes double or worse. Tested twice, failed both times.
- **Source-neighbor suppression on Chord eager forwarding**: still
  cascades, doesn't solve the fundamental problem.
- **Descending distance order for Chord**: 77% worse than ascending.
  Close-first accumulation is essential.
- **Pure RIBLT (no BF) at J=0.5**: 10% worse than BF+RIBLT. BF
  pre-filtering is valuable when diffs are non-trivial.
- **Pure RIBLT for Chord powers 1-3**: diff still too large at early
  powers, bidirectional billing makes it more expensive than BF+RIBLT.

---

## Strategy recommendations for the full matrix

### Tier 1 — Start here

#### 1. Topology-dispatched hybrid protocol
The proven architecture from prior runs:
```
if Chord:
    sequential distance-doubling with BF+RIBLT (ascending powers)
else:  // Star, Tree
    single BF+RIBLT sketch round 1 + eager forwarding
```
Use `topology.kind` for dispatch but do NOT hardcode node counts.
The distance-doubling power cycle should go from 0 to
`floor(log2(node_count)) - 1` (not hardcoded `% 5`).

#### 2. Asymmetric sketching everywhere
Only lower-id node sends the sketch. Higher-id decodes both directions.
This works for all topologies and all scales.

#### 3. Adaptive m_ratio based on estimated diff size
- J=0.25 (large diffs): m_ratio=1.0 or higher — BF savings are large
- J=0.75 (small diffs): lower m_ratio or even pure RIBLT — BF overhead
  may exceed savings for small diffs
- The protocol doesn't know J directly, but it knows set_size and can
  estimate diff from sketch decode results in later rounds

### Tier 2 — After the basics work

#### 4. Direct element transfer for tiny remaining diffs
After the main reconciliation phase, remaining diffs are often < 50
elements per pair. BF+RIBLT overhead (~10KB per sketch) exceeds the
cost of just sending 50 elements (2KB). Switch to direct transfer
in late rounds.

#### 5. BF-skip for converged pairs
If a node's set hasn't grown since its last sketch to a specific
neighbor, skip that sketch. Most useful in later rounds after
initial convergence.

#### 6. Tree: directed upward-downward flow
Instead of bidirectional sketch+forward on all edges:
- Upward phase: children sketch to parent only
- Downward phase: root pushes missing elements to children
May reduce Tree rounds and avoid redundant sibling forwarding.

### Tier 3 — Advanced

#### 7. Multiparty sketch aggregation
`src/simulator/algorithms/multiparty_sketch.rs` implements finite-field
coded sketches with `add_sketch()` / `sub_sketch()` / `try_decode()`.
Enables aggregating multiple nodes' sketches along tree paths. Could
be much more efficient than pairwise reconciliation at 64 nodes.

---

## Geometric mean fitness — practical implications

- An X% improvement in ANY cell reduces fitness by ~X/18%.
- The cells with worst absolute performance (Chord/N=64/J=0.25 at 5.6B)
  and best (Star/N=16/J=0.75 at 36M) contribute equally.
- **Don't neglect small cells.** A 50% improvement on Star/N=16/J=0.75
  (36M → 18M) has the same fitness impact as 50% on Chord/N=64/J=0.25
  (5.6B → 2.8B).
- **Check std_bytes**: if the standard deviation across seeds exceeds
  the improvement, the change is noise, not signal. Reject it.

## General principles

- **The protocol must generalize.** No hardcoded node counts, no
  topology-specific constants that break at different scales.
- **Rounds are cheap (cap=100), bytes are expensive.**
- **Log every attempt** (success AND failure) with `kept: yes/no`.
- **Check experiments.tsv before each attempt** to avoid retrying failures.
- **Test one thing at a time.** Isolate changes so you know what worked.
- **Each experiment runs 54 simulations** — it will take longer than
  before. Be patient with evaluation time.
