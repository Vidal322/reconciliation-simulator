# Strategy Hints for Bandwidth Researcher Agent

## Workload context

32 replicas, 10,000 elements each, Jaccard similarity 0.5, 200k universe.
This means each pair of replicas shares ~6,667 elements and each has ~3,333
unique elements. The union is much larger than individual sets.

Compared to the previous 8-node / 90%-similarity run:
- **4× more nodes** → more edges, more hops, more redundancy potential
- **~3× more unique elements per replica** → BF pre-filtering becomes MORE
  valuable (larger diffs = more savings from filtering)
- **32-node Chord** has power-of-2 links at distances 1,2,4,8,16 → 5
  neighbors per node, diameter ~5 hops (vs 3 at 8 nodes)
- **32-node Tree** has depth ~5 (vs 3 at 8 nodes) → more propagation rounds
- **Star hub** handles 31 neighbors (vs 7) → hub bandwidth is critical

---

## Reference: what worked at 8 nodes / 90% similarity

A prior autoresearch run at 8 nodes / 10% divergence achieved **91.6%
bandwidth reduction** (89.7M → 7.5M). The final architecture:

1. **Single sketch round only** — one RatelessBF+RIBLT sketch (Star/Tree)
   or RIBLT sketch (Chord) in round 1. No more metadata after that.
2. **Eager-forwarding** — newly received elements are forwarded to all
   neighbors with `sent_to` / `received_from` dedup to prevent echoes.
3. **Hash-based forwarder selection** (Chord) — among common neighbors of
   (source, dest), only `digest % |common|` forwards → zero redundancy.
4. **Source-neighbor suppression** (Chord) — skip forwarding if source is
   a direct neighbor of destination.

### The full v1 solution code (for reference, DO NOT copy blindly)

The code below was optimized for 8 nodes with 90% similarity. At 32 nodes
with 50% similarity, several aspects need rethinking:

- The hash-based forwarder selection assumed 3-4 common neighbors in 8-node
  Chord. With 32 nodes the neighbor structure is different.
- Eager-forwarding may cause cascade storms with 32 nodes — consider
  limiting forwarding depth or using a smarter routing strategy.
- With 50% Jaccard, diffs are much larger (~3,333 per pair vs ~1,000).
  BF pre-filtering is even more important to reduce RIBLT metadata.
- The single-sketch approach may need multiple sketch rounds at 32 nodes
  because elements need more hops to propagate.

```rust
pub struct MultiReplicaV2Protocol {
    m_ratio: f64,
    state: HashMap<usize, ReplicaState>,
}

struct ReplicaState {
    pending: HashMap<usize, Vec<Element>>,
    recently_gained: Vec<(Element, usize)>,
    sent_to: HashMap<usize, HashSet<u64>>,
    received_from: HashMap<usize, HashSet<u64>>,
    sketch_set_size: usize,
    first_sketch_sent: bool,
}
```

**send_phase had 3 steps:**
1. Drain pending elements from sketch decode (with sent_to dedup)
2. Eager-forward recently gained elements (topology-specific suppression)
3. Send sketch (only once, controlled by first_sketch_sent flag)

**recv_phase had 2 passes:**
1. Apply all Elements messages first (update local set)
2. Decode sketches against the updated set (fresher diffs)

**Key per-topology choices:**
- Star/Tree: RatelessBF+RIBLT hybrid sketch (BF pre-filters, RIBLT resolves)
- Chord: pure RIBLT sketch (BF overhead not worth it on dense graph at 8 nodes,
  but at 32 nodes with 50% Jaccard this may change — BF could help more)

---

## Strategies to consider for 32 nodes / 50% Jaccard

### Tier 1 — High impact

#### 1. Hybrid BF+RIBLT for ALL topologies
At 50% Jaccard, the symmetric difference is large (~3,333 per pair). A
RatelessBF pre-filter can identify most definitely-missing elements cheaply
(a few bits per element), leaving only false positives for RIBLT. This was
only used for Star/Tree at 8 nodes, but at 50% similarity it likely helps
Chord too.

#### 2. Eager-forwarding with cascade control
At 32 nodes, naive eager-forwarding can explode. Consider:
- **TTL / hop limit**: each element carries a hop counter, stop forwarding
  after `max_hops` (e.g., tree depth or chord diameter)
- **Forwarding budget**: limit the number of elements forwarded per round
  per neighbor to prevent bandwidth spikes
- **Selective forwarding**: only forward to neighbors that you know are
  missing the element (based on sketch results or set-size tracking)

#### 3. Tree: directed upward-downward flow
With depth ~5, undirected reconciliation is wasteful. Instead:
- Upward phase: children only reconcile with parent (not bidirectional)
- Downward phase: once root has everything, push missing elements down
- This reduces from ~2×depth rounds of bidirectional exchange to organized
  flow

#### 4. Gossip suppression on Chord
With 32 nodes and 5 neighbors each (80 directed edges), redundancy is huge.
Track `sent_to` and `received_from` per digest per neighbor. Use hash-based
forwarder selection but adapt for 32-node topology structure.

### Tier 2 — Medium impact

#### 5. Adaptive strategy per round
- Round 1: full sketch (BF+RIBLT or RIBLT)
- Round 2+: if many new elements, eager-forward; if few, consider a fresh
  sketch to clean up remaining diffs precisely
- Late rounds: direct element broadcast for tiny diffs (< 50 elements)

#### 6. m_ratio tuning
With 50% Jaccard and 10k elements, the BF needs to handle ~3,333 diffs.
A higher m_ratio (larger BF) means better filtering but more metadata.
Try m_ratio = 0.3 to 0.8 and see what works at this scale.

#### 7. Star: hub aggregation
The hub sees 31 leaves. After round 1, hub knows what each leaf has.
Hub can send targeted missing-element lists to each leaf, and leaves
send only their unique elements to hub. 2-3 rounds total.

### Tier 3 — Advanced

#### 8. Multiparty sketch (MSketch)
`src/simulator/algorithms/multiparty_sketch.rs` implements finite-field
coded sketches with `add_sketch()` / `sub_sketch()` / `try_decode()`.
This enables aggregating multiple nodes' sketches along tree paths —
potentially much more efficient than pairwise reconciliation at 32 nodes.

---

## General principles

- **Metadata scales with edges × diff_size.** At 32 nodes, there are many
  more edges. Minimizing per-edge metadata is critical.
- **50% Jaccard means BF pre-filtering is valuable.** Each pair has ~3,333
  diffs. A BF at 5 bits/element costs 50 KB to identify most of them,
  vs RIBLT which costs ~3,333 × 1.5 × 8 = 40 KB per pair. The BF is
  competitive and reduces RIBLT input.
- **Rounds are cheap (cap=100), bytes are expensive.** Spending more rounds
  to reduce bytes is a good trade.
- **Test one thing at a time.** The codebase is complex. Isolate changes.
