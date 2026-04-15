# Strategy Hints for Bandwidth Researcher Agent

These hints are ordered by expected impact. Use them as starting points —
combine, adapt, or invent new approaches as results dictate.

Read `experiments.tsv` before each attempt to avoid re-exploring dead ends.

---

## Understanding the problem

8 replicas, each with ~10,000 elements (40 B wire size), 10% divergence.
That means ~1,000 unique elements per replica, ~9,000 shared, union ~16,091.

**Theoretical minimum state bytes** (elements that must cross the wire):
- Star: ~2.2 MB (leaves→hub + hub→leaves, no redundancy possible)
- Tree: ~2.0–2.5 MB (each element traverses each edge at most once)
- Chord: ~2.5–3.5 MB (smart routing: each element crosses 2–3 edges)

Current fitness is 11.4 M. The floor is roughly 7–8 M (state only). The gap
is metadata overhead and redundant element transfers.

**Chord is 49% of the fitness** and has the most headroom (~2 M saveable).
Prioritise Chord, then Tree, then Star.

---

## Tier 1 — High impact, directly implementable

### 1. Chord: gossip-suppression via received-set tracking

**Problem:** On Chord (16 edges, ~5 neighbours per node), every node sends a
full RIBLT sketch to every neighbour every round. When node A receives
elements from B, then A reconciles with C, A may send those same elements
to C — but C may have already received them directly from B (since B and C
are also neighbours).

**Idea:** Maintain a per-replica `received_from: HashMap<u64, HashSet<usize>>`
that records, for each digest, which neighbours sent it. When building the
"to_send" list for neighbour X, skip elements that were received from a
node that is also a neighbour of X (since X likely already has them).

Alternatively, simpler: track a `sent_to: HashMap<usize, HashSet<u64>>` —
never re-send a digest to a neighbour you've already sent it to.

**Expected saving:** 20–30% on Chord (1–1.5 M bytes).

### 2. Tree: directed upward-downward flow

**Problem:** The current protocol treats Tree edges as bidirectional peer
pairs, so leaves waste bandwidth sending data upward that the parent already
received from a sibling. Tree takes 10 rounds.

**Idea:** Two-phase protocol keyed on `topology.kind == Tree`:
- **Upward phase** (rounds 1..depth): each node reconciles only with its
  `topology.parent()`. The parent accumulates children's data naturally.
  Don't send downward yet.
- **Downward phase** (rounds depth+1..2×depth): the root (which now has the
  full union) pushes missing elements to children. Children forward to their
  children.

For 8 nodes (depth 3), this is ~6 rounds vs current 10.

Use `topology.parent(id)`, `topology.children(id)`, `topology.is_root(id)`.

**Expected saving:** 15–25% on Tree bytes + fewer rounds.

### 3. Direct element broadcast for small diffs

**Problem:** BF + RIBLT has fixed metadata overhead. When the diff is tiny
(say < 50 elements), the metadata (BF + RIBLT sketch) costs more than just
sending the elements themselves (50 × 40 B = 2 KB).

**Idea:** Track `new_elements_since_last_round: Vec<Element>`. If the count
is below a threshold (e.g., 100), skip BF/RIBLT entirely and broadcast them
as `ProtocolMsg::Elements`. This is especially useful in rounds 3+ when most
convergence is done.

Combine with BF-skip: if no new elements, skip entirely (already done).
If few new elements, direct broadcast. If many, use BF/RIBLT.

**Expected saving:** reduces late-round metadata waste across all topologies.

### 4. Adaptive diff estimation → strategy selection

**Idea:** Before choosing a strategy for a given neighbour, estimate the diff
size. Simple heuristic: `|my_set| + |their_last_known_set| - 2 × |expected_common|`.
Or track the actual diff size from the previous round's reconciliation.

- diff < 50: direct element broadcast (Tier 1.3)
- 50 < diff < 500: RIBLT only (O(|diff|) metadata, no BF overhead)
- diff > 500: BF + RIBLT hybrid

Currently, RIBLT is used for Chord and BF+RIBLT for Star/Tree regardless of
diff size. Making this adaptive per-neighbour and per-round could help.

---

## Tier 2 — Medium impact

### 5. Star: hub-first 2-round protocol

**Idea:** The hub (node 0) is connected to all leaves.
- **Round 1:** All leaves send BF/RIBLT to hub. Hub reconciles and learns
  the full union.
- **Round 2:** Hub sends each leaf exactly the elements it's missing
  (hub knows each leaf's state from round 1's reconciliation).
- **Round 3 (if needed):** Leaves confirm or send any remaining diffs.

Target: 2–3 rounds instead of 4. The hub has perfect knowledge after round 1.

### 6. Piggybacking elements alongside metadata

**Idea:** When sending a BF/RIBLT to neighbour B, also include elements that
you recently received from other neighbours (that B likely doesn't have).
This combines metadata and state transfer in one round, reducing round count.

Implementation: alongside the sketch message, send a small `Elements` message
with recently acquired elements (limited to, say, 200 elements to cap cost).

### 7. m_ratio tuning

Current m_ratio = 0.5. This controls BF size (bits per element).

- **Early rounds** (large diffs): lower m_ratio → smaller BF, more RIBLT.
  The BF overhead is less justified when diffs are large.
- **Late rounds** (small diffs): higher m_ratio → larger BF, less RIBLT.
  BF catches more false positives, RIBLT handles fewer.
- **Per-topology:** Star (few edges) can afford larger BFs. Chord with many
  edges should minimise per-edge metadata.

Try: m_ratio = 0.3 for Chord (if switching from pure RIBLT), m_ratio = 0.7
for Star/Tree early rounds, m_ratio = 0.3 for late rounds.

### 8. Cumulative digest hash for early termination

**Idea:** Each node computes `XOR(all digests)` as a fast fingerprint.
If a node's fingerprint matches the expected union fingerprint (the target),
it's converged — skip all further messages from that node.

This is cheaper than BF-skip because it doesn't require per-neighbour
tracking. A single XOR fingerprint detects global convergence.

Could save 1–2 rounds of unnecessary metadata exchange at the tail end.

---

## Tier 3 — Multiparty sketch aggregation (advanced)

### 9. Use `multiparty_sketch::MSketch` for tree/star aggregation

`src/simulator/algorithms/multiparty_sketch.rs` implements a finite-field
(GF(P)) coded sketch that supports:
- `add_sketch()` — aggregate multiple contributors' sketches
- `sub_sketch()` — compute the diff between aggregated sketch and local
- `try_decode()` — peeling decoder to recover missing digests

Unlike XOR-based RIBLT, the field arithmetic correctly handles elements that
appear at multiple contributors (k copies contribute k×e, recoverable via
modular inverse). This is exactly what's needed for multiparty.

**Tree protocol:**
1. Leaves build MSketch, send upward.
2. Internal nodes aggregate children's sketches via `add_sketch()`, forward up.
3. Root has the aggregated sketch of the full network; subtracts its own
   sketch to find what it's missing.
4. Root sends missing elements downward.

**Billing:** An MCell is 24 bytes (3 × u64). Bill as
`RibltSketch { symbols: num_cells * 3 }` which gives `num_cells × 24` bytes
of metadata — a fair approximation.

**Hint data:** Use `SimulatorHint::RibltDigests` to pass the actual digests
for sketch reconstruction (since the simulator is single-machine, the sketch
is rebuilt from digests, but billed correctly via ProtocolMsg).

**Why this matters:** In a tree with 8 nodes, instead of 7 independent
pairwise reconciliations (each with its own RIBLT overhead), you get a
single aggregated sketch flowing up/down. The metadata cost scales with
|total symmetric difference| rather than |edges| × |pairwise diff|.

### 10. Chord: hash-based routing to reduce flooding

**Idea:** Assign each element a "home node" based on `digest % num_nodes`.
Elements flow toward their home node first, which then distributes to
neighbours. This imposes a structure on the gossip, reducing random flooding.

Alternatively, designate a primary flow direction (e.g., clockwise on the
ring) so elements primarily propagate in one direction, with backwards
transfers only for missed elements.

---

## General principles

- **Metadata is the enemy.** Every BF or RIBLT sketch has fixed overhead.
  On Chord (16 edges), that overhead multiplies by 16. Reducing per-edge
  metadata or reducing the number of edges that need metadata is key.
- **Rounds are cheap, bytes are expensive.** An extra round costs nothing
  in the fitness function. If spending one more round saves metadata, do it.
  (But note: round cap is 20.)
- **Late rounds are wasteful.** After 2–3 rounds, most nodes are nearly
  converged. The current BF-skip helps, but switching to direct element
  broadcast or skipping entirely can save more.
- **Test one thing at a time.** Change one variable per experiment so you
  know what worked. If a combined change fails, try the components separately.
