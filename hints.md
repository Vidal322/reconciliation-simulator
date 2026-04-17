# Strategy Hints for Bandwidth Researcher Agent

## Current state (after 13 experiments)

Best fitness: **260,060,241** (attempt 12).
- Star: 77.5M (3 rounds) — near theoretical minimum (~73M)
- Tree: 77.5M (10 rounds) — near theoretical minimum
- Chord: 105.0M (10 rounds) — **40% of total, main target**

We are **51.6% below HybridRbfRiblt** (537M).

---

## What we know works

- **Star/Tree**: single BF+RIBLT sketch (m_ratio=1.0) + eager forwarding. Solved.
- **Chord**: sequential distance-doubling (power 0→4, ascending). BF+RIBLT
  hybrid with chord_m_ratio=1.0. Pairwise, no eager forwarding.

## What we know FAILS — do NOT retry

- **Hash-based forwarder for Chord** (attempt 2): fails to converge
- **Eager forwarding on Chord** (attempts 1, 4): cascade storms, doubles bandwidth
- **Descending power order** (attempt 10): 77% worse, ascending is essential
- **chord_m_ratio tuning** (attempts 8-9): 0.7 and 1.3 both ≈105M, insensitive
- **Pure RIBLT for Chord** (attempt 11): 10% worse than BF+RIBLT. BF
  pre-filtering is essential at 50% Jaccard.
- **Skip re-sketch if unchanged** (attempt 6): condition never fires

---

## Priority ideas to try (Chord-focused)

### 1. Asymmetric / unidirectional sketch (HIGH PRIORITY)

Currently both A→B and B→A sketch simultaneously on each Chord edge.
Each direction incurs full BF+RIBLT metadata cost. Instead:

- **Round N**: A sends sketch to B only (not B→A).
- B decodes, learns what A has that B doesn't, and vice versa.
- **Round N+1**: B sends A the missing elements. A sends B the missing
  elements. No sketch needed for B→A — B already decoded the diff.

This could **halve the metadata** on Chord (~35M → ~17M), saving ~18M.
The key insight: when B decodes A's sketch, B learns BOTH directions of
the diff (local_only AND remote_only). B can immediately queue elements
for A without A ever sketching to B.

Implementation: in the distance-doubling loop, for each power, only send
the sketch in ONE direction (e.g., lower-id → higher-id). The receiver
decodes both local_only (queue for sender) and the sender's missing
elements (already in pending from decode). The sender gets elements back
in the next element-drain round without having sent its own sketch.

### 2. Direct element transfer after first cycle (HIGH PRIORITY)

After the first distance-doubling cycle (powers 0-4, 10 rounds), most
nodes have most elements. The remaining diffs per pair are tiny (maybe
< 50 elements). Starting a SECOND cycle of BF+RIBLT sketches is wasteful —
the fixed BF overhead (~10KB per sketch) exceeds the cost of just sending
the remaining elements directly.

Implementation: after `chord_sketch_power >= 5`, switch to a cleanup mode:
- Each node sends all its elements as digests (8B each) to neighbors
- Neighbors compare and request only missing elements
- Or simply broadcast remaining new elements (since last round) directly

This could save 1-2 rounds of unnecessary BF+RIBLT overhead.

### 3. Skip sketch to already-converged neighbors

Track the set size reported by each neighbor (via elements received).
If a neighbor's set appears to match yours (same size, no new elements
exchanged in the last round), skip the sketch to that neighbor.

This is different from attempt 6 which checked OUR set size. Here we
check the PAIR's convergence state.

### 4. Combine distance-doubling with limited post-cycle forwarding

After the first distance-doubling cycle (5 sketch rounds), enable a
single round of eager forwarding ONLY for elements received in the last
sketch round. This is limited (not all accumulated elements, just the
latest batch) so it won't cascade, but it reaches nodes that may still
be missing a few elements, potentially avoiding a second sketch cycle.

### 5. Reduce metadata per sketch with adaptive m_ratio per power

All 5 distance-doubling rounds use the same m_ratio=1.0. But:
- Power 0 (distance 1, close neighbors): diff is largest → BF is most
  valuable → m_ratio=1.0 or higher is good
- Power 4 (distance 16, far neighbors): prior rounds have reduced the
  diff significantly → BF overhead may exceed savings → try lower m_ratio
  (0.5) or even pure RIBLT for power 4 only

Note: attempt 11 tried pure RIBLT for powers 1-4 and it was worse.
But pure RIBLT for ONLY power 4 (smallest remaining diff) might help.

---

## Lower priority ideas

### 6. Tree: directed upward-downward flow
Tree is at 77.5M in 10 rounds. A directed approach (children→parent upward,
root→children downward) might save some bytes by avoiding redundant sibling
forwarding. Small gain expected (~2-5M).

### 7. Star: skip hub's outbound sketch
The hub receives sketches from all 31 leaves and decodes the full union.
The hub doesn't need to send its OWN sketch — it can just send the missing
elements directly to each leaf based on what it decoded. Saves 31 sketch
messages from hub.

---

## General principles

- **Chord is the bottleneck at 105M (40% of total).** Focus there.
- **Rounds are cheap (cap=100), bytes are expensive.**
- **Log every attempt** (success AND failure) with `kept: yes/no`.
- **Check experiments.tsv before each attempt** to avoid retrying failed ideas.
- **Test one thing at a time.** Isolate changes.
