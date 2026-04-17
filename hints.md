# Strategy Hints for Bandwidth Researcher Agent

## Workload context

32 replicas, 10,000 elements each, Jaccard similarity 0.5, 200k universe.
Each pair shares ~6,667 elements and each has ~3,333 unique. Union ~66,002.

---

## What we know from 11 experiments so far

Current best: **260.6M** (attempt 7). Star=77.8M, Tree=77.7M, Chord=105.1M.
HybridRbfRiblt baseline: 537M. We are **51% below** the best reference.

### What works

1. **Star/Tree: single BF+RIBLT sketch + eager forwarding** — one sketch
   in round 1, then forward all newly received elements to all neighbors
   with `sent_to` dedup. Converges in 3 rounds (Star) / 10 rounds (Tree).
   Delivers ~78M per topology. This is essentially solved.

2. **Chord: sequential distance-doubling** — instead of sketching all 5
   neighbors every round, sketch only the pair at distance 2^power each
   round (power cycles 0→4). This means each directed edge is sketched
   exactly once, reducing decode events from 864 to 288. Chord went from
   276M to 105M in one step. Ascending order (close→far) is essential:
   reconcile nearby first so their elements help when reconciling distant.

3. **BF+RIBLT hybrid for Chord** — at 50% Jaccard, BF pre-filtering helps
   even on Chord. chord_m_ratio=1.0 is optimal (tested 0.7 and 1.3).

### What does NOT work at 32 nodes

- **Hash-based forwarder selection for Chord** (attempt 2): the designated
  forwarder may not have the element yet → Chord fails to converge even
  at 100 rounds. DO NOT retry this approach.
- **Source-neighbor-suppressed eager forwarding for Chord** (attempt 4):
  cascade still too strong, state bytes doubled. Eager forwarding on
  32-node Chord is fundamentally problematic — too many paths.
- **Descending power order** for Chord (attempt 10): 186M vs 105M ascending.
  Close-first is essential for element accumulation.
- **chord_m_ratio tuning** around 1.0 (attempts 8-9): insensitive. 0.7 and
  1.3 both give ~105M. Don't waste experiments on fine BF tuning.

---

## Where the remaining opportunity is

Star (78M) and Tree (78M) seem near-optimal. **Chord (105M) is 40% of the
total** and the main target. Current Chord breakdown (approximate):
- State bytes: ~70M (actual elements transferred)
- Metadata bytes: ~35M (BF + RIBLT sketches)

### Ideas to explore for Chord

#### 1. Selective eager forwarding after distance-doubling
Currently Chord does NO eager forwarding (it was disabled because naive
forwarding cascades). But after completing the distance-doubling cycle
(5 rounds), most nodes have most elements. A final selective forward
round — only send elements to neighbors whose set size is smaller — might
converge stragglers cheaply without cascading.

#### 2. Skip redundant sketch rounds
After the distance-doubling cycle completes (power 0→4, covering distances
1,2,4,8,16), a second cycle of 5 sketch rounds starts. By then the sets
are highly similar. Consider: after the first cycle, switch to direct
element transfer for remaining diffs (< 100 elements per pair), skipping
the BF+RIBLT overhead entirely.

#### 3. BF-skip on Chord
If a node's set hasn't grown since its last sketch to a specific neighbor,
skip that sketch. Attempt 6 tried this but the condition never fired because
sets always grow during the first cycle. Try it only for the SECOND cycle
(power >= 5).

#### 4. Combine distance-doubling with limited eager forwarding
Instead of fully disabling eager forwarding on Chord, enable it but only
for the first round (power 0, distance 1). After the closest neighbors
reconcile, forward elements received from them to distance-2 neighbors.
This pre-loads distant neighbors before their sketch round, reducing the
sketch diff size.

#### 5. Reduce metadata per sketch
Each BF+RIBLT sketch at m_ratio=1.0 costs ~10k bits BF + RIBLT symbols.
Consider:
- Use pure RIBLT (no BF) for the close neighbors (power 0-1) where the
  diff is largest and BF overhead is relatively small
- Use BF+RIBLT for distant neighbors (power 3-4) where prior rounds have
  already reduced the diff and BF is very effective

#### 6. Asymmetric sketch: only one direction per round
Currently both A→B and B→A sketch simultaneously. If A sketches to B in
round N, B learns what A has. B can then send missing elements to A in
round N+1 WITHOUT sketching back. This halves the metadata.

### Ideas for Star/Tree (incremental improvements)

#### 7. Tree: directed upward then downward
Currently Tree uses the same strategy as Star (sketch all neighbors round 1,
then eager forward). A directed approach might save some bytes:
- Upward: children sketch to parent only (not bidirectional)
- Downward: root pushes missing elements to children
This avoids redundant sibling-to-parent-to-sibling forwarding.

#### 8. Star: hub-aware element distribution
The hub receives sketches from all 31 leaves in round 1. It knows exactly
what each leaf has. Instead of eager-forwarding everything, the hub could
send each leaf only what it's missing — no dedup overhead, no wasted sends.

---

## General principles

- **Chord is the bottleneck.** Focus experiments there.
- **Rounds are cheap (cap=100), bytes are expensive.**
- **Test one thing at a time.** Log failures with `kept: no`.
- **Don't retry failed approaches** — check experiments.tsv first.
- **Star/Tree are near-optimal at ~78M** — small gains possible but not the priority.
