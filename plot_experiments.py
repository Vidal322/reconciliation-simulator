#!/usr/bin/env python3
"""Plot autoresearch experiment progress from experiments.tsv."""

import csv
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

TSV = "experiments.tsv"

# --- Load data ---
attempts, fitness, star, tree, chord, kept = [], [], [], [], [], []
descriptions = []

with open(TSV) as f:
    reader = csv.DictReader(f, delimiter="\t")
    for row in reader:
        attempts.append(int(row["attempt"]))
        fitness.append(int(row["fitness"]))
        star.append(int(row["star_bytes"]))
        tree.append(int(row["tree_bytes"]))
        chord.append(int(row["chord_bytes"]))
        kept.append(row["kept"].strip() == "yes")
        descriptions.append(row["description"].strip())

attempts = np.array(attempts)
fitness = np.array(fitness)
star = np.array(star)
tree = np.array(tree)
chord = np.array(chord)
kept = np.array(kept)

# --- Compute best-so-far envelope ---
best_fitness = np.minimum.accumulate(fitness)
best_star = star.copy()
best_tree = tree.copy()
best_chord = chord.copy()
for i in range(1, len(fitness)):
    if best_fitness[i] == best_fitness[i - 1] and fitness[i] != best_fitness[i]:
        best_star[i] = best_star[i - 1]
        best_tree[i] = best_tree[i - 1]
        best_chord[i] = best_chord[i - 1]

# --- Reference baselines ---
baselines = {
    "FullStateTransfer": 89_678_000,
    "HybridRbfRiblt": 12_733_675,
    "Riblt": 13_221_632,
    "StaticBfIblt": 17_294_358,
}

# --- Figure ---
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 9), gridspec_kw={"height_ratios": [2, 1]})
fig.suptitle("Autoresearch: Multiparty Set Reconciliation", fontsize=14, fontweight="bold")

# --- Top panel: fitness over experiments ---
# All attempts (faded)
ax1.scatter(attempts[~kept], fitness[~kept], color="salmon", alpha=0.4,
            s=30, zorder=3, label="Reverted")
ax1.scatter(attempts[kept], fitness[kept], color="steelblue", alpha=0.5,
            s=30, zorder=3, label="Kept")

# Best-so-far line
ax1.step(attempts, best_fitness, where="post", color="darkblue",
         linewidth=2.5, zorder=4, label="Best so far")

# Annotate improvements
for i in range(len(attempts)):
    if kept[i] and (i == 0 or best_fitness[i] < best_fitness[i - 1]):
        ax1.annotate(
            descriptions[i][:50],
            (attempts[i], fitness[i]),
            textcoords="offset points", xytext=(8, 8),
            fontsize=6.5, color="darkblue", alpha=0.8,
            arrowprops=dict(arrowstyle="-", color="grey", alpha=0.4),
        )

# Reference baselines
for name, val in baselines.items():
    if val < ax1.get_ylim()[1] * 1.5:
        ax1.axhline(y=val, color="grey", linestyle="--", alpha=0.4, linewidth=1)
        ax1.text(attempts[-1] + 0.3, val, name, fontsize=7, color="grey",
                 va="center", alpha=0.7)

ax1.set_ylabel("Total bytes sent (fitness)", fontsize=11)
ax1.set_xlabel("Experiment #", fontsize=11)
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x / 1e6:.1f}M"))
ax1.legend(loc="upper right", fontsize=9)
ax1.grid(True, alpha=0.2)

# Improvement annotation
if len(fitness) > 1:
    imp = (fitness[0] - best_fitness[-1]) / fitness[0] * 100
    ax1.text(
        0.02, 0.05,
        f"Improvement: {imp:.1f}%  ({fitness[0]:,} → {best_fitness[-1]:,})\n"
        f"Experiments: {len(fitness)} total, {kept.sum()} kept",
        transform=ax1.transAxes, fontsize=9,
        bbox=dict(boxstyle="round,pad=0.4", facecolor="lightyellow", alpha=0.8),
    )

# --- Bottom panel: per-topology breakdown ---
width = 0.25
x = np.arange(len(attempts))
ax2.bar(x - width, best_star / 1e6, width, label="Star", color="gold", alpha=0.8)
ax2.bar(x, best_tree / 1e6, width, label="Tree", color="forestgreen", alpha=0.8)
ax2.bar(x + width, best_chord / 1e6, width, label="Chord", color="steelblue", alpha=0.8)

ax2.set_xlabel("Experiment # (kept improvements only shown)", fontsize=11)
ax2.set_ylabel("Bytes sent (M)", fontsize=11)
ax2.set_xticks(x)
ax2.set_xticklabels([str(a) for a in attempts], fontsize=8)
ax2.legend(loc="upper right", fontsize=9)
ax2.grid(True, axis="y", alpha=0.2)

plt.tight_layout()
plt.savefig("fig_experiments.png", dpi=150, bbox_inches="tight")
print(f"Saved fig_experiments.png ({len(fitness)} experiments)")
