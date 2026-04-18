#!/usr/bin/env python3
"""Plot autoresearch experiment progress from experiments.tsv (v3 format)."""

import csv
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

TSV = "experiments.tsv"

# --- Load data ---
attempts, fitness, kept_list, descriptions = [], [], [], []

with open(TSV) as f:
    reader = csv.DictReader(f, delimiter="\t")
    for row in reader:
        attempts.append(int(row["attempt"]))
        fit = float(row["fitness"])
        fitness.append(fit)
        kept_list.append(row["kept"].strip() == "yes")
        descriptions.append(row["description"].strip())

attempts = np.array(attempts)
fitness = np.array(fitness)
kept = np.array(kept_list)

# Cap infinite fitness for plotting
plot_fitness = np.where(fitness > 1e12, np.nan, fitness)

# --- Compute best-so-far envelope ---
best_fitness = np.full_like(fitness, np.inf)
current_best = np.inf
for i in range(len(fitness)):
    if kept[i] and fitness[i] < current_best:
        current_best = fitness[i]
    best_fitness[i] = current_best

# --- Reference baseline ---
FST_FITNESS = 459_237_474

# --- Figure ---
fig, ax = plt.subplots(1, 1, figsize=(12, 6))
fig.suptitle("Autoresearch: Multiparty Set Reconciliation (Full Matrix)",
             fontsize=14, fontweight="bold")

# All attempts
reverted = ~kept & ~np.isnan(plot_fitness)
ax.scatter(attempts[reverted], plot_fitness[reverted], color="salmon", alpha=0.4,
           s=30, zorder=3, label="Reverted")
ax.scatter(attempts[kept], plot_fitness[kept], color="steelblue", alpha=0.5,
           s=30, zorder=3, label="Kept")

# Best-so-far line
valid_best = best_fitness < 1e12
if valid_best.any():
    ax.step(attempts[valid_best], best_fitness[valid_best], where="post",
            color="darkblue", linewidth=2.5, zorder=4, label="Best so far")

# FST baseline
ax.axhline(y=FST_FITNESS, color="red", linestyle="--", alpha=0.5, linewidth=1)
ax.text(attempts[-1] + 0.3, FST_FITNESS, "FullStateTransfer", fontsize=8,
        color="red", va="center", alpha=0.7)

# Annotate key improvements
for i in range(len(attempts)):
    if kept[i] and (i == 0 or best_fitness[i] < best_fitness[i - 1]):
        desc = descriptions[i][:55]
        ax.annotate(
            desc,
            (attempts[i], fitness[i]),
            textcoords="offset points", xytext=(8, 8),
            fontsize=6, color="darkblue", alpha=0.8,
            arrowprops=dict(arrowstyle="-", color="grey", alpha=0.4),
        )

ax.set_ylabel("Fitness (geometric mean bytes)", fontsize=11)
ax.set_xlabel("Experiment #", fontsize=11)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x / 1e6:.0f}M"))
ax.legend(loc="upper right", fontsize=9)
ax.grid(True, alpha=0.2)

# Improvement annotation
if len(fitness) > 1 and current_best < np.inf:
    imp = (FST_FITNESS - current_best) / FST_FITNESS * 100
    ax.text(
        0.02, 0.05,
        f"Improvement vs FST: {imp:.1f}%  ({FST_FITNESS:,} → {current_best:,.0f})\n"
        f"Experiments: {len(fitness)} total, {kept.sum()} kept, {(~kept).sum()} reverted",
        transform=ax.transAxes, fontsize=9,
        bbox=dict(boxstyle="round,pad=0.4", facecolor="lightyellow", alpha=0.8),
    )

plt.tight_layout()
plt.savefig("fig_experiments.png", dpi=150, bbox_inches="tight")
print(f"Saved fig_experiments.png ({len(fitness)} experiments)")
