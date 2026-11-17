"""Turn the benchmark CSVs and timelines into PNG plots.

    python benchmarks/plot.py                       # reads benchmarks/results/, writes benchmarks/plots/
    python benchmarks/plot.py --results DIR --out DIR

Each plot uses the median over repetitions, with a thin line spanning the
fastest and slowest run. Only runs that succeeded with correct output count.
"""

import argparse
import csv
import json
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

BENCH_DIR = Path(__file__).resolve().parent

# Light-mode reference palette: two categorical slots, neutral ink for text.
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
GRID = "#e4e3df"
SERIES = ["#2a78d6", "#eb6834"]  # slot 1 blue, slot 2 orange

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK_SECONDARY, "text.color": INK,
    "xtick.color": INK_SECONDARY, "ytick.color": INK_SECONDARY,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
    "axes.spines.top": False, "axes.spines.right": False,
    "font.size": 10, "axes.titlesize": 11, "axes.titleweight": "bold", "legend.frameon": False,
})


def read_rows(csv_path):
    """Rows that succeeded with correct output, with numeric fields converted."""
    if not Path(csv_path).exists():
        return []
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["state"] != "succeeded" or row["correct"] != "True":
                continue
            for field in ("workers", "maps", "reduces", "intermediate_bytes"):
                row[field] = int(row[field])
            for field in ("delay_s", "map_s", "reduce_s", "total_s", "wall_s",
                          "kill_s", "expired_s", "reassigned_s"):
                row[field] = float(row[field]) if row.get(field) not in (None, "") else None
            row["speculation"] = row["speculation"] == "True"
            row["combine"] = row["combine"] == "True"
            rows.append(row)
    return rows


def summary(values):
    """(median, min, max) of a non-empty list."""
    return statistics.median(values), min(values), max(values)


def grouped(rows, key, value):
    groups = {}
    for row in rows:
        if row[value] is not None:
            groups.setdefault(key(row), []).append(row[value])
    return {k: summary(v) for k, v in sorted(groups.items())}


def range_bar(ax, x, low, high):
    ax.plot([x, x], [low, high], color=INK_SECONDARY, linewidth=1.2, solid_capstyle="round", zorder=3)


def save(fig, out_dir, name):
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / name
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_parallelism(rows, out_dir):
    times = grouped(rows, lambda r: r["workers"], "total_s")
    if not times or 1 not in times:
        return None
    workers = list(times)
    base = times[1][0]
    fig, (left, right) = plt.subplots(1, 2, figsize=(9, 3.6))

    for i, w in enumerate(workers):
        median, low, high = times[w]
        left.bar(i, median, width=0.6, color=SERIES[0])
        range_bar(left, i, low, high)
        left.annotate(f"{median:.1f}s", (i, high), textcoords="offset points", xytext=(0, 4),
                      ha="center", color=INK, fontsize=9)
    left.set_xticks(range(len(workers)), [str(w) for w in workers])
    left.set_xlabel("workers (1 CPU each)")
    left.set_ylabel("job time (s)")
    left.set_title("Median job time")
    left.grid(axis="x", visible=False)

    speedup = [base / times[w][0] for w in workers]
    right.plot(workers, workers, color=INK_SECONDARY, linewidth=1, linestyle="--")
    right.annotate("ideal", (workers[-1], workers[-1]), textcoords="offset points", xytext=(-4, 4),
                   ha="right", color=INK_SECONDARY, fontsize=9)
    right.plot(workers, speedup, color=SERIES[0], linewidth=2, marker="o", markersize=7)
    for w, s in zip(workers, speedup):
        right.annotate(f"{s:.2f}x", (w, s), textcoords="offset points", xytext=(8, -12), color=INK, fontsize=9)
    right.set_xticks(workers)
    right.set_xlabel("workers (1 CPU each)")
    right.set_ylabel("speedup vs. 1 worker")
    right.set_title("Speedup")
    right.set_ylim(0, max(workers) + 0.5)
    return save(fig, out_dir, "parallelism.png")


def plot_on_off(rows, out_dir, flag, labels, title, filename, by_delay=False):
    """Grouped bars comparing a feature on vs. off (optionally per injected delay)."""
    groups = sorted({r["delay_s"] for r in rows}) if by_delay else [None]
    times = grouped(rows, lambda r: (r["delay_s"] if by_delay else None, r[flag]), "total_s")
    if not times:
        return None
    fig, ax = plt.subplots(figsize=(6.4, 3.6))
    width = 0.36
    for slot, (state, label) in enumerate(zip((True, False), labels)):
        xs = []
        for g_index, group in enumerate(groups):
            if (group, state) not in times:
                continue
            x = g_index + (slot - 0.5) * (width + 0.04)
            median, low, high = times[(group, state)]
            ax.bar(x, median, width=width, color=SERIES[slot], label=label if not xs else None)
            range_bar(ax, x, low, high)
            ax.annotate(f"{median:.1f}s", (x, high), textcoords="offset points", xytext=(0, 4),
                        ha="center", color=INK, fontsize=9)
            xs.append(x)
    if by_delay:
        ax.set_xticks(range(len(groups)), [f"{g:g}s delay" for g in groups])
        ax.set_xlabel("delay added to one of four workers")
    else:
        ax.set_xticks([])
    ax.set_ylabel("job time (s)")
    ax.set_title(title)
    ax.grid(axis="x", visible=False)
    ax.legend(loc="upper left", bbox_to_anchor=(1, 1))
    return save(fig, out_dir, filename)


def plot_combiner_bytes(rows, out_dir):
    sizes = grouped(rows, lambda r: r["combine"], "intermediate_bytes")
    if not sizes:
        return None
    fig, ax = plt.subplots(figsize=(6.4, 3.2))
    order = [(True, "with combiner"), (False, "without combiner")]
    for i, (state, label) in enumerate(order):
        if state not in sizes:
            continue
        median_mb = sizes[state][0] / 1e6
        ax.barh(i, median_mb, height=0.55, color=SERIES[i])
        ax.annotate(f"{median_mb:.1f} MB", (median_mb, i), textcoords="offset points", xytext=(6, 0),
                    va="center", color=INK, fontsize=9)
    ax.set_yticks(range(len(order)), [label for _, label in order])
    ax.invert_yaxis()
    ax.set_xlabel("intermediate data written by map tasks (MB)")
    ax.set_title("Combiner: data shuffled to reducers")
    ax.grid(axis="y", visible=False)
    return save(fig, out_dir, "combiner_bytes.png")


def read_timeline(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def plot_timeline(events, title, out_path, kill_s=None):
    """One lane per worker; a bar per attempt from assignment to commit or expiry."""
    starts = {e["attempt_id"]: e for e in events if e["event"] == "assigned"}
    ends = {e["attempt_id"]: e for e in events if e["event"] in ("committed", "expired")}
    if not starts:
        return None
    workers = sorted({e["worker_id"] for e in starts.values()})
    lane = {w: i for i, w in enumerate(workers)}
    last = max(e["elapsed_s"] for e in events)

    fig, ax = plt.subplots(figsize=(8, 0.5 * len(workers) + 1.6))
    labelled = set()
    for attempt, start in starts.items():
        end = ends.get(attempt)
        finish = end["elapsed_s"] if end else last
        expired = bool(end and end["event"] == "expired")
        kind = "backup attempt" if start.get("speculative") else "attempt"
        if expired:
            kind += " (expired)"
        color = SERIES[1] if start.get("speculative") else SERIES[0]
        ax.barh(lane[start["worker_id"]], finish - start["elapsed_s"], left=start["elapsed_s"],
                height=0.6, color=color, edgecolor=SURFACE, linewidth=2,
                hatch="//" if expired else None,
                label=kind if kind not in labelled else None)
        labelled.add(kind)
        ax.annotate(start["task_id"], (start["elapsed_s"] + (finish - start["elapsed_s"]) / 2,
                                       lane[start["worker_id"]]),
                    ha="center", va="center", color=SURFACE, fontsize=7)
    if kill_s is not None:
        ax.axvline(kill_s, color=INK, linewidth=1.2, linestyle="--")
        ax.annotate("worker killed", (kill_s, 1.0), xycoords=("data", "axes fraction"),
                    textcoords="offset points", xytext=(4, 2), va="bottom", color=INK, fontsize=9)
    ax.set_yticks(range(len(workers)), [w[:8] for w in workers])
    ax.set_ylabel("worker")
    ax.set_xlabel("seconds since job accepted")
    ax.set_title(title, pad=16 if kill_s is not None else 6)
    ax.grid(axis="y", visible=False)
    ax.legend(loc="upper left", bbox_to_anchor=(1, 1))
    return save(fig, out_path.parent, out_path.name)


def plot_worker_loss(rows, results_dir, out_dir):
    recovered = [r for r in rows if r["expired_s"] is not None and r["reassigned_s"] is not None]
    if not recovered:
        return []
    paths = []
    fig, ax = plt.subplots(figsize=(6.4, 3.2))
    for i, (field, label) in enumerate((("expired_s", "attempt expired"),
                                        ("reassigned_s", "task reassigned"))):
        median, low, high = summary([r[field] for r in recovered])
        ax.barh(i, median, height=0.55, color=SERIES[0])
        ax.plot([low, high], [i, i], color=INK_SECONDARY, linewidth=1.2)
        ax.annotate(f"{median:.1f}s", (high, i), textcoords="offset points", xytext=(6, 0),
                    va="center", color=INK, fontsize=9)
    ax.set_yticks([0, 1], ["attempt expired", "task reassigned"])
    ax.invert_yaxis()
    ax.set_xlabel("seconds after the worker was killed")
    ax.set_title(f"Worker loss: recovery time ({len(recovered)} runs)")
    ax.grid(axis="y", visible=False)
    paths.append(save(fig, out_dir, "worker_loss.png"))

    example = recovered[0]
    timeline = results_dir / "timelines" / f"{example['job_id']}.jsonl"
    if timeline.exists():
        paths.append(plot_timeline(read_timeline(timeline), "Worker loss: task timeline",
                                   out_dir / "worker_loss_timeline.png", kill_s=example["kill_s"]))
    return paths


def plot_speculation_timeline(rows, results_dir, out_dir):
    """Timeline of the speculative run that started the most backups."""
    best = None
    for row in rows:
        if not row["speculation"]:
            continue
        path = results_dir / "timelines" / f"{row['job_id']}.jsonl"
        if not path.exists():
            continue
        events = read_timeline(path)
        backups = sum(1 for e in events if e["event"] == "assigned" and e.get("speculative"))
        if best is None or backups > best[0]:
            best = (backups, events, row)
    if best is None:
        return None
    _, events, row = best
    return plot_timeline(events, f"Speculation: task timeline ({row['delay_s']:g}s delayed worker)",
                         out_dir / "speculation_timeline.png")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results", type=Path, default=BENCH_DIR / "results")
    parser.add_argument("--out", type=Path, default=BENCH_DIR / "plots")
    args = parser.parse_args(argv)

    written = [
        plot_parallelism(read_rows(args.results / "parallelism.csv"), args.out),
        plot_on_off(read_rows(args.results / "speculation.csv"), args.out, "speculation",
                    ("speculation on", "speculation off"), "Speculation with one slow worker",
                    "speculation.png", by_delay=True),
        plot_speculation_timeline(read_rows(args.results / "speculation.csv"), args.results, args.out),
        plot_on_off(read_rows(args.results / "combiner.csv"), args.out, "combine",
                    ("with combiner", "without combiner"), "Combiner: job time", "combiner_time.png"),
        plot_combiner_bytes(read_rows(args.results / "combiner.csv"), args.out),
        *plot_worker_loss(read_rows(args.results / "worker-loss.csv"), args.results, args.out),
    ]
    written = [p for p in written if p]
    for path in written:
        print(f"wrote {path}")
    if not written:
        print(f"no results found in {args.results}; run benchmarks/run_benchmarks.py first")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
