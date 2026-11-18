"""Draw figures from measured replay and serving artifacts."""
import argparse
import json
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

TEAL = "#007e78"
BLUE = "#4069ba"
INK = "#172b3b"
plt.rcParams.update({"font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.axisbelow": True, "grid.color": "#d5e1e7", "text.color": INK})


def save(fig, out, name, title, note):
    fig.suptitle(title, x=.055, y=.96, ha="left", fontsize=20, fontweight="bold", color=INK)
    fig.text(.055, .035, note, color="#526575", fontsize=9)
    fig.tight_layout(rect=(.025, .086, .995, .865), pad=1.5)
    fig.savefig(out / (name + ".png"), dpi=180, facecolor="white")
    plt.close(fig)


def render(run, out):
    replay = json.loads((run / "verification.json").read_text(encoding="utf-8"))
    serving = json.loads((run / "serving-verification.json").read_text(encoding="utf-8"))
    rows = [json.loads(line) for line in (run / "hourly.jsonl").read_text(encoding="utf-8").splitlines()]
    times = [datetime.fromisoformat(row["hour_utc"]) for row in rows]
    fig, ax = plt.subplots(figsize=(9.5, 4.5))
    ax.fill_between(times, [row.get("min_flow", float("nan")) for row in rows],
                    [row.get("max_flow", float("nan")) for row in rows], color="#a1d6d2",
                    label="Within-hour minimum to maximum")
    ax.plot(times, [row.get("mean_flow", float("nan")) for row in rows], color=TEAL, label="Hourly mean")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax.set(ylabel="Discharge (ft³/s)", xlabel="UTC")
    ax.grid(axis="y")
    ax.legend(loc="upper left", frameon=False)
    save(fig, out, "potomac-hourly-flow", "Hourly discharge during the Potomac replay",
         f"USGS-01646500 · {len(rows)} hourly groups · observations are assigned to UTC hours by Spark")

    fig, ax = plt.subplots(figsize=(9.5, 4.5))
    phases = [replay["first"], replay["second"]]
    names = ["First publish", "Resume and republish"]
    unique = [r["latest_observations"] for r in phases]
    repeats = [r["replayed_duplicates"] for r in phases]
    ax.bar(names, unique, color=TEAL, label="Distinct observations", width=.56)
    bars = ax.bar(names, repeats, bottom=unique, color=BLUE, label="Repeated deliveries", width=.56)
    ax.bar_label(bars, labels=[f"{r['bronze_rows']:,} raw deliveries" for r in phases], padding=5)
    ax.set(ylabel="Cumulative Parquet rows", ylim=(0, max(r["bronze_rows"] for r in phases)*1.22))
    ax.legend(loc="upper left", frameon=False)
    ax.grid(axis="y")
    save(fig, out, "checkpoint-replay", "A second publish increases the raw history",
         f"The ordered Spark result has an identical SHA-256 across both passes · {phases[-1]['hourly_groups']} hourly groups")

    fig, ax = plt.subplots(figsize=(9.5, 4.5))
    tests = [serving["cold_read"]["wall_ms"], serving["warm_read"]["wall_ms"]]
    bars = ax.bar(["Cold read and populate cache", "Warm Redis cache read"], tests, color=[BLUE, TEAL], width=.56)
    ax.bar_label(bars, fmt="%.0f ms", padding=5)
    ax.set(ylabel="Local elapsed wall time (ms)", ylim=(0, max(tests) * 1.23))
    ax.grid(axis="y")
    save(fig, out, "serving-read", "Cassandra and Redis return the same hourly rows",
         f"One local test, {serving['cache_partition_rows']} rows · includes Docker CLI startup and cache population")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, default=Path("runs/potomac-validation-03"))
    parser.add_argument("--out", type=Path, default=Path("figures/results"))
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    render(args.run, args.out)
