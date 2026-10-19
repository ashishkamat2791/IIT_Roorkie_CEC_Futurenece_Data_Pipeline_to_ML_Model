"""
scripts/dq_trend_chart.py  (bonus: data-quality trend chart from dq_results)
---------------------------------------------------------------------------
Reads dq_results from the production warehouse and every isolated experiment
warehouse (read-only), and writes docs/dq_trend.html: one small panel per
quality dimension, one bar per ETL run in time order, height = rows failed.
Spikes line up with the experiments (bad rows, volume drop, drift, freshness).
ELT counts are identical by construction (see scripts/compare_etl_elt.py).

  .venv/bin/python scripts/dq_trend_chart.py
"""

from __future__ import annotations

import html
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "dq_trend.html"
DIMS = ["Completeness", "Uniqueness", "Validity", "Accuracy", "Consistency", "Timeliness"]


def load() -> pd.DataFrame:
    whs = [ROOT / "include" / "warehouse_ashish_kamat.duckdb"]
    whs += sorted((ROOT / "include" / "experiments").glob("*/etl/warehouse.duckdb"))
    frames = []
    for wh in whs:
        con = duckdb.connect(str(wh), read_only=True)
        df = con.execute("""
            SELECT run_id, min(checked_at) AS checked_at, dimension, sum(rows_failed) AS rows_failed,
                   count(*) FILTER (WHERE status IN ('FAIL','WARN')) AS checks_not_passing
            FROM dq_results WHERE dag_id = 'nammamart_etl_ashish_kamat' GROUP BY run_id, dimension""").df()
        con.close()
        df["warehouse"] = "production" if wh.name == "warehouse_ashish_kamat.duckdb" else wh.parent.parent.name
        frames.append(df)
    df = pd.concat(frames)
    order = df.groupby("run_id")["checked_at"].min().sort_values()
    df["run_no"] = df["run_id"].map({r: i for i, r in enumerate(order.index)})
    return df.sort_values(["run_no", "dimension"])


def panel(df: pd.DataFrame, dim: str, runs: list[str]) -> str:
    w, h, pad_l, pad_b, pad_t = 470, 210, 44, 92, 26
    d = df[df["dimension"] == dim].set_index("run_id")["rows_failed"].reindex(runs).fillna(0)
    ymax = max(1, int(d.max()))
    bw = (w - pad_l - 34) / len(runs)
    bars, labels, max_done = [], [], False
    for i, (run, v) in enumerate(d.items()):
        x = pad_l + i * bw + 1
        bh = (h - pad_b - pad_t) * v / ymax
        y = h - pad_b - bh
        tip = html.escape(f"{run}\n{dim}: {int(v):,} rows failed")
        bars.append(f'<g><title>{tip}</title><rect class="hit" x="{x:.1f}" y="{pad_t}" width="{bw - 2:.1f}" '
                    f'height="{h - pad_b - pad_t}"/><path class="bar" d="M{x:.1f},{h - pad_b} v{-max(bh - 4, 0):.1f} '
                    f'q0,-4 4,-4 h{max(bw - 10, 0):.1f} q4,0 4,4 v{max(bh - 4, 0):.1f} z"/></g>' if bh >= 4 else
                    f'<g><title>{tip}</title><rect class="hit" x="{x:.1f}" y="{pad_t}" width="{bw - 2:.1f}" '
                    f'height="{h - pad_b - pad_t}"/><rect class="bar" x="{x:.1f}" y="{y:.1f}" width="{bw - 2:.1f}" height="{bh:.1f}"/></g>')
        short = (f"scheduled {run[11:21]}" if run.startswith("scheduled__") else run.replace("_etl", "").replace("drift_consistent", "drift_cons").replace("missing_file", "missing"))
        labels.append(f'<text class="tick" transform="translate({x + bw / 2:.1f},{h - pad_b + 8}) rotate(45)">{html.escape(short[:24])}</text>')
        if v == ymax and v > 0 and not max_done:
            max_done = True
            labels.append(f'<text class="val" x="{x + bw / 2:.1f}" y="{y - 4:.1f}" text-anchor="middle">{int(v):,}</text>')
    return (f'<figure><figcaption>{dim}</figcaption><svg viewBox="0 0 {w} {h}" role="img" '
            f'aria-label="{dim}: rows failed per run, max {ymax:,}">'
            f'<line class="axis" x1="{pad_l}" x2="{w - 8}" y1="{h - pad_b}" y2="{h - pad_b}"/>'
            f'<line class="grid" x1="{pad_l}" x2="{w - 8}" y1="{pad_t}" y2="{pad_t}"/>'
            f'<text class="tick" x="{pad_l - 6}" y="{pad_t + 4}" text-anchor="end">{ymax:,}</text>'
            f'<text class="tick" x="{pad_l - 6}" y="{h - pad_b + 4}" text-anchor="end">0</text>'
            + "".join(bars) + "".join(labels) + "</svg></figure>")


def main() -> None:
    df = load()
    runs = list(dict.fromkeys(df.sort_values("run_no")["run_id"]))
    table = df.pivot_table(index="run_id", columns="dimension", values="rows_failed", aggfunc="sum").reindex(runs)[DIMS]
    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>DQ trend</title>
<style>
.viz-root {{ color-scheme: light; --surface-1:#fcfcfb; --text-primary:#0b0b0b; --text-secondary:#52514e;
  --text-muted:#7a7974; --grid:#e6e5e1; --series-1:#2a78d6; }}
@media (prefers-color-scheme: dark) {{ :root:where(:not([data-theme="light"])) .viz-root {{ color-scheme: dark;
  --surface-1:#1a1a19; --text-primary:#f0efec; --text-secondary:#c3c2b7; --text-muted:#8f8e86; --grid:#2e2e2c; --series-1:#3987e5; }} }}
:root[data-theme="dark"] .viz-root {{ color-scheme: dark; --surface-1:#1a1a19; --text-primary:#f0efec;
  --text-secondary:#c3c2b7; --text-muted:#8f8e86; --grid:#2e2e2c; --series-1:#3987e5; }}
body {{ margin:0; background:var(--surface-1); }}
.viz-root {{ background:var(--surface-1); color:var(--text-primary); font:14px/1.4 system-ui, sans-serif; padding:16px; }}
h1 {{ font-size:18px; margin:0 0 4px; }} p {{ color:var(--text-secondary); margin:0 0 12px; max-width:900px; }}
@media (max-width: 900px) {{ .grid6 {{ grid-template-columns:1fr !important; }} }}
.grid6 {{ display:grid; grid-template-columns:repeat(3, minmax(0, 1fr)); gap:12px; }}
figure {{ margin:0; }} figcaption {{ font-weight:600; font-size:13px; }}
svg {{ width:100%; height:auto; }} .bar {{ fill:var(--series-1); }} .hit {{ fill:transparent; }}
g:hover .bar {{ opacity:.75; }} .axis {{ stroke:var(--text-muted); }} .grid {{ stroke:var(--grid); stroke-dasharray:2 3; }}
.tick {{ fill:var(--text-muted); font-size:10px; }} .val {{ fill:var(--text-secondary); font-size:11px; font-weight:600; }}
details {{ margin-top:16px; color:var(--text-secondary); }} table {{ border-collapse:collapse; font-size:12px; }}
td,th {{ border:1px solid var(--grid); padding:2px 6px; text-align:right; }} th:first-child, td:first-child {{ text-align:left; }}
</style></head><body><div class="viz-root">
<h1>Data-quality trend: rows failing checks, per dimension, per ETL run</h1>
<p>Source: <code>dq_results</code> (dag <code>nammamart_etl_ashish_kamat</code>) across the production warehouse and the isolated
experiment warehouses, in run order. Each panel has its own scale (max labelled). Hover a bar for the exact count.
Spikes: exp05 bad rows, exp07 volume drop (payments orphaned), exp08 price drift. ELT counts are identical.</p>
<div class="grid6">{"".join(panel(df, d, runs) for d in DIMS)}</div>
<details><summary>Table view</summary>{table.fillna(0).astype(int).to_html()}</details>
</div></body></html>"""
    OUT.write_text(page)
    print(f"wrote {OUT} ({len(runs)} runs)")


if __name__ == "__main__":
    main()
