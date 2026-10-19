"""
scripts/build_submission_pdf.py
-------------------------------
Builds the printable HTML behind docs/submission/NammaMart_ETL_ELT_Submission_Ashish_Kamat.pdf:
a cover page of evaluator notes (GitHub link, commit), the full SUBMISSION.md, and appendices
with every screenshot, the business-question results and the data dictionary.

  .venv/bin/python scripts/build_submission_pdf.py "$PWD" /tmp/submission_print.html
  PDF_OUT=docs/submission/NammaMart_ETL_ELT_Submission_Ashish_Kamat.pdf \
    node <browser-automation>/browser.mjs file:///tmp/submission_print.html --script scripts/screenshots/to_pdf.mjs
"""
import re
import subprocess
import sys
from pathlib import Path

import markdown

REPO = Path(sys.argv[1])
OUT = Path(sys.argv[2])
BRANCH = "nammamart-assignment-ashish-kamat"
URL = f"https://github.com/ashishkamat2791/IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model/tree/{BRANCH}"
COMMIT = subprocess.run(["git", "-C", str(REPO), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()

LIST = re.compile(r"^(\s*)([-*]|\d+\.)\s")


def normalise(s: str) -> str:
    """GitHub-style lists -> Python-Markdown: blank line before a list, 4-space nesting."""
    out, prev, in_code = [], "", False
    for line in s.splitlines():
        if line.lstrip().startswith("```"):
            in_code = not in_code
        m = LIST.match(line) if not in_code else None
        if m:
            ind = len(m.group(1))
            if ind:  # 2-3 spaces -> 4, 5-6 -> 8 (nested under an ordered item)
                line = " " * (4 * ((ind + 2) // 4 or 1)) + line.lstrip()
            if prev.strip() and not LIST.match(prev) and not prev.startswith("|") and not prev.startswith(" "):
                out.append("")
        elif not in_code and line.startswith("   ") and not line.startswith("    ") and out and line.strip():
            line = " " + line   # continuation paragraphs inside list items
        out.append(line)
        prev = line
    return "\n".join(out)


md = lambda s: markdown.markdown(normalise(s), extensions=["tables", "fenced_code", "sane_lists"])

cover = f"""
<section class="cover">
<h1>NammaMart Quick Commerce: ETL &amp; ELT Pipelines</h1>
<p class="sub">FDAIE Module 2 · Data Engineering Foundations · Assignment submission</p>
<p class="sub"><b>Learner:</b> Ashish Kamat · <b>Submitted:</b> 19 October 2026</p>
<h2>Notes for the evaluator</h2>
<table>
<tr><th>Source code (GitHub)</th><td><a href="{URL}">{URL}</a></td></tr>
<tr><th>Branch / commit</th><td><code>{BRANCH}</code> @ <code>{COMMIT}</code></td></tr>
<tr><th>Start here</th><td><code>SUBMISSION.md</code> (this PDF reproduces it, plus an appendix of every screenshot and the business-question results)</td></tr>
<tr><th>DAGs</th><td><code>dags/nammamart_etl_ashish_kamat.py</code>, <code>dags/nammamart_elt_ashish_kamat.py</code></td></tr>
<tr><th>Helpers / SQL</th><td><code>include/nammamart_ashish_kamat/</code>, <code>include/sql/</code> (common, etl, elt raw/staging/dq/mart)</td></tr>
<tr><th>Evidence</th><td><code>docs/</code>: profiling, business questions, data dictionary, runbook, Airflow run histories, experiment outputs, screenshots</td></tr>
<tr><th>How to run</th><td><code>astro dev start</code> → unpause both DAGs (09:00 IST Mon–Sat) or trigger them; increment: <code>--conf '{{"apply_increment": true}}'</code>. Details in <code>docs/RUNBOOK.md</code></td></tr>
<tr><th>Tests</th><td><code>.venv/bin/python -m pytest tests/nammamart</code> (32 passed) · <code>astro dev pytest tests/dags</code> (19 passed)</td></tr>
<tr><th>Not in the zip, on purpose</th><td>The DuckDB warehouse (it holds unmasked customer PII in <code>raw_customers</code>), virtualenvs, scratch experiment data and all secrets. The Slack webhook lives only in an Airflow Variable.</td></tr>
</table>
<h2>Headline results</h2>
<ul>
<li>ETL and ELT give <b>identical</b> answers to all six business questions. September revenue is ₹1,134,962.80 before the increment and ₹1,130,769.10 after it (10 DELIVERED→RETURNED corrections).</li>
<li>260 data issues were logged across all files; 167 rows were quarantined with reasons; customer PII is masked in every reporting table.</li>
<li>All 10 observability experiments ran on both DAGs. Four defects were found and fixed, and an unplanned real failure (laptop asleep at 09:00) was recorded and recovered.</li>
<li>All four bonus items are done: a real Slack alert via an Airflow Variable, an SCD-2 customer dimension, the store-capacity check, and the DQ trend chart.</li>
</ul>
</section>
"""

sub = (REPO / "SUBMISSION.md").read_text()
body = md(sub)

shots = [
    ("etl_graph_success.png", "ETL DAG Graph view: successful run (Airflow 3 UI)"),
    ("elt_graph_success.png", "ELT DAG Graph view: successful run"),
    ("dags_schedule_next_run.png", "Both DAGs: schedule 0 9 * * 1-6 and next run (UI shown in +05:30)"),
    ("quarantine_file.png", "Quarantine file with failure_reason (orders, scheduled run)"),
    ("dq_results.png", "dq_results: checks that did not pass (increment runs)"),
    ("pipeline_runs.png", "pipeline_runs: run audit for every production run"),
    ("payment_reconciliation.png", "Payment reconciliation: mismatches by reason, plus sample rows"),
    ("dq_trend.png", "Bonus: data-quality trend chart built from dq_results"),
    ("slack_alert.png", "Bonus: real Slack failure alerts (ETL and ELT) via the SLACK_WEBHOOK_URL Variable"),
]
appendix = ['<section class="appendix"><h1>Appendix A: Screenshots</h1>']
for f, cap in shots:
    appendix.append(f'<figure><img src="docs/screenshots/{f}"><figcaption>{cap} · <code>docs/screenshots/{f}</code></figcaption></figure>')
appendix.append("</section>")
bq = (REPO / "docs" / "business_questions.md").read_text()
bq = re.sub(r"^# .*\n", "", bq, count=1)
appendix.append('<section class="appendix"><h1>Appendix B: Business questions, SQL and results (both pipelines)</h1>' + md(bq) + "</section>")
appendix.append('<section class="appendix"><h1>Appendix C: Data dictionary</h1>' +
                md(re.sub(r"^# .*\n", "", (REPO / "docs" / "data_dictionary.md").read_text(), count=1)) + "</section>")

css = """
@page { size: A4 landscape; margin: 12mm 12mm 14mm 12mm; }
body { font: 10.5px/1.45 -apple-system, 'Segoe UI', Roboto, sans-serif; color: #111; }
h1 { font-size: 20px; margin: 18px 0 8px; border-bottom: 2px solid #2a78d6; padding-bottom: 4px; }
h2 { font-size: 15px; margin: 16px 0 6px; } h3 { font-size: 12.5px; margin: 12px 0 4px; }
table { border-collapse: collapse; width: 100%; margin: 6px 0 10px; page-break-inside: auto; }
th, td { border: 1px solid #c9c8c3; padding: 3px 5px; vertical-align: top; text-align: left; word-break: break-word; }
th { background: #eef3fb; } tr { page-break-inside: avoid; }
code { font: 9.5px Menlo, monospace; background: #f3f3f1; padding: 0 2px; word-break: break-all; }
pre { background: #f6f6f4; padding: 6px; white-space: pre-wrap; font-size: 9.5px; page-break-inside: avoid; }
pre code { background: none; }
img { max-width: 100%; max-height: 165mm; display: block; margin: 0 auto; border: 1px solid #ddd; }
figure { margin: 0 0 10px; page-break-inside: avoid; page-break-after: always; }
figcaption { text-align: center; color: #444; margin-top: 4px; }
a { color: #1d5fb0; }
.cover { page-break-after: always; } .cover h1 { font-size: 26px; } .sub { font-size: 13px; color: #333; margin: 2px 0; }
.cover th { width: 18%; } .appendix { page-break-before: always; }
"""
html = f"""<!doctype html><html><head><meta charset="utf-8"><base href="file://{REPO}/">
<title>NammaMart ETL & ELT Submission — Ashish Kamat</title><style>{css}</style></head>
<body>{cover}{body}{''.join(appendix)}</body></html>"""
OUT.write_text(html)
print("html written", OUT, len(html))
