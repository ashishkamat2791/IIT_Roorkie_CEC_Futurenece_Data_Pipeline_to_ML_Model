**To:** Sameer (CTO), NammaMart · **From:** Ashish Kamat (FDE) · **Re:** ETL or ELT for the 9 AM report

**Recommendation: adopt ELT (raw → staging → marts in the warehouse) with an ETL-grade quality gate in front of the finance marts.** The gate means marts read only rows that passed every critical check. This is a deliberate hybrid. ELT gives analytics and compliance the history they asked for. The gate gives finance the guarantee it asked for, and I showed it produces exactly the same numbers as the ETL pipeline.

**Observed results** (both pipelines ran on the same data, in Airflow, on 18 Oct 2026):

| Criterion | ETL (pandas → star schema) | ELT (DuckDB SQL layers) | Evidence |
|---|---|---|---|
| Correctness | Sept revenue ₹1,134,962.80; 2,308 valid orders | **Identical**, row for row, for all six questions and 95 quality-check counts | `scripts/compare_etl_elt.py` |
| Data-quality guarantee | Bad rows never reach the fact; 167 rows quarantined with reasons | Same 167 rows marked FAILED in staging; marts exclude them | experiment 5: all 5 injected bad rows caught by both |
| Auditability | Fact keeps file + row number, but the original record lives only in the CSV and the quarantine copy | Every row of every load kept byte-for-byte; one SQL query goes from a mart number to the exact raw rows | lineage and audit in SUBMISSION E3/E8 |
| Late corrections | 10 DELIVERED→RETURNED applied (150 inserted, 10 updated). Only the restated view survives, plus `previous_status` | Same result via SQL MERGE; **both** views available: as-reported ₹1,134,962.80 and restated ₹1,130,769.10 | experiment 3, E9 |
| Ease of changing a rule | Change Python and re-run; past days need the old files | Change SQL and replay from raw; history recomputes itself | design |
| Performance (measured ×100 = 241k orders) | 142.5 s, 1.8 GB peak | **16.6 s**, 1.7 GB peak | `docs/evidence/benchmark_scale.txt` |
| Privacy | Raw PII never enters the warehouse | Raw PII is stored and needs locked-down access | data dictionary |
| Operations | Fewer objects, easier to debug | 20 SQL files. A drift test exposed a decimal-overflow bug, which failed loudly and has been fixed | experiment 8 |
| Recovery | Clear re-ran only the failed task and its downstream | Same; raw loads that succeeded were kept | experiment 10 |

**At 1,000× volume** (≈2.4 M orders per file; this is a projection, not a measurement): ETL time grew linearly from ×10 to ×100 (14 s → 142 s), so ×1000 would take about 24 minutes and need about 18 GB of RAM in one task. That would break the 09:00–09:30 window on a laptop-class worker. DuckDB's vectorised SQL scaled better than linearly so far (3.4 s → 16.6 s) and can spill to disk, so ELT should stay within minutes. Beyond single-node scale, the same SQL moves to an open-source columnar warehouse.

**Cost:** both run on free tools. ELT's real cost is storage: raw grows by the full export on every run (5,212 rows per day today, about 5 M per day at 1,000×), so it needs a retention policy from day one.

**Why not pure ETL?** It is simpler and keeps PII out of the warehouse. But it cannot answer finance's own question in E9 ("what did we report for September?") once a correction arrives, and it does not scale.

**Why not ungated ELT?** Finance must never see a bad row. The gate, plus a publish check that blocks the marts on a critical dataset anomaly, keeps that promise.

**Top 3 risks of the recommended design, and mitigations**

1. **Raw layer holds unmasked customer PII (DPDP Act, 2023).** Put raw in a separate restricted schema with access logging. Tokenise email and phone at landing, keeping the mapping in a vault. Purge raw after 13 months. Only the DPO role gets break-glass access.
2. **SQL rule logic can drift or break.** Experiment 8 found an overflow, and experiment 8b showed a ×100 price with a consistent amount passed every rule until I added the gross-outlier rule. Mitigations:
   - Keep the ETL implementation as the reference test.
   - Run the parity and edge-case suite in CI on every rule change.
   - Use wide `DECIMAL` types by default.
   - Require the metric owner (CFO or COO) to approve any rule change.
3. **Availability: a laptop or single-node DuckDB cannot guarantee 9 AM.** Run Airflow on an always-on host with a Postgres metadata DB. Add a 09:15 "not yet successful" deadline alert (Slack Variable is already wired), raw backups, and `max_active_runs=1` (already set) for the single writer.
