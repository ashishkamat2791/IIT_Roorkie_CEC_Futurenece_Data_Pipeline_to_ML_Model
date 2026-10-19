"""
Run audit (pipeline_runs) and failure alerting, shared by both DAGs.

The audit task runs with trigger_rule="all_done", so it also runs after
failures. It records real task states, then re-raises if the pipeline failed,
so the DAG run is still marked failed in the UI instead of being hidden behind
a green audit task.
"""

from __future__ import annotations

import json
import logging
import urllib.request
from datetime import datetime

from .warehouse import connect, ensure_audit_tables

log = logging.getLogger(__name__)

FAILED_STATES = {"failed", "upstream_failed"}


def fetch_task_states(dag_id: str, run_id: str) -> dict[str, str]:
    """Task states for this run via the Airflow 3 Task SDK (workers have no metadata-DB access)."""
    try:
        from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
        states = RuntimeTaskInstance.get_task_states(dag_id=dag_id, run_ids=[run_id])
        return {k: (str(v) if v is not None else "none") for k, v in states.get(run_id, {}).items()}
    except Exception as exc:  # outside Airflow (tests) or API unavailable
        log.warning("could not read task states: %s", exc)
        return {}


def write_pipeline_run(warehouse, *, run_id: str, dag_id: str, start_time: str | None,
                       rows_extracted: int | None, rows_loaded: int | None, rows_quarantined: int | None,
                       task_states: dict[str, str], warnings: list[str], params: dict,
                       audit_task_id: str) -> dict:
    states = {k: v for k, v in task_states.items() if k != audit_task_id}
    failed = sorted(k for k, v in states.items() if v == "failed")             # root causes
    upstream = sorted(k for k, v in states.items() if v == "upstream_failed")  # collateral
    if failed or upstream:
        status = "FAILED"
    elif warnings:
        status = "SUCCESS_WITH_WARNINGS"
    else:
        status = "SUCCESS"
    end_time = datetime.now().isoformat(sep=" ", timespec="seconds")
    row = dict(run_id=run_id, dag_id=dag_id, start_time=start_time, end_time=end_time,
               rows_extracted=rows_extracted, rows_loaded=rows_loaded, rows_quarantined=rows_quarantined,
               status=status, failed_tasks=",".join(failed), warnings=" | ".join(warnings),
               task_states=json.dumps(states, sort_keys=True), params=json.dumps(params, default=str),
               upstream_failed_tasks=",".join(upstream))
    cols = ["run_id", "dag_id", "start_time", "end_time", "rows_extracted", "rows_loaded", "rows_quarantined",
            "status", "failed_tasks", "warnings", "task_states", "params", "upstream_failed_tasks"]
    with connect(warehouse) as con:
        ensure_audit_tables(con)
        con.execute(f"""
            INSERT INTO pipeline_runs ({", ".join(cols)})
            VALUES (?, ?, CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP), ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (dag_id, run_id) DO UPDATE SET
              start_time = excluded.start_time, end_time = excluded.end_time,
              rows_extracted = excluded.rows_extracted, rows_loaded = excluded.rows_loaded,
              rows_quarantined = excluded.rows_quarantined, status = excluded.status,
              failed_tasks = excluded.failed_tasks, warnings = excluded.warnings,
              task_states = excluded.task_states, params = excluded.params,
              upstream_failed_tasks = excluded.upstream_failed_tasks
        """, [row[k] for k in cols])
        con.execute("""
            INSERT INTO pipeline_run_history VALUES (?, ?, current_timestamp, CAST(? AS TIMESTAMP),
                   CAST(? AS TIMESTAMP), ?, ?, ?, ?, ?, ?, ?, ?)
        """, [row[k] for k in ["run_id", "dag_id", "start_time", "end_time", "rows_extracted", "rows_loaded",
                               "rows_quarantined", "status", "failed_tasks", "upstream_failed_tasks",
                               "warnings", "task_states"]])
    log.info("[audit] pipeline_runs %s", json.dumps({k: row[k] for k in row if k not in ("params", "task_states")}))
    return row


def notify_failure(context) -> None:
    """on_failure_callback: post to Slack only if the SLACK_WEBHOOK_URL Variable exists.

    The webhook is read at run time from an Airflow Variable (or a secrets
    backend), never from code or Git, and is never written to logs.
    """
    try:
        from airflow.sdk import Variable
        url = Variable.get("SLACK_WEBHOOK_URL", default=None)
    except Exception:
        url = None
    ti = context.get("task_instance")
    msg = (f":rotating_light: NammaMart {getattr(ti, 'dag_id', '?')} task {getattr(ti, 'task_id', '?')} "
           f"failed (run {context.get('run_id')}, try {getattr(ti, 'try_number', '?')}): "
           f"{str(context.get('exception'))[:300]}")
    log.error("[alert] %s", msg)
    if not url:
        log.info("[alert] SLACK_WEBHOOK_URL Variable not set; alert logged only")
        return
    try:
        req = urllib.request.Request(url, data=json.dumps({"text": msg}).encode(),
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
        log.info("[alert] Slack notification sent")
    except Exception as exc:  # alerting must never mask the original failure
        log.warning("[alert] Slack notification failed: %s", type(exc).__name__)


def record_dag_failure(context) -> None:
    """DAG-level on_failure_callback (found 19 Oct 2026: the laptop slept through the 09:00 run).

    When Airflow kills a run (dagrun_timeout), unfinished tasks - including the
    all_done run_audit task - are skipped, so nothing reached pipeline_runs.
    This callback records the run as FAILED unless run_audit already wrote a row.
    """
    from .config import RunConfig
    try:
        dr = context.get("dag_run")
        run_id, dag_id = dr.run_id, dr.dag_id
        cfg = RunConfig.from_params(run_id, dag_id, dict(context.get("params") or {}))
        reason = str(context.get("reason") or "dag run failed")
        with connect(cfg.warehouse) as con:
            ensure_audit_tables(con)
            exists = con.execute("SELECT count(*) FROM pipeline_runs WHERE dag_id = ? AND run_id = ?",
                                 [dag_id, run_id]).fetchone()[0]
        if exists:
            return  # run_audit ran and already recorded the outcome
        start = getattr(dr, "start_date", None)
        write_pipeline_run(cfg.warehouse, run_id=run_id, dag_id=dag_id,
                           start_time=start.strftime("%Y-%m-%d %H:%M:%S") if start else None,
                           rows_extracted=None, rows_loaded=None, rows_quarantined=None,
                           task_states={"dag_run": "failed"}, warnings=[f"run killed before audit: {reason}"],
                           params=cfg.to_dict(), audit_task_id="run_audit")
        log.error("[audit] %s %s recorded as FAILED by DAG callback: %s", dag_id, run_id, reason)
    except Exception as exc:  # a callback must never raise
        log.warning("[audit] DAG failure callback could not record the run: %s", exc)
