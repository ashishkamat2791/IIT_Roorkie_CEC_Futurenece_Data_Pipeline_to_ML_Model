#!/usr/bin/env bash
# Capture the real task-instance history of one DAG run from the local Airflow
# metadata DB (states, try numbers, start/end, durations), including earlier
# tries from task_instance_history. Read-only queries.
#
#   scripts/airflow_run_evidence.sh <dag_id> <run_id> [output_file]
#
# PG_CONTAINER defaults to the Astro Postgres container of this project.
set -euo pipefail
DAG_ID="$1"; RUN_ID="$2"; OUT="${3:-/dev/stdout}"
PG_CONTAINER="${PG_CONTAINER:-$(docker ps --format '{{.Names}}' | grep -E 'etl-elt-dag_c085e4-postgres' | head -1)}"

SQL=$(cat <<EOF
\\echo '== DAG run'
SELECT run_id, state, run_type, start_date, end_date, round(extract(epoch FROM end_date - start_date)::numeric, 1) AS seconds,
       conf::text AS conf
FROM dag_run WHERE dag_id = '$DAG_ID' AND run_id = '$RUN_ID';
\\echo '== Task instances (current try)'
SELECT task_id, state, try_number, to_char(start_date, 'HH24:MI:SS.MS') AS started, to_char(end_date, 'HH24:MI:SS.MS') AS ended,
       round(duration::numeric, 2) AS seconds
FROM task_instance WHERE dag_id = '$DAG_ID' AND run_id = '$RUN_ID' ORDER BY start_date NULLS LAST, task_id;
\\echo '== Earlier tries (task_instance_history)'
SELECT task_id, try_number, state, to_char(start_date, 'HH24:MI:SS.MS') AS started, to_char(end_date, 'HH24:MI:SS.MS') AS ended
FROM task_instance_history WHERE dag_id = '$DAG_ID' AND run_id = '$RUN_ID' ORDER BY task_id, try_number;
EOF
)
docker exec -i "$PG_CONTAINER" psql -U postgres -d postgres -P pager=off -c "SET search_path TO public" -f - <<<"$SQL" > "$OUT"
