"""
Script: stage3_event_driven_processing (Lambda handler)

Purpose
- When new bronze files arrive (or when invoked manually), run two Athena INSERTs:
  1) Insert processed counts into stage3_accredited_facilities_per_state
  2) Insert DQ rejects into stage3_rejects_per_state
- Then UNLOAD both result sets to CSV in S3 for downstream use.
- Keep everything idempotent per run using a unique run_ts.

What this function does (high level flow)
1) Optional S3 event guard: ignore keys that aren’t under bronze batch *.jsonl
2) Generate a unique run_ts (timestamp + short uuid)
3) Execute two INSERT statements (processed + rejects)
4) Count inserted rows (fast scalar queries)
5) UNLOAD the inserted rows for this run_ts to CSV target prefixes
6) Skip UNLOAD if the target prefix already exists (idempotent)
7) Return counts and S3 locations

Inputs / Configuration (Environment variables)
- ATHENA_DB               : Athena database name (default: "medlaunch_db")
- ATHENA_WORKGROUP        : Athena workgroup (default: "primary")
- DATA_BUCKET             : Data lake bucket (required)
- OUTPUT_SCRATCH          : S3 URI for Athena scratch results; we write a probe file
- KMS_KEY_ARN             : Optional KMS key ARN for Athena outputs (SSE-KMS)
- BRONZE_TABLE            : Source table for Stage 3 (default: "bronze_facilities_json_np")
- CSV_PROCESSED_PREFIX    : S3 prefix for processed CSV UNLOADs (default provided)
- CSV_REJECTED_PREFIX     : S3 prefix for rejects CSV UNLOADs (default provided)
- ATHENA_TIMEOUT_SECONDS  : Per-query timeout (default: 540 seconds)
- POLL_INTERVAL_SECONDS   : Poll frequency while waiting on Athena (default: 2.5s)

Outputs
- Athena rows inserted into:
  • medlaunch_db.stage3_accredited_facilities_per_state
  • medlaunch_db.stage3_rejects_per_state
- CSV files UNLOADed to the configured prefixes under run_ts=<ts>/

Security / Permissions (Lambda execution role needs)
- Athena: StartQueryExecution, StopQueryExecution, GetQueryExecution, GetQueryResults
- S3: PutObject/DeleteObject on OUTPUT_SCRATCH and CSV prefixes; ListBucket for existence checks
- KMS: Encrypt/Decrypt if KMS_KEY_ARN is set

Operational notes
- Idempotency: UNLOAD step skips if the target run_ts prefix already exists
- Resiliency: query loop cancels long-running Athena queries and raises TimeoutError
- Observability: JSON-structured logs via log(**kw)
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, Any, Tuple
import uuid

import boto3

# Root logger configured to INFO; keep logs structured via log(**kw)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def log(**kw):
    """Log a JSON line with arbitrary fields; default=str handles timestamps etc."""
    logger.info(json.dumps(kw, default=str))


# Reuse clients at module scope for connection pooling
ATHENA = boto3.client("athena")
S3 = boto3.client("s3")

# --------- configuration (env) ----------
DB = os.environ.get("ATHENA_DB", "medlaunch_db")
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
DATA_BUCKET = os.environ["DATA_BUCKET"]  # required
OUTPUT_SCRATCH = os.environ["OUTPUT_SCRATCH"]  # s3://.../_scratch/
KMS_KEY_ARN = os.environ.get("KMS_KEY_ARN")
BRONZE_TABLE = os.environ.get("BRONZE_TABLE", "bronze_facilities_json_np")

# Where CSV exports land (processed and rejects)
CSV_PROCESSED_PREFIX = os.environ.get(
    "CSV_PROCESSED_PREFIX",
    f"s3://{DATA_BUCKET}/stage3-athena-query-results/Processed Results/accredited_facilities_per_state/",
)
CSV_REJECTED_PREFIX = os.environ.get(
    "CSV_REJECTED_PREFIX",
    f"s3://{DATA_BUCKET}/stage3-athena-query-results/Rejected Results/accredited_facilities_per_state/",
)

# Tuning
ATHENA_TIMEOUT_SECONDS = int(os.environ.get("ATHENA_TIMEOUT_SECONDS", "540"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL_SECONDS", "2.5"))


# ---------- tiny utils ----------
def _ensure_slash(u: str) -> str:
    """Ensure trailing slash on an S3 URL prefix."""
    return u if u.endswith("/") else u + "/"


def _parse_s3(u: str) -> Tuple[str, str]:
    """Split s3://bucket/prefix → (bucket, prefix)."""
    assert u.startswith("s3://"), f"not s3 url: {u}"
    path = u[5:]
    b, _, k = path.partition("/")
    return b, k


def _normalize_scratch(u: str) -> str:
    """Normalize scratch prefix so it includes '_scratch/' and ends with '/'."""
    u = _ensure_slash(u)
    if "/_scratch/" not in u:
        u = u + "_scratch/"
    return _ensure_slash(u)


def _probe_scratch(u: str) -> str:
    """
    Write/delete a tiny object to verify the scratch prefix is writable.
    Honors KMS if provided. Raises on failure.
    """
    u = _ensure_slash(_normalize_scratch(u))
    bucket, prefix = _parse_s3(u)
    key = f"{prefix}__probe_{int(time.time())}.tmp"
    put = dict(Bucket=bucket, Key=key, Body=b"")
    if KMS_KEY_ARN:
        put["ServerSideEncryption"] = "aws:kms"
        put["SSEKMSKeyId"] = KMS_KEY_ARN
    try:
        S3.put_object(**put)
    except Exception as e:
        raise RuntimeError(f"scratch not writable {u}: {e}")
    finally:
        # Best-effort cleanup; ignore delete errors
        try:
            S3.delete_object(Bucket=bucket, Key=key)
        except Exception:
            pass
    return u


def _athena_result_cfg(u: str) -> dict:
    """Build Athena ResultConfiguration with optional SSE-KMS."""
    cfg = {"OutputLocation": _ensure_slash(u)}
    if KMS_KEY_ARN:
        cfg["EncryptionConfiguration"] = {
            "EncryptionOption": "SSE_KMS",
            "KmsKey": KMS_KEY_ARN,
        }
    return cfg


def run_athena(query: str, ctx_remaining_ms: int) -> str:
    """
    Run an Athena query and block until completion or timeout.
    Returns the QueryExecutionId on success, raises on failure/timeout.
    """
    scratch = _probe_scratch(OUTPUT_SCRATCH)
    start = time.time()

    q = ATHENA.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DB},
        WorkGroup=WORKGROUP,
        ResultConfiguration=_athena_result_cfg(scratch),
    )
    qid = q["QueryExecutionId"]
    log(event="athena_start", qid=qid)

    def cancel():
        """Attempt to cancel the running query if we time out."""
        try:
            ATHENA.stop_query_execution(QueryExecutionId=qid)
        except Exception:
            pass

    # Poll until SUCCEEDED/FAILED/CANCELLED or we hit our timeout/remaining-time guard
    while True:
        time.sleep(POLL_INTERVAL)
        st = ATHENA.get_query_execution(QueryExecutionId=qid)["QueryExecution"][
            "Status"
        ]["State"]
        if time.time() - start > ATHENA_TIMEOUT_SECONDS or ctx_remaining_ms < 10_000:
            cancel()
            raise TimeoutError(f"Athena timed out (qid={qid})")
        if st == "SUCCEEDED":
            log(event="athena_done", qid=qid, seconds=round(time.time() - start, 2))
            return qid
        if st in ("FAILED", "CANCELLED"):
            reason = ATHENA.get_query_execution(QueryExecutionId=qid)["QueryExecution"][
                "Status"
            ].get("StateChangeReason", "unknown")
            raise RuntimeError(f"Athena {st}: {reason} (qid={qid})")


def run_athena_scalar(query: str, ctx_remaining_ms: int) -> str:
    """
    Convenience for small SELECTs: returns the first cell (row 1, col 1) as a string.
    If no data, returns '0'.
    """
    qid = run_athena(query, ctx_remaining_ms)
    res = ATHENA.get_query_results(QueryExecutionId=qid, MaxResults=2)
    rows = res.get("ResultSet", {}).get("Rows", [])
    if len(rows) < 2 or not rows[1]["Data"]:
        return "0"
    return rows[1]["Data"][0].get("VarCharValue", "0")


def _s3_prefix_exists(url: str) -> bool:
    """Return True if at least one object exists under the given S3 URL prefix."""
    bucket, prefix = _parse_s3(_ensure_slash(url))
    resp = S3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return resp.get("KeyCount", 0) > 0


def _unload_target(base_prefix: str, run_ts: str) -> str:
    """Build the run_ts-aware destination prefix: <base>/run_ts=<run_ts>/"""
    return f"{_ensure_slash(base_prefix)}run_ts={run_ts}/"


def _build_unload(sql: str, dest_prefix: str) -> str:
    """
    Wrap a SELECT in Athena UNLOAD to write CSV (TEXTFILE) to the destination prefix.
    Note: field delimiter is ','; adjust here if you need quoted CSV later.
    """
    return f"""
UNLOAD (
  {sql}
)
TO '{_ensure_slash(dest_prefix)}'
WITH (format='TEXTFILE', field_delimiter=',')
"""


# ---------- SQL builders (hard-dedupe + run_ts injected) ----------
def build_insert_processed(run_ts: str) -> str:
    """
    Insert state-level accredited facility counts for each snapshot_date.
    - DQ: facility_id starts with FAC; name present; employee_count >= 0; state valid.
    - "Currently accredited" if any accreditation valid_until >= current_date.
    - Dedup by (state_code, snapshot_date); only insert if not already present.
    """
    return f"""
INSERT INTO medlaunch_db.stage3_accredited_facilities_per_state
WITH base AS (
  SELECT DISTINCT
    UPPER(TRIM(f.facility_id))        AS facility_id_t,
    NULLIF(TRIM(f.facility_name), '') AS facility_name_t,
    CAST(f.employee_count AS INTEGER) AS employee_count_t,
    UPPER(TRIM(f.location.state))     AS state_t,
    f.accreditations                  AS accs,
    TRIM(CAST(f.snapshot_date AS VARCHAR)) AS snapshot_date
  FROM {DB}.{BRONZE_TABLE} f
),
valid AS (
  SELECT *
  FROM base
  WHERE facility_id_t IS NOT NULL AND REGEXP_LIKE(facility_id_t, '^FAC')
    AND facility_name_t IS NOT NULL
    AND employee_count_t IS NOT NULL AND employee_count_t >= 0
    AND state_t IN (
      'AL','AK','AZ','AR','CA','CO','CT','DC','DE','FL','GA','HI','IA','ID','IL','IN','KS','KY',
      'LA','MA','MD','ME','MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ','NM','NV','NY','OH',
      'OK','OR','PA','RI','SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY'
    )
),
flags AS (
  SELECT
    v.facility_id_t,
    v.state_t,
    v.snapshot_date,
    MAX(CASE WHEN TRY(CAST(a.valid_until AS DATE)) >= current_date THEN 1 ELSE 0 END) AS has_current_valid
  FROM valid v
  LEFT JOIN UNNEST(v.accs) AS t(a) ON TRUE
  GROUP BY v.facility_id_t, v.state_t, v.snapshot_date
),
counts AS (
  SELECT
    v.state_t AS state_code,
    v.snapshot_date,
    COUNT(DISTINCT CASE WHEN has_current_valid=1 THEN v.facility_id_t END) AS facilities_currently_accredited
  FROM flags v
  GROUP BY v.state_t, v.snapshot_date
),
dedup AS (
  SELECT
    state_code,
    snapshot_date,
    MAX(facilities_currently_accredited) AS facilities_currently_accredited
  FROM counts
  GROUP BY state_code, snapshot_date
)
SELECT
  element_at(
    MAP(
      ARRAY['AL','AK','AZ','AR','CA','CO','CT','DC','DE','FL','GA','HI','IA','ID','IL','IN','KS','KY',
            'LA','MA','MD','ME','MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ','NM','NV','NY','OH',
            'OK','OR','PA','RI','SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY'],
      ARRAY['Alabama','Alaska','Arizona','Arkansas','California','Colorado','Connecticut','District of Columbia','Delaware','Florida','Georgia','Hawaii','Iowa','Idaho','Illinois','Indiana','Kansas','Kentucky',
            'Louisiana','Massachusetts','Maryland','Maine','Michigan','Minnesota','Missouri','Mississippi','Montana','North Carolina','North Dakota','Nebraska','New Hampshire','New Jersey','New Mexico','Nevada','New York','Ohio',
            'Oklahoma','Oregon','Pennsylvania','Rhode Island','South Carolina','South Dakota','Tennessee','Texas','Utah','Virginia','Vermont','Washington','Wisconsin','West Virginia','Wyoming']
    ),
    state_code
  ) AS state_name,
  state_code,
  facilities_currently_accredited,
  snapshot_date,
  '{run_ts}' AS run_ts
FROM dedup d
WHERE NOT EXISTS (
  SELECT 1
  FROM medlaunch_db.stage3_accredited_facilities_per_state t
  WHERE t.snapshot_date = d.snapshot_date
    AND t.state_code   = d.state_code
);
"""


def build_insert_rejects(run_ts: str) -> str:
    """
    Insert data-quality rejects per (facility_id, state_code, snapshot_date).
    Reasons include: bad FAC prefix, blank name, negative/NULL employee_count, invalid state.
    Dedup so we don’t insert duplicate rejects for the same triplet.
    """
    return f"""
INSERT INTO medlaunch_db.stage3_rejects_per_state
WITH b AS (
  SELECT DISTINCT
    TRIM(f.facility_id)                AS facility_id_raw,
    TRIM(f.facility_name)              AS facility_name_raw,
    CAST(f.employee_count AS INTEGER)  AS employee_count_t,
    UPPER(TRIM(f.location.state))      AS state_t,
    TRIM(CAST(f.snapshot_date AS VARCHAR)) AS snapshot_date
  FROM {DB}.{BRONZE_TABLE} f
),
flags AS (
  SELECT
    facility_id_raw,
    facility_name_raw,
    employee_count_t,
    state_t,
    snapshot_date,
    (facility_id_raw IS NULL OR NOT REGEXP_LIKE(UPPER(facility_id_raw), '^FAC')) AS bad_facility_id_prefix,
    (NULLIF(facility_name_raw, '') IS NULL)                                      AS bad_facility_name_blank,
    (employee_count_t IS NULL OR employee_count_t < 0)                           AS bad_employee_count,
    (state_t NOT IN (
      'AL','AK','AZ','AR','CA','CO','CT','DC','DE','FL','GA','HI','IA','ID','IL','IN','KS','KY',
      'LA','MA','MD','ME','MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ','NM','NV','NY','OH',
      'OK','OR','PA','RI','SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY'
    )) AS bad_state
  FROM b
),
rej AS (
  SELECT
    COALESCE(
      CASE
        WHEN bad_facility_id_prefix  THEN 'facility_id_does_not_start_with_FAC'
        WHEN bad_facility_name_blank THEN 'facility_name_blank_after_trim'
        WHEN bad_employee_count      THEN 'employee_count_negative_or_null'
        WHEN bad_state               THEN 'state_invalid'
        ELSE 'unknown'
      END,
      'unknown'
    ) AS reject_reason,
    facility_id_raw AS facility_id,
    state_t         AS state_code,
    snapshot_date
  FROM flags
  WHERE bad_facility_id_prefix OR bad_facility_name_blank OR bad_employee_count OR bad_state
),
dedup AS (
  SELECT
    facility_id, state_code, snapshot_date, reject_reason
  FROM rej
  GROUP BY facility_id, state_code, snapshot_date, reject_reason
)
SELECT
  element_at(
    MAP(
      ARRAY['AL','AK','AZ','AR','CA','CO','CT','DC','DE','FL','GA','HI','IA','ID','IL','IN','KS','KY',
            'LA','MA','MD','ME','MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ','NM','NV','NY','OH',
            'OK','OR','PA','RI','SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY'],
      ARRAY['Alabama','Alaska','Arizona','Arkansas','California','Colorado','Connecticut','District of Columbia','Delaware','Florida','Georgia','Hawaii','Iowa','Idaho','Illinois','Indiana','Kansas','Kentucky',
            'Louisiana','Massachusetts','Maryland','Maine','Michigan','Minnesota','Missouri','Mississippi','Montana','North Carolina','North Dakota','Nebraska','New Hampshire','New Jersey','New Mexico','Nevada','New York','Ohio',
            'Oklahoma','Oregon','Pennsylvania','Rhode Island','South Carolina','South Dakota','Tennessee','Texas','Utah','Virginia','Vermont','Washington','Wisconsin','West Virginia','Wyoming']
    ),
    state_code
  ) AS state_name,
  state_code,
  facility_id,
  reject_reason,
  snapshot_date,
  '{run_ts}' AS run_ts
FROM dedup d
WHERE NOT EXISTS (
  SELECT 1
  FROM medlaunch_db.stage3_rejects_per_state t
  WHERE t.snapshot_date = d.snapshot_date
    AND t.state_code   = d.state_code
    AND t.facility_id  = d.facility_id
    AND t.reject_reason = d.reject_reason
);
"""


def build_unload_select_processed(run_ts: str) -> str:
    """Select the processed rows for this run, cast to VARCHARs for clean CSV."""
    return f"""
SELECT
  CAST(state_name AS VARCHAR)                      AS state_name,
  CAST(state_code AS VARCHAR)                      AS state_code,
  CAST(facilities_currently_accredited AS VARCHAR) AS facilities_currently_accredited,
  CAST(snapshot_date AS VARCHAR)                   AS snapshot_date,
  CAST(run_ts AS VARCHAR)                          AS run_ts
FROM medlaunch_db.stage3_accredited_facilities_per_state
WHERE run_ts = '{run_ts}'
"""


def build_unload_select_rejects(run_ts: str) -> str:
    """Select the reject rows for this run, cast to VARCHARs for clean CSV."""
    return f"""
SELECT
  CAST(facility_id AS VARCHAR)   AS facility_id,
  CAST(state_code AS VARCHAR)    AS state_code,
  CAST(reject_reason AS VARCHAR) AS reject_reason,
  CAST(snapshot_date AS VARCHAR) AS snapshot_date,
  CAST(run_ts AS VARCHAR)        AS run_ts
FROM medlaunch_db.stage3_rejects_per_state
WHERE run_ts = '{run_ts}'
"""


# ---------- handler ----------
def handler(event: Dict[str, Any], context):
    """
    Lambda entrypoint.
    - If invoked by S3, ignore non-bronze *.jsonl events.
    - Build a unique run_ts and execute Stage 3 inserts + CSV exports.
    """
    # S3 trigger safety: only respond to bronze *.jsonl
    if "Records" in event:
        rec = event["Records"][0]
        key = rec["s3"]["object"]["key"]
        if not key.endswith(".jsonl") or not key.startswith(
            "bronze-raw-ingested-data/batch/"
        ):
            log(event="skip_event", key=key)
            return {"skipped": True, "key": key}

    # Unique run ID (timestamp + short random suffix)
    run_ts = (
        datetime.now(timezone.utc).strftime("%Y-%m-%d-%H-%M-%S")
        + "-"
        + uuid.uuid4().hex[:6]
    )
    log(event="run_start", run_ts=run_ts)

    # Inserts (write to Parquet-backed Stage 3 tables)
    run_athena(build_insert_processed(run_ts), context.get_remaining_time_in_millis())
    run_athena(build_insert_rejects(run_ts), context.get_remaining_time_in_millis())

    # Row counts for this run (used to decide whether to UNLOAD)
    proc_rows = int(
        run_athena_scalar(
            f"SELECT COALESCE(COUNT(*),0) FROM medlaunch_db.stage3_accredited_facilities_per_state WHERE run_ts='{run_ts}'",
            context.get_remaining_time_in_millis(),
        )
    )
    rej_rows = int(
        run_athena_scalar(
            f"SELECT COALESCE(COUNT(*),0) FROM medlaunch_db.stage3_rejects_per_state WHERE run_ts='{run_ts}'",
            context.get_remaining_time_in_millis(),
        )
    )
    log(event="insert_counts", processed=proc_rows, rejects=rej_rows, run_ts=run_ts)

    # Build UNLOAD targets and skip if already exported (idempotent)
    proc_target = _unload_target(CSV_PROCESSED_PREFIX, run_ts)
    rej_target = _unload_target(CSV_REJECTED_PREFIX, run_ts)

    if proc_rows > 0 and not _s3_prefix_exists(proc_target):
        run_athena(
            _build_unload(build_unload_select_processed(run_ts), proc_target),
            context.get_remaining_time_in_millis(),
        )
    else:
        log(
            event="skip_unload_processed",
            reason="no_rows_or_exists",
            rows=proc_rows,
            target=proc_target,
        )

    if rej_rows > 0 and not _s3_prefix_exists(rej_target):
        run_athena(
            _build_unload(build_unload_select_rejects(run_ts), rej_target),
            context.get_remaining_time_in_millis(),
        )
    else:
        log(
            event="skip_unload_rejects",
            reason="no_rows_or_exists",
            rows=rej_rows,
            target=rej_target,
        )

    log(event="run_done", run_ts=run_ts)
    return {
        "status": "ok",
        "run_ts": run_ts,
        "csv_processed_prefix": _ensure_slash(proc_target),
        "csv_rejected_prefix": _ensure_slash(rej_target),
        "inserted": {"processed": proc_rows, "rejects": rej_rows},
    }
