"""
Script: stage2_expiring_accreditations.py
Purpose: Sweep ALL NDJSON snapshots, find accreditations expiring soon, and
         write a single CSV + per-file quarantine to S3.

What this function does
- Lists every *.jsonl (and *.jsonl.gz) under s3://medlaunch-elt-datalake-050451385876-us-east-1/bronze-raw-ingested-data/batch/**/*
- Validates a minimal schema and applies DQ:
    • facility_id starts with 'FAC' (case-insensitive)
    • facility_name is not blank after trim
    • employee_count is non-negative (enforced by parsing)
- For each accreditation:
    • valid_until within WINDOW_DAYS from “today” → add one CSV (and Parquet) row
    • missing/invalid valid_until → add one quarantine line (for that accreditation only)
- Writes:
    • CSV     → s3://<bucket>/python-computed-outputs/expiring-soon/run_date=<YYYY-MM-DD>/expiring_soon.csv
    • Rejects → s3://<bucket>/python-computed-outputs/quarantine/<snapshot_date>/<file>_rejects.jsonl

Inputs (S3 layout)
- bronze-raw-ingested-data/batch/<YYYY-MM-DD>/<any>.jsonl    (or .jsonl.gz)

Outputs
- Aggregated CSV and optional Parquet of “expiring soon” rows, plus NDJSON quarantine files.

Environment
- DATA_BUCKET: data lake bucket (e.g., medlaunch-elt-datalake-...-us-east-1)
- WINDOW_DAYS: optional int (default 180)

IAM needed (Lambda execution role)
- s3:ListBucket on the bucket
- s3:GetObject on bronze-raw-ingested-data/*
- s3:PutObject on python-computed-outputs/*

Notes
- Designed to be triggered manually (CLI) or by Step Functions. It ignores the event payload.
- Uses connection pooling and streaming to keep memory low and throughput high.
- If awswrangler isn’t in the layer, Parquet is skipped gracefully.
"""

from __future__ import annotations

# ===== Standard Library =====
import csv
import gzip
import io
import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ===== AWS / Third-Party =====
import boto3
from botocore.config import Config

# Optional Parquet support
try:
    import awswrangler as wr  # type: ignore

    _HAVE_WR = True
except Exception:
    _HAVE_WR = False

# ===== Logging =====
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

# ===== Constants / Paths =====
BRONZE_PREFIX = "bronze-raw-ingested-data/batch/"
CSV_OUT_PREFIX = "python-computed-outputs/expiring-soon"
PARQUET_OUT_PREFIX = "gold-curated-stage2-parquet/expiring-soon"
QUARANTINE_PREFIX = "python-computed-outputs/quarantine"
CSV_HEADER = [
    "facility_id",
    "facility_name",
    "address",
    "city",
    "state",
    "zip",
    "employee_count",
    "accreditation_body",
    "accreditation_id",
    "valid_until",
    "days_until_expiry",
    "run_date",
    "snapshot_date",
]

# ===== Boto3 clients (module-scope for connection reuse) =====
_BOTO_CFG = Config(
    retries={"max_attempts": 5, "mode": "standard"},
    max_pool_connections=50,
    read_timeout=60,
    connect_timeout=10,
)

S3 = boto3.client("s3", config=_BOTO_CFG)
LIST_PAGINATOR = S3.get_paginator("list_objects_v2")


# ========= Lightweight parsing & DQ (no Pydantic: faster in Lambda) =========
def _parse_date(raw: Optional[str]) -> Optional[date]:
    """Parse 'YYYY-MM-DD' or ISO datetime; return None if invalid."""
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw[:10]).date()
    except Exception:
        return None


def _is_good_facility_id(s: Any) -> bool:
    return isinstance(s, str) and s.strip().upper().startswith("FAC")


def _is_good_name(s: Any) -> bool:
    return isinstance(s, str) and s.strip() != ""


def _parse_employee_count(v: Any) -> Optional[int]:
    try:
        n = int(v)
        return n if n >= 0 else None
    except Exception:
        return None


@dataclass
class Outputs:
    expiring_rows: List[Dict[str, Any]]
    rejects: List[Dict[str, Any]]


def _extract_snapshot_from_key(key: str) -> Optional[str]:
    """
    Expect keys like:
      bronze-raw-ingested-data/batch/<YYYY-MM-DD>/facilities_0001.jsonl
    """
    try:
        return key.split("/batch/")[1].split("/")[0]
    except Exception:
        return None


def _process_facility(
    record: Dict[str, Any],
    window_days: int,
    run_date: date,
    snapshot_date: str,
    source_key: str,
) -> Outputs:
    """
    Validate one facility and emit expiring-accreditation rows + quarantine items.
    Designed to be allocation-light and branch-predictable.
    """
    expiring: List[Dict[str, Any]] = []
    rejects: List[Dict[str, Any]] = []

    fid = record.get("facility_id")
    fname = record.get("facility_name")
    loc = record.get("location") or {}
    accs = record.get("accreditations")

    # Minimal, fast DQ checks (no Pydantic here for speed)
    if not _is_good_facility_id(fid):
        rejects.append(
            {
                "reject_reason": "bad_facility_id_prefix",
                "facility_id": fid,
                "source_key": source_key,
                "snapshot_date": snapshot_date,
            }
        )
        return Outputs(expiring, rejects)

    if not _is_good_name(fname):
        rejects.append(
            {
                "reject_reason": "facility_name_blank_after_trim",
                "facility_id": fid,
                "source_key": source_key,
                "snapshot_date": snapshot_date,
            }
        )
        return Outputs(expiring, rejects)

    emp = _parse_employee_count(record.get("employee_count"))
    if emp is None:
        rejects.append(
            {
                "reject_reason": "employee_count_negative_or_invalid",
                "facility_id": fid,
                "source_key": source_key,
                "snapshot_date": snapshot_date,
            }
        )
        return Outputs(expiring, rejects)

    if not isinstance(accs, list) or not accs:
        rejects.append(
            {
                "reject_reason": "accreditations_missing_or_empty",
                "facility_id": fid,
                "source_key": source_key,
                "snapshot_date": snapshot_date,
            }
        )
        return Outputs(expiring, rejects)

    # Precompute once
    cutoff = run_date + timedelta(days=window_days)

    # Per-accreditation evaluation (tight loop)
    address = (loc or {}).get("address")
    city = (loc or {}).get("city")
    state = (loc or {}).get("state")
    zipc = (loc or {}).get("zip")
    f_id_stripped = str(fid).strip()
    f_name_stripped = str(fname).strip()
    run_iso = run_date.isoformat()

    for acc in accs:
        acc = acc or {}
        dt = _parse_date(acc.get("valid_until"))
        if dt is None:
            rejects.append(
                {
                    "reject_reason": "invalid_or_missing_valid_until",
                    "facility_id": f_id_stripped,
                    "accreditation_id": acc.get("accreditation_id"),
                    "accreditation_body": acc.get("accreditation_body"),
                    "raw_valid_until": acc.get("valid_until"),
                    "source_key": source_key,
                    "snapshot_date": snapshot_date,
                }
            )
            continue

        if run_date <= dt <= cutoff:
            expiring.append(
                {
                    "facility_id": f_id_stripped,
                    "facility_name": f_name_stripped,
                    "address": address,
                    "city": city,
                    "state": state,
                    "zip": zipc,
                    "employee_count": emp,
                    "accreditation_body": acc.get("accreditation_body"),
                    "accreditation_id": acc.get("accreditation_id"),
                    "valid_until": dt.isoformat(),
                    "days_until_expiry": (dt - run_date).days,
                    "run_date": run_iso,
                    "snapshot_date": snapshot_date,
                }
            )

    return Outputs(expiring, rejects)


# ============================ S3 IO Helpers ============================
def _list_all_bronze_keys(bucket: str, prefix: str = BRONZE_PREFIX) -> List[str]:
    """List every *.jsonl (or *.jsonl.gz) under the bronze batch prefix."""
    keys: List[str] = []
    for page in LIST_PAGINATOR.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".jsonl") or k.endswith(".jsonl.gz"):
                keys.append(k)
    return keys


def _iter_ndjson_from_s3(bucket: str, key: str) -> Iterable[Tuple[Dict[str, Any], int]]:
    """
    Yield (json_obj, line_no) for each line. Supports plain and gzip NDJSON.
    Uses streaming to avoid loading entire objects into memory.
    """
    resp = S3.get_object(Bucket=bucket, Key=key)
    body = resp["Body"]

    # Choose iterator based on extension
    if key.endswith(".gz"):
        # Wrap streaming body with GzipFile for on-the-fly decompression
        with gzip.GzipFile(fileobj=body) as gz:
            for i, line in enumerate(gz, start=1):
                if not line:
                    continue
                try:
                    yield json.loads(line.decode("utf-8")), i
                except json.JSONDecodeError:
                    yield {"__malformed__": True}, i
    else:
        for i, bline in enumerate(body.iter_lines(), start=1):
            if not bline:
                continue
            try:
                yield json.loads(bline.decode("utf-8")), i
            except json.JSONDecodeError:
                yield {"__malformed__": True}, i


def _append_quarantine_ndjson(
    bucket: str, snapshot_date: str, source_key: str, rejects: List[Dict[str, Any]]
) -> None:
    """Write per-file quarantine NDJSON. No-op if rejects empty."""
    if not rejects:
        return
    base = os.path.basename(source_key).replace(".jsonl.gz", "").replace(".jsonl", "")
    out_key = f"{QUARANTINE_PREFIX}/{snapshot_date}/{base}_rejects.jsonl"
    body = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rejects)
    S3.put_object(Bucket=bucket, Key=out_key, Body=body.encode("utf-8"))
    LOG.info("quarantine_written key=%s rows=%s", out_key, len(rejects))


def _write_csv_aggregated(
    bucket: str, rows: List[Dict[str, Any]], run_date: date
) -> Optional[str]:
    """
    Stream a single CSV to /tmp then upload (handles large outputs without big RAM spikes).
    Returns the S3 key if written.
    """
    if not rows:
        return None

    out_key = f"{CSV_OUT_PREFIX}/run_date={run_date.isoformat()}/expiring_soon.csv"
    tmp_path = "/tmp/expiring_soon.csv"

    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        writer.writeheader()
        writer.writerows(rows)

    with open(tmp_path, "rb") as f:
        S3.put_object(Bucket=bucket, Key=out_key, Body=f)

    LOG.info("csv_written key=%s rows=%s", out_key, len(rows))
    return out_key


def _write_parquet_if_available(
    bucket: str, rows: List[Dict[str, Any]], run_date: date
) -> Optional[str]:
    """Optionally write Parquet (awswrangler), returns the S3 folder path if written."""
    if not _HAVE_WR or not rows:
        return None
    import pandas as pd

    df = pd.DataFrame(rows)
    path = f"s3://{bucket}/{PARQUET_OUT_PREFIX}/run_date={run_date.isoformat()}/"
    wr.s3.to_parquet(df=df, path=path, dataset=True, compression="snappy")
    LOG.info("parquet_written path=%s rows=%s", path, len(rows))
    return path


# =============================== Lambda Entry ===============================
def handler(event, context):
    """
    Stage 2 sweep — processes ALL JSONL under bronze batch prefix.
    Ignores the event payload so you can invoke from CLI/Step Functions without ceremony.
    """
    bucket = os.environ["DATA_BUCKET"]
    window_days = int(os.environ.get("WINDOW_DAYS", "180"))
    run_date = datetime.now(timezone.utc).date()

    keys = _list_all_bronze_keys(bucket, BRONZE_PREFIX)
    if not keys:
        LOG.info(
            json.dumps(
                {
                    "stage": "start",
                    "message": "no_keys_found",
                    "bucket": bucket,
                    "prefix": BRONZE_PREFIX,
                }
            )
        )
        return {"processed_files": 0, "expiring": 0, "rejected": 0, "snapshots": []}

    all_expiring: List[Dict[str, Any]] = []
    total_rejects = 0
    snapshots_seen: set[str] = set()

    for key in keys:
        snap = _extract_snapshot_from_key(key) or run_date.isoformat()
        snapshots_seen.add(snap)

        expiring_rows: List[Dict[str, Any]] = []
        rejects: List[Dict[str, Any]] = []

        for rec, line_no in _iter_ndjson_from_s3(bucket, key):
            if rec.get("__malformed__"):
                rejects.append(
                    {
                        "reject_reason": "json_decode_error",
                        "line_no": line_no,
                        "source_key": key,
                        "snapshot_date": snap,
                    }
                )
                continue

            out = _process_facility(rec, window_days, run_date, snap, key)
            expiring_rows.extend(out.expiring_rows)
            rejects.extend(out.rejects)

        _append_quarantine_ndjson(bucket, snap, key, rejects)

        all_expiring.extend(expiring_rows)
        total_rejects += len(rejects)

    csv_key = _write_csv_aggregated(bucket, all_expiring, run_date)
    parquet_path = _write_parquet_if_available(bucket, all_expiring, run_date)

    LOG.info(
        json.dumps(
            {
                "stage": "done",
                "files_processed": len(keys),
                "records_expiring": len(all_expiring),
                "records_rejected": total_rejects,
                "snapshots_seen": sorted(snapshots_seen),
                "csv_key": csv_key,
                "parquet_path": parquet_path,
            }
        )
    )

    return {
        "processed_files": len(keys),
        "expiring": len(all_expiring),
        "rejected": total_rejects,
        "snapshots": sorted(snapshots_seen),
        "csv_key": csv_key,
        "parquet_path": parquet_path,
    }
