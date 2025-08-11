"""
make_facility_batch.py

Generate a synthetic batch of healthcare facility records in NDJSON (one JSON per line)
under: data/batch/<snapshot_date>/

Why NDJSON?
- Easy to stream/append and Lambda-friendly (no need to load entire arrays).
- Mirrors common ingestion patterns (logs/events/feeds).

Usage examples:
  # Default IDs start at FAC00001
  python scripts/make_facility_batch.py --n 1000 --chunk 100 --snapshot 2025-08-10

  # Different snapshot with non-overlapping IDs (start at FAC00021)
  python scripts/make_facility_batch.py --n 50 --chunk 10 --snapshot 2025-08-11 --start-index 21

Outputs:
  data/batch/<snapshot_date>/facilities_0001.jsonl
  data/batch/<snapshot_date>/facilities_0002.jsonl
  ...
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from faker import Faker

# ------------------------------ Config / Constants ---------------------------

US_STATES: Sequence[str] = (
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DC",
    "DE",
    "FL",
    "GA",
    "HI",
    "IA",
    "ID",
    "IL",
    "IN",
    "KS",
    "KY",
    "LA",
    "MA",
    "MD",
    "ME",
    "MI",
    "MN",
    "MO",
    "MS",
    "MT",
    "NC",
    "ND",
    "NE",
    "NH",
    "NJ",
    "NM",
    "NV",
    "NY",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VA",
    "VT",
    "WA",
    "WI",
    "WV",
    "WY",
)

SERVICES: Sequence[str] = (
    "Primary Care",
    "Pediatrics",
    "Radiology",
    "Emergency Care",
    "Surgery",
    "Cardiology",
    "Orthopedics",
    "Maternity",
    "Oncology",
    "Laboratory",
    "Dermatology",
    "Gastroenterology",
)

LAB_NAMES: Sequence[str] = (
    "Hematology Lab",
    "Microbiology Lab",
    "Clinical Chemistry Lab",
    "Pathology Lab",
    "Molecular Diagnostics Lab",
)

CERTS: Sequence[str] = ("CLIA", "CAP")
ACCREDITORS: Sequence[str] = ("Joint Commission", "NCQA")


@dataclass(frozen=True)
class Args:
    """Parsed CLI arguments in a typed container."""

    n: int
    chunk: int
    snapshot: str
    outdir: str
    seed: int
    start_index: int  # new: first facility index to use (FACxxxxx)


# ------------------------------ Utilities -----------------------------------


def parse_args() -> Args:
    """Parse and validate CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Generate synthetic facility NDJSON for ELT testing."
    )
    parser.add_argument(
        "--n", type=int, default=1000, help="Total records to generate (default: 1000)"
    )
    parser.add_argument(
        "--chunk", type=int, default=100, help="Records per file (default: 100)"
    )
    parser.add_argument(
        "--snapshot",
        type=str,
        default=date.today().isoformat(),
        help="Snapshot date in YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--outdir", type=str, default="data/batch", help="Base output directory"
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed for reproducibility"
    )
    parser.add_argument(
        "--start-index",
        type=int,
        default=1,
        help="First facility index to use for FACxxxxx (default: 1)",
    )

    ns = parser.parse_args()

    # Basic validation with clear messages (fail fast)
    if ns.n <= 0:
        parser.error("--n must be > 0")
    if ns.chunk <= 0:
        parser.error("--chunk must be > 0")
    if ns.start_index <= 0:
        parser.error("--start-index must be > 0")
    try:
        datetime.strptime(ns.snapshot, "%Y-%m-%d")
    except ValueError:
        parser.error(
            "Invalid --snapshot format. Use YYYY-MM-DD"
        )  # argparse shows usage

    return Args(
        n=ns.n,
        chunk=ns.chunk,
        snapshot=ns.snapshot,
        outdir=ns.outdir,
        seed=ns.seed,
        start_index=ns.start_index,
    )


def configure_logging() -> None:
    """Set up concise, informative logging."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def rand_services(rng: random.Random) -> List[str]:
    """Pick a small, diverse set of services for a facility."""
    k = rng.randint(1, min(6, len(SERVICES)))
    return sorted(rng.sample(list(SERVICES), k))


def rand_labs(rng: random.Random) -> List[Dict[str, Any]]:
    """Optionally attach up to two labs with 1–2 certifications each."""
    labs: List[Dict[str, Any]] = []
    for _ in range(rng.randint(0, 2)):
        labs.append(
            {
                "lab_name": rng.choice(list(LAB_NAMES)),
                "certifications": sorted(
                    list({rng.choice(list(CERTS)) for _ in range(rng.randint(1, 2))})
                ),
            }
        )
    return labs


def rand_accreditations(rng: random.Random, snapshot: date) -> List[Dict[str, Any]]:
    """
    Create 1–2 accreditations whose 'valid_until' dates are spread around the snapshot.
    This ensures some facilities are 'expiring soon' and will exercise the filter logic.
    """
    accs: List[Dict[str, Any]] = []
    for _ in range(rng.randint(1, 2)):
        offset_days = rng.randint(-90, 900)  # past to far future
        valid_until = snapshot + timedelta(days=offset_days)
        body = rng.choice(list(ACCREDITORS))
        suffix = rng.randint(100, 999)
        accs.append(
            {
                "accreditation_body": body,
                "accreditation_id": f"{'JC' if body == 'Joint Commission' else 'NCQA'}{suffix}",
                "valid_until": valid_until.isoformat(),
            }
        )
    return accs


def build_record(
    idx: int, fake: Faker, rng: random.Random, snapshot: date
) -> Dict[str, Any]:
    """
    Build one facility record shaped like the sample dataset plus an explicit snapshot_date.
    Keep fields minimal (principle of minimum necessary) for downstream processing.
    """
    city = fake.city()
    state = rng.choice(list(US_STATES))
    return {
        "facility_id": f"FAC{idx:05d}",
        "facility_name": f"{city} Medical Center",
        "location": {
            "address": fake.street_address(),
            "city": city,
            "state": state,
            "zip": fake.postcode(),
        },
        "employee_count": rng.randint(20, 1000),
        "services": rand_services(rng),
        "labs": rand_labs(rng),
        "accreditations": rand_accreditations(rng, snapshot),
        "snapshot_date": snapshot.isoformat(),
    }


def write_ndjson(path: Path, records: Iterable[Dict[str, Any]]) -> None:
    """Write records to `path` as NDJSON (UTF-8), one object per line."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


# ------------------------------ Main ----------------------------------------


def main() -> None:
    configure_logging()
    args = parse_args()

    # Deterministic randomness helps reproducibility during demos/debugging.
    rng = random.Random(args.seed)
    fake = Faker("en_US")
    Faker.seed(args.seed)

    snapshot_dt = datetime.strptime(args.snapshot, "%Y-%m-%d").date()
    out_base = Path(args.outdir).expanduser().resolve()
    out_dir = out_base / args.snapshot

    total_files = math.ceil(args.n / args.chunk)
    logging.info(
        "Generating %s records across %s files (chunk=%s) for snapshot %s (start-index=%s)",
        args.n,
        total_files,
        args.chunk,
        args.snapshot,
        args.start_index,
    )

    # Generate and write file-by-file without holding everything in memory.
    start_idx = args.start_index
    remaining = args.n

    for file_no in range(1, total_files + 1):
        file_count = min(args.chunk, remaining)
        this_path = out_dir / f"facilities_{file_no:04d}.jsonl"

        # Build a small iterator that yields exactly file_count records with correct indices.
        records_iter = (
            build_record(idx, fake, rng, snapshot_dt)
            for idx in range(start_idx, start_idx + file_count)
        )
        write_ndjson(this_path, records_iter)
        logging.info("Wrote %s", this_path)

        start_idx += file_count
        remaining -= file_count
        if remaining <= 0:
            break

    logging.info("Done. Output folder: %s", out_dir)


if __name__ == "__main__":
    main()
