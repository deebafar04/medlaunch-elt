/* ============================================================================
Stage 1 — Curated Facility Metrics
Auto-append all NEW snapshot_date partitions (no manual date)

What this does
  - Finds distinct snapshot_date values in bronze that are NOT present in curated.
  - Appends only those partitions to medlaunch_db.stage1_facility_metrics.
  - Keeps your Stage-1 DQ (facility_id starts with FAC; trimmed facility_name not blank;
    non-negative employee_count & number_of_offered_services).
  - Dedupes per (snapshot_date, facility_id).

How to run
  1) Set Database = medlaunch_db (or: USE medlaunch_db).
  2) Run this single statement.

Notes
  - If curated is empty, this will load ALL bronze dates.
  - If you re-run after adding more bronze data for the same dates, you’ll either need to
    delete the target partition(s) first or switch to your “overwrite partition” process.
  - For very large bronze ranges, consider limiting to recent days (see comment below).
============================================================================ */

INSERT INTO medlaunch_db.stage1_facility_metrics
WITH curated_dates AS (
  SELECT DISTINCT CAST(snapshot_date AS VARCHAR) AS sd FROM medlaunch_db.stage1_facility_metrics
),
bronze_dates AS (
  SELECT DISTINCT CAST(snapshot_date AS VARCHAR) AS sd FROM medlaunch_db.bronze_facilities_json_np
  -- Optional: only consider recent dates to reduce scan:
  -- WHERE snapshot_date >= date_add('day', -7, current_date)
),
to_load AS (
  -- Dates present in bronze but not already in curated
  SELECT b.snapshot_date
  FROM bronze_dates b
  LEFT JOIN curated_dates c
    ON b.snapshot_date = c.snapshot_date
  WHERE c.snapshot_date IS NULL
),
src AS (
  SELECT
    TRIM(facility_id)                                   AS facility_id_t,      -- DQ: must start with 'FAC'
    TRIM(facility_name)                                 AS facility_name_t,    -- DQ: trim-only, not blank
    employee_count,
    COALESCE(cardinality(services), 0)                  AS number_of_offered_services,
    CASE WHEN COALESCE(cardinality(accreditations), 0) >= 1
         THEN TRY(CAST(accreditations[1].valid_until AS DATE))
    END                                                 AS expiry_date_of_first_accreditation,
    snapshot_date
  FROM medlaunch_db.bronze_facilities_json_np
  WHERE CAST(snapshot_date AS VARCHAR) IN (SELECT sd FROM to_load)
),
validated AS (
  SELECT *
  FROM src
  WHERE
    regexp_like(UPPER(facility_id_t), '^FAC')           -- id starts with FAC (case-insensitive)
    AND NULLIF(facility_name_t, '') IS NOT NULL         -- name present after trim
    AND employee_count IS NOT NULL AND employee_count >= 0
    AND number_of_offered_services >= 0
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY snapshot_date, facility_id_t
      ORDER BY facility_id_t
    ) AS rn
  FROM validated
)
SELECT
  facility_id_t                                   AS facility_id,
  facility_name_t                                 AS facility_name,
  employee_count,
  number_of_offered_services,
  expiry_date_of_first_accreditation,
  snapshot_date                                   -- partition column LAST (Athena requirement)
FROM dedup
WHERE rn = 1;
