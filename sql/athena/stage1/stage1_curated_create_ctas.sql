/* ============================================================================
Stage 1 — Curated Facility Metrics (CTAS, Parquet, partitioned)

Rules:
  - facility_id: must start with 'FAC' (case-insensitive). No digit/length check.
  - facility_name: TRIM only; must not be blank.
  - employee_count: non-negative.
  - number_of_offered_services: non-negative (COALESCE(cardinality(...),0)).
Notes:
  - expiry_date_of_first_accreditation comes from the 1st accreditation via TRY(CAST(... AS DATE));
    if it can’t parse, it will be NULL (allowed).
  - snapshot_date must be the last column (Athena partition rule).
============================================================================ */

CREATE TABLE medlaunch_db.stage1_facility_metrics
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://medlaunch-elt-datalake-050451385876-us-east-1/silver-cleaned-stage1-parquet/stage1_facility_metrics/',
  partitioned_by = ARRAY['snapshot_date']
) AS
WITH src AS (
  SELECT
    TRIM(facility_id)                                   AS facility_id_t,      -- DQ: must start with 'FAC'
    TRIM(facility_name)                                 AS facility_name_t,    -- DQ: trim-only, not blank
    employee_count,
    COALESCE(cardinality(services), 0)                  AS number_of_offered_services,
    CASE WHEN COALESCE(cardinality(accreditations),0) >= 1
         THEN TRY(CAST(accreditations[1].valid_until AS DATE))
    END                                                 AS expiry_date_of_first_accreditation,
    snapshot_date
  FROM medlaunch_db.bronze_facilities_json_np
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
  facility_id_t                                 AS facility_id,
  facility_name_t                               AS facility_name,
  employee_count,
  number_of_offered_services,
  expiry_date_of_first_accreditation,
  snapshot_date                                  -- partition column LAST
FROM dedup
WHERE rn = 1;
