/* ============================================================================
Stage 1 — Rejects (CTAS, Parquet, partitioned)

Purpose
  Persist rows that FAIL Stage-1 DQ rules so we can inspect and fix them later.

DQ rules (fail if any is true)
  - facility_id does NOT start with 'FAC' (case-insensitive)
  - facility_name is blank after TRIM
  - employee_count is NULL or negative
  - number_of_offered_services is negative (should never happen, but checked)

Output
  Table:  medlaunch_db.stage1_facility_metrics_rejects
  Files:  s3://medlaunch-elt-datalake-050451385876-us-east-1/
          silver-cleaned-stage1-parquet/stage1_facility_metrics_rejects/
  Format: PARQUET + SNAPPY, partitioned by snapshot_date
============================================================================ */

CREATE TABLE medlaunch_db.stage1_facility_metrics_rejects
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://medlaunch-elt-datalake-050451385876-us-east-1/silver-cleaned-stage1-parquet/stage1_facility_metrics_rejects/',
  partitioned_by = ARRAY['snapshot_date']
) AS
WITH src AS (
  SELECT
    TRIM(facility_id)                                   AS facility_id_t,
    TRIM(facility_name)                                 AS facility_name_t,
    employee_count,
    COALESCE(cardinality(services), 0)                  AS number_of_offered_services,
    CASE WHEN COALESCE(cardinality(accreditations),0) >= 1
         THEN TRY(CAST(accreditations[1].valid_until AS DATE))
    END                                                 AS expiry_date_of_first_accreditation,
    snapshot_date
  FROM medlaunch_db.bronze_facilities_json_np
),
flags AS (
  SELECT
    *,
    -- DQ flags (true means "bad")
    (NOT regexp_like(UPPER(facility_id_t), '^FAC'))                 AS bad_facility_id_prefix,
    (NULLIF(facility_name_t, '') IS NULL)                           AS bad_facility_name_blank,
    (employee_count IS NULL OR employee_count < 0)                  AS bad_employee_count,
    (number_of_offered_services < 0)                                AS bad_services_count
  FROM src
),
rejects AS (
  SELECT
    *,
    CASE
      WHEN bad_facility_id_prefix  THEN 'facility_id_does_not_start_with_FAC'
      WHEN bad_facility_name_blank THEN 'facility_name_blank_after_trim'
      WHEN bad_employee_count      THEN 'employee_count_negative_or_null'
      WHEN bad_services_count      THEN 'number_of_offered_services_negative'
      ELSE 'unknown'
    END AS reject_reason
  FROM flags
  WHERE bad_facility_id_prefix
     OR bad_facility_name_blank
     OR bad_employee_count
     OR bad_services_count
)
SELECT
  facility_id_t                                   AS facility_id,
  facility_name_t                                 AS facility_name,
  employee_count,
  number_of_offered_services,
  expiry_date_of_first_accreditation,
  reject_reason,
  snapshot_date                                    -- partition column LAST
FROM rejects;
