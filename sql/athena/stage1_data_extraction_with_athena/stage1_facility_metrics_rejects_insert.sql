/* -----------------------------------------------------------------------------
SQL Script Name:
    stage1_facility_metrics_rejects_insert.sql

Purpose:
    Insert only NEW snapshot_date partitions into the Stage 1
    rejects table, capturing rows that fail DQ checks for inspection/remediation.

What This Script Does:
    1) Finds snapshot_date values present in bronze_facilities_json_np but not yet present in rejects.
    2) For those dates only, applies Stage 1 DQ rules and assigns a reject_reason.
    3) Appends results to stage1_facility_metrics_rejects (Parquet, partitioned).

Inputs:
    Source Table: medlaunch_db.bronze_facilities_json_np
    
Outputs:
    Target Table: medlaunch_db.stage1_facility_metrics_rejects
    Format: Parquet (Snappy), partitioned by snapshot_date
    Behavior: Appends only missing snapshot_date partitions
----------------------------------------------------------------------------- */

INSERT INTO medlaunch_db.stage1_facility_metrics_rejects
WITH existing_dates AS (
  SELECT DISTINCT snapshot_date
  FROM medlaunch_db.stage1_facility_metrics_rejects
),
bronze_dates AS (
  SELECT DISTINCT snapshot_date
  FROM medlaunch_db.bronze_facilities_json_np
  WHERE snapshot_date IS NOT NULL
),
to_load AS (
  -- Dates present in bronze but not yet in rejects
  SELECT b.snapshot_date
  FROM bronze_dates b
  LEFT JOIN existing_dates e
    ON b.snapshot_date = e.snapshot_date
  WHERE e.snapshot_date IS NULL
),
src AS (
  SELECT
    TRIM(facility_id)                                   AS facility_id_t,
    TRIM(facility_name)                                 AS facility_name_t,
    TRY_CAST(employee_count AS BIGINT)                  AS employee_count_t,
    COALESCE(cardinality(services), 0)                  AS number_of_offered_services_t,
    CASE
      WHEN COALESCE(cardinality(accreditations), 0) >= 1
        THEN TRY(CAST(accreditations[1].valid_until AS DATE))
    END                                                 AS expiry_date_of_first_accreditation_t,
    snapshot_date
  FROM medlaunch_db.bronze_facilities_json_np
  WHERE snapshot_date IN (SELECT snapshot_date FROM to_load)
),
flags AS (
  SELECT
    *,
    -- DQ flags (true means "bad")
    (facility_id_t IS NULL OR NOT regexp_like(UPPER(facility_id_t), '^FAC'))  AS bad_facility_id_prefix,
    (NULLIF(facility_name_t, '') IS NULL)                                     AS bad_facility_name_blank,
    (employee_count_t IS NULL OR employee_count_t < 0)                        AS bad_employee_count,
    (number_of_offered_services_t < 0)                                        AS bad_services_count
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
  facility_id_t                           AS facility_id,
  facility_name_t                         AS facility_name,
  employee_count_t                        AS employee_count,
  number_of_offered_services_t            AS number_of_offered_services,
  expiry_date_of_first_accreditation_t    AS expiry_date_of_first_accreditation,
  reject_reason,
  snapshot_date
FROM rejects;
