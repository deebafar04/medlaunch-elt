/* -----------------------------------------------------------------------------
SQL Script Name:
    stage1_facility_metrics_insert.sql

Purpose:
    Insert only NEW records by snapshot_date partitions from bronze_facilities_json_np
    into stage1_facility_metrics table.

What This Script Does:
    1) Finds snapshot_date values present in bronze_facilities_json_np but not yet present in stage1_facility_metrics.
    2) For those dates only, applies DQ rules and dedupes per (snapshot_date, facility_id).
    3) Inserts cleaned records into medlaunch_db.stage1_facility_metrics.

Inputs:
    Source Table: medlaunch_db.bronze_facilities_json_np
    
Outputs:
    Target Table: medlaunch_db.stage1_facility_metrics
    Format: Parquet (Snappy), partitioned by snapshot_date
    Behavior: Appends only new snapshot_date partitions

----------------------------------------------------------------------------- */

INSERT INTO medlaunch_db.stage1_facility_metrics
WITH curated_dates AS (
  SELECT DISTINCT snapshot_date
  FROM medlaunch_db.stage1_facility_metrics
),
bronze_dates AS (
  SELECT DISTINCT snapshot_date
  FROM medlaunch_db.bronze_facilities_json_np
  WHERE snapshot_date IS NOT NULL
),
to_load AS (
  -- Dates present in bronze but absent in Stage 1
  SELECT b.snapshot_date
  FROM bronze_dates b
  LEFT JOIN curated_dates c
    ON b.snapshot_date = c.snapshot_date
  WHERE c.snapshot_date IS NULL
),
src AS (
  SELECT
    TRIM(facility_id)                                   AS facility_id_t,      -- must start with 'FAC'
    TRIM(facility_name)                                 AS facility_name_t,    -- not blank after trim
    TRY_CAST(employee_count AS BIGINT)                  AS employee_count_t,   -- tolerate bad types
    COALESCE(cardinality(services), 0)                  AS number_of_offered_services_t,
    CASE
      WHEN COALESCE(cardinality(accreditations), 0) >= 1
        THEN TRY(CAST(accreditations[1].valid_until AS DATE))
    END                                                 AS expiry_date_of_first_accreditation_t,
    snapshot_date
  FROM medlaunch_db.bronze_facilities_json_np
  WHERE snapshot_date IN (SELECT snapshot_date FROM to_load)
),
validated AS (
  SELECT *
  FROM src
  WHERE
    facility_id_t IS NOT NULL
    AND regexp_like(UPPER(facility_id_t), '^FAC')       -- case-insensitive prefix check
    AND NULLIF(facility_name_t, '') IS NOT NULL
    AND employee_count_t IS NOT NULL AND employee_count_t >= 0
    AND number_of_offered_services_t >= 0
    AND snapshot_date IS NOT NULL
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
  facility_id_t                            AS facility_id,
  facility_name_t                          AS facility_name,
  employee_count_t                         AS employee_count,
  number_of_offered_services_t             AS number_of_offered_services,
  expiry_date_of_first_accreditation_t     AS expiry_date_of_first_accreditation,
  snapshot_date
FROM dedup
WHERE rn = 1;
