/* -----------------------------------------------------------------------------
SQL Script Name:
    stage1_facility_metrics_create.sql

What This Script Does:
    1. Reads data from the bronze_facilities_json_np.
    2. Applies data quality rules:
        - facility_id must start with 'FAC' (case-insensitive).
        - facility_name must be present after trimming.
        - employee_count >= 0 and not null.
        - number_of_offered_services >= 0.
    3. Calculates:
        - number_of_offered_services from array length.
        - expiry_date_of_first_accreditation from first accreditation record (nullable).
    4. Removes duplicates per facility_id + snapshot_date.
    5. Writes partitioned Parquet output to the Stage 1 S3 path.

Inputs:
    Source Table: medlaunch_db.bronze_facilities_json_np
    Columns Required:
        facility_id (string)
        facility_name (string)
        employee_count (bigint/int)
        services (array)
        accreditations (array of structs with valid_until)
        snapshot_date (date/string)

Outputs:
    Destination Table: medlaunch_db.stage1_facility_metrics
    Output Format: Parquet (Snappy compression)
    Partition Key: snapshot_date
    Output Location: 
        s3://medlaunch-elt-datalake-050451385876-us-east-1/stage1-athena-parquet-results/stage1_facility_metrics/

----------------------------------------------------------------------------- *
----------------------------------------------------------------------------- */

CREATE TABLE medlaunch_db.stage1_facility_metrics
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://medlaunch-elt-datalake-050451385876-us-east-1/stage1-athena-parquet-results/stage1_facility_metrics/',
  partitioned_by = ARRAY['snapshot_date']
) AS
WITH src AS (
  SELECT
    TRIM(facility_id)                                           AS facility_id_t,      -- must start with 'FAC'
    TRIM(facility_name)                                         AS facility_name_t,    -- not blank after trim
    TRY_CAST(employee_count AS BIGINT)                           AS employee_count_t,   -- tolerate bad types
    COALESCE(cardinality(services), 0)                           AS number_of_offered_services_t,
    CASE
      WHEN COALESCE(cardinality(accreditations), 0) >= 1
        THEN TRY(CAST(accreditations[1].valid_until AS DATE))
    END                                                          AS expiry_date_of_first_accreditation_t,
    snapshot_date
  FROM medlaunch_db.bronze_facilities_json_np
),
validated AS (
  SELECT *
  FROM src
  WHERE
    facility_id_t IS NOT NULL
    AND regexp_like(UPPER(facility_id_t), '^FAC')                -- id starts with FAC (case-insensitive)
    AND NULLIF(facility_name_t, '') IS NOT NULL                  -- name present after trim
    AND employee_count_t IS NOT NULL AND employee_count_t >= 0
    AND number_of_offered_services_t >= 0
    AND snapshot_date IS NOT NULL                                -- keep partitions clean
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
  facility_id_t                           AS facility_id,
  facility_name_t                         AS facility_name,
  employee_count_t                        AS employee_count,
  number_of_offered_services_t            AS number_of_offered_services,
  expiry_date_of_first_accreditation_t    AS expiry_date_of_first_accreditation,
  snapshot_date                           -- keeping last for readability
FROM dedup
WHERE rn = 1;
