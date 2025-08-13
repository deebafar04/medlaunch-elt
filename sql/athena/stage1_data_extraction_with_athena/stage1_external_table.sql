/* -----------------------------------------------------------------------------
SQL Script Name:
    stage1_external_table.sql

Purpose:
    Create a non-partitioned Athena external table over the raw NDJSON facility
    data stored in S3 under bronze-raw-ingested-data. This is the bronze-level
    dataset from which Stage 1 curated and rejects tables will be derived.

What This Script Does:
    - Defines schema mapping for the JSON fields.
    - Uses Athena's JSON SerDe to parse each record (one JSON object per line).
    - Points the table to the S3 location containing raw ingested files.
    - Allows downstream stages to query raw data with consistent structure.

Inputs:
    Source Files Location: 
        s3://medlaunch-elt-datalake-050451385876-us-east-1/bronze-raw-ingested-data/
    File Format: Newline-delimited JSON (NDJSON)
    Columns:
        facility_id         string
        facility_name       string
        location            struct<address:string, city:string, state:string, zip:string>
        employee_count      bigint
        services            array<string>
        labs                array<struct<lab_name:string, certifications:array<string>>>
        accreditations      array<struct<accreditation_body:string, accreditation_id:string, valid_until:string>>
        snapshot_date       date

Outputs:
    Athena Table: medlaunch_db.bronze_facilities_json_np
    Format: JSON (via org.openx.data.jsonserde.JsonSerDe)
    SerDe Properties: ignore malformed JSON lines

----------------------------------------------------------------------------- */
--Athena runs one command at a time.
--CREATE DATABASE IF NOT EXISTS medlaunch_db;

--DROP TABLE IF EXISTS medlaunch_db.bronze_facilities_json_np;

CREATE EXTERNAL TABLE medlaunch_db.bronze_facilities_json_np (
  facility_id         string,
  facility_name       string,
  location            struct<
                        address:string,
                        city:string,
                        state:string,
                        zip:string
                      >,
  employee_count      bigint,
  services            array<string>,
  labs                array<struct<
                        lab_name:string,
                        certifications:array<string>
                      >>,
  accreditations      array<struct<
                        accreditation_body:string,
                        accreditation_id:string,
                        valid_until:string
                      >>,
  snapshot_date       date
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
LOCATION 's3://medlaunch-elt-datalake-050451385876-us-east-1/bronze-raw-ingested-data/'
TBLPROPERTIES (
  'classification' = 'json',
  'projection.enabled' = 'false'
);
