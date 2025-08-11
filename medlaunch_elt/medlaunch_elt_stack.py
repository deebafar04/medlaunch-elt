"""
medlaunch_elt_stack.py

Sets up the S3 foundation for the MedLaunch ELT pipeline with HIPAA-aware defaults:
- Customer-managed KMS key (CMK) for at-rest encryption
- Main data-lake bucket with clear, purpose-driven prefixes:
    * bronze-raw-ingested-data/        -> raw NDJSON ingest
    * silver-cleaned-stage1-parquet/   -> curated Parquet from Stage 1 (Athena/CTAS)
    * gold-curated-stage2-parquet/     -> curated Parquet from Stage 2 (Python writes Parquet)
    * athena-query-results/            -> Athena query result scratch
    * python-computed-outputs/         -> Python/boto3 outputs (e.g., JSON manifests)
- Separate access-logs bucket
- TLS-only bucket policy, public access blocks, versioning
- Lifecycle rules for short-lived prefixes
- Useful tags and CloudFormation outputs

Adds Glue Data Catalog bits:
- Glue Database for our tables
- Glue Data Catalog encryption at rest (SSE-KMS with our CMK)
- Bronze JSON table with partition projection on snapshot_date

Notes:
- For the assessment, resources use DESTROY/auto_delete_objects for fast teardown.
  For production, switch to RETAIN and remove auto_delete_objects.
"""

from __future__ import annotations

from typing import Final

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    Tags,
    aws_iam as iam,
    aws_kms as kms,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_glue as glue,
)
from constructs import Construct

# ---- Constants (easy to tweak; avoid magic numbers) -------------------------

PROJECT_TAG: Final[str] = "MedLaunch"
KMS_ALIAS: Final[str] = "alias/medlaunch-data-lake"

ATHENA_RESULTS_RETENTION_DAYS: Final[int] = 14
QUARANTINE_RETENTION_DAYS: Final[int] = 60

GLUE_DB_NAME: Final[str] = "medlaunch_db"


class MedlaunchEltStack(Stack):
    """Primary infrastructure for the ELT data lake (S3 + KMS + Glue + policies + lifecycle)."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1) Customer-managed KMS key (CMK) for the data lake
        data_key: kms.Key = kms.Key(
            self,
            "DataLakeKey",
            alias=KMS_ALIAS,
            enable_key_rotation=True,
            # DEV/Assessment: allow clean teardown; PROD: use RETAIN
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Compose globally-unique, readable bucket names (lowercase, hyphenated)
        logs_bucket_name = f"medlaunch-elt-logs-{self.account}-{self.region}"
        data_bucket_name = f"medlaunch-elt-datalake-{self.account}-{self.region}"

        # 2) S3 bucket for server access logs (use S3-managed enc for log target)
        logs_bucket: s3.Bucket = s3.Bucket(
            self,
            "AccessLogsBucket",
            bucket_name=logs_bucket_name,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        _tag(logs_bucket, Purpose="S3AccessLogs")

        # 3) Main data-lake bucket with SSE-KMS, versioning, logging, and lifecycle
        data_bucket: s3.Bucket = s3.Bucket(
            self,
            "DataLakeBucket",
            bucket_name=data_bucket_name,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=data_key,
            versioned=True,
            server_access_logs_bucket=logs_bucket,
            server_access_logs_prefix="s3-access-logs/",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # Enforce TLS-only access (deny any non-HTTPS requests)
        _deny_insecure_transport(data_bucket)

        # Lifecycle: keep short-lived areas tidy/cost-effective
        data_bucket.add_lifecycle_rule(
            id="ExpireAthenaResults",
            prefix="athena-query-results/",
            expiration=Duration.days(ATHENA_RESULTS_RETENTION_DAYS),
        )
        data_bucket.add_lifecycle_rule(
            id="ExpireQuarantine",
            prefix="bronze-raw-ingested-data/quarantine/",
            expiration=Duration.days(QUARANTINE_RETENTION_DAYS),
        )

        # Governance tags
        _tag(
            data_bucket,
            DataClass="Confidential",
            ContainsPHI="No",
            DataSource="Synthetic",
        )

        # Seed prefixes so they’re visible immediately in the console
        s3deploy.BucketDeployment(
            self,
            "SeedPrefixes",
            destination_bucket=data_bucket,
            sources=[
                s3deploy.Source.data("bronze-raw-ingested-data/.keep", "seed"),
                s3deploy.Source.data("silver-cleaned-stage1-parquet/.keep", "seed"),
                s3deploy.Source.data("gold-curated-stage2-parquet/.keep", "seed"),
                s3deploy.Source.data("athena-query-results/.keep", "seed"),
                s3deploy.Source.data("python-computed-outputs/.keep", "seed"),
            ],
        )

        # Save references for later (other stacks/stages)
        self.data_bucket = data_bucket
        self.logs_bucket = logs_bucket
        self.data_key = data_key

        # 4) Glue: Database + Data Catalog encryption (SSE-KMS with our CMK)
        glue_db = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=GLUE_DB_NAME,
                description="Glue database for MedLaunch ELT",
            ),
        )

        # Encrypt Glue Data Catalog metadata at rest using our KMS key
        glue.CfnDataCatalogEncryptionSettings(
            self,
            "GlueCatalogEncryption",
            catalog_id=self.account,
            data_catalog_encryption_settings=glue.CfnDataCatalogEncryptionSettings.DataCatalogEncryptionSettingsProperty(
                encryption_at_rest=glue.CfnDataCatalogEncryptionSettings.EncryptionAtRestProperty(
                    catalog_encryption_mode="SSE-KMS",
                    sse_aws_kms_key_id=self.data_key.key_arn,
                ),
                # No Glue connections defined here
            ),
        )

        # 5) Glue: Bronze JSON table with partition projection on snapshot_date
        bronze_table = glue.CfnTable(
            self,
            "FacilitiesBronzeJson",
            catalog_id=self.account,
            database_name=GLUE_DB_NAME,
            table_input=glue.CfnTable.TableInputProperty(
                name="bronze_facilities_json",
                description="Raw facilities NDJSON in bronze-raw-ingested-data/ (partitioned by snapshot_date)",
                table_type="EXTERNAL_TABLE",
                parameters={
                    "classification": "json",
                    "typeOfData": "file",
                    # Partition projection = no MSCK REPAIR needed
                    "projection.enabled": "true",
                    "projection.snapshot_date.type": "date",
                    "projection.snapshot_date.format": "yyyy-MM-dd",
                    "projection.snapshot_date.range": "2024-01-01,NOW",
                    # Map the partition key to our folder layout:
                    "storage.location.template": f"s3://{self.data_bucket.bucket_name}/bronze-raw-ingested-data/batch/${{snapshot_date}}/",
                },
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name="snapshot_date", type="string"),
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=f"s3://{self.data_bucket.bucket_name}/bronze-raw-ingested-data/batch/",
                    input_format="org.apache.hadoop.mapred.TextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        name="json",
                        serialization_library="org.openx.data.jsonserde.JsonSerDe",
                    ),
                    columns=[
                        glue.CfnTable.ColumnProperty(name="facility_id", type="string"),
                        glue.CfnTable.ColumnProperty(
                            name="facility_name", type="string"
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="location",
                            type="struct<address:string,city:string,state:string,zip:string>",
                        ),
                        glue.CfnTable.ColumnProperty(name="employee_count", type="int"),
                        glue.CfnTable.ColumnProperty(
                            name="services", type="array<string>"
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="labs",
                            type="array<struct<lab_name:string,certifications:array<string>>>",
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="accreditations",
                            type="array<struct<accreditation_body:string,accreditation_id:string,valid_until:string>>",
                        ),
                        # snapshot_date is a PARTITION KEY
                    ],
                ),
            ),
        )
        bronze_table.add_dependency(glue_db)

        # Helpful CloudFormation outputs (easy to fetch via CLI/console)
        CfnOutput(self, "DataLakeBucketName", value=data_bucket.bucket_name)
        CfnOutput(self, "LogsBucketName", value=logs_bucket.bucket_name)
        CfnOutput(self, "KmsKeyArn", value=data_key.key_arn)
        CfnOutput(self, "GlueDatabaseName", value=GLUE_DB_NAME)


# ---- Helpers ----------------------------------------------------------------


def _deny_insecure_transport(bucket: s3.Bucket) -> None:
    """Attach a bucket policy that denies any request not using TLS (HTTPS)."""
    bucket.add_to_resource_policy(
        iam.PolicyStatement(
            sid="DenyInsecureTransport",
            effect=iam.Effect.DENY,
            principals=[iam.AnyPrincipal()],
            actions=["s3:*"],
            resources=[bucket.bucket_arn, f"{bucket.bucket_arn}/*"],
            conditions={"Bool": {"aws:SecureTransport": "false"}},
        )
    )


def _tag(resource: Construct, **tags: str) -> None:
    """Apply common tags consistently."""
    Tags.of(resource).add("Project", PROJECT_TAG)
    for k, v in tags.items():
        Tags.of(resource).add(k, v)
