# medlaunch_elt_stack.py
"""
MedLaunch ELT (CDK) — S3, KMS, Glue, and two Lambdas. No Step Functions.

Builds the data lake stack: KMS key, S3 data + access-log buckets with lifecycle rules,
Glue database + bronze JSON table (projection), and two Lambdas. Wires an S3
ObjectCreated trigger to the Stage 3 Athena runner. Exposes useful outputs

Creates
- KMS CMK (rotation on)
- S3 data lake + access-logs buckets with lifecycle rules
- Glue DB + bronze JSON table (snapshot_date projection)
- Lambda Stage 2: expiring accreditations (sweeps bronze)
- Lambda Stage 3: Athena runner + S3 trigger on bronze *.jsonl

Key S3 prefixes
- bronze-raw-ingested-data/                  raw NDJSON ingest
- stage1-athena-*/                           Stage 1 scratch + Parquet
- stage3-athena-query-results/{Processed Results, Rejected Results, _scratch}/
- stage3-athena-parquet-results/             Stage 3 Parquet outputs
- python-computed-outputs/                   Python/boto3 outputs
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
    aws_logs as logs,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_glue as glue,
)
from constructs import Construct
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_s3_notifications as s3n

# ---- Constants ---------------------------------------------------------------

PROJECT_TAG: Final[str] = "MedLaunch"
KMS_ALIAS: Final[str] = "alias/medlaunch-data-lake"

ATHENA_RESULTS_RETENTION_DAYS: Final[int] = 14
QUARANTINE_RETENTION_DAYS: Final[int] = 60
GLUE_DB_NAME: Final[str] = "medlaunch_db"

# ---- Stack -------------------------------------------------------------------


class MedlaunchEltStack(Stack):
    """Primary infra for the ELT data lake (S3 + KMS + Glue + Lambdas)."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1) KMS CMK (DEV: destroy on stack delete; use RETAIN in prod)
        data_key = kms.Key(
            self,
            "DataLakeKey",
            alias=KMS_ALIAS,
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Globally-unique bucket names
        logs_bucket_name = f"medlaunch-elt-logs-{self.account}-{self.region}"
        data_bucket_name = f"medlaunch-elt-datalake-{self.account}-{self.region}"

        # 2) Access-logs bucket (no public access)
        logs_bucket = s3.Bucket(
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

        # 3) Data lake bucket (KMS-encrypted, versioned, server logs enabled)
        data_bucket = s3.Bucket(
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
        _deny_insecure_transport(data_bucket)  # enforce HTTPS

        # Lifecycle: clean short-lived areas
        data_bucket.add_lifecycle_rule(
            id="ExpireStage1AthenaResults",
            prefix="stage1-athena-query-results/",
            expiration=Duration.days(ATHENA_RESULTS_RETENTION_DAYS),
        )
        data_bucket.add_lifecycle_rule(
            id="ExpireStage3ProcessedCsv",
            prefix="stage3-athena-query-results/Processed Results/",
            expiration=Duration.days(ATHENA_RESULTS_RETENTION_DAYS),
        )
        data_bucket.add_lifecycle_rule(
            id="ExpireStage3RejectedCsv",
            prefix="stage3-athena-query-results/Rejected Results/",
            expiration=Duration.days(ATHENA_RESULTS_RETENTION_DAYS),
        )
        data_bucket.add_lifecycle_rule(
            id="ExpireQuarantine",
            prefix="python-computed-outputs/quarantine/",
            expiration=Duration.days(QUARANTINE_RETENTION_DAYS),
        )

        # Tag data bucket for quick discovery/compliance filters
        _tag(
            data_bucket,
            DataClass="Confidential",
            ContainsPHI="No",
            DataSource="Synthetic",
        )

        # Seed console-visible prefixes with .keep files
        s3deploy.BucketDeployment(
            self,
            "SeedPrefixes",
            destination_bucket=data_bucket,
            sources=[
                s3deploy.Source.data("bronze-raw-ingested-data/.keep", "seed"),
                s3deploy.Source.data("stage1-athena-query-results/.keep", "seed"),
                s3deploy.Source.data("stage1-athena-parquet-results/.keep", "seed"),
                s3deploy.Source.data(
                    "stage3-athena-query-results/Processed Results/.keep", "seed"
                ),
                s3deploy.Source.data(
                    "stage3-athena-query-results/Rejected Results/.keep", "seed"
                ),
                s3deploy.Source.data(
                    "stage3-athena-query-results/_scratch/.keep", "seed"
                ),
                s3deploy.Source.data("stage3-athena-parquet-results/.keep", "seed"),
                s3deploy.Source.data("python-computed-outputs/.keep", "seed"),
            ],
        )

        # Save refs for other constructs/outputs
        self.data_bucket = data_bucket
        self.logs_bucket = logs_bucket
        self.data_key = data_key

        # 4) Glue: DB + catalog encryption
        glue_db = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=GLUE_DB_NAME,
                description="Glue database for MedLaunch ELT",
            ),
        )
        glue.CfnDataCatalogEncryptionSettings(
            self,
            "GlueCatalogEncryption",
            catalog_id=self.account,
            data_catalog_encryption_settings=glue.CfnDataCatalogEncryptionSettings.DataCatalogEncryptionSettingsProperty(
                encryption_at_rest=glue.CfnDataCatalogEncryptionSettings.EncryptionAtRestProperty(
                    catalog_encryption_mode="SSE-KMS",
                    sse_aws_kms_key_id=self.data_key.key_arn,
                ),
            ),
        )

        # 5) Glue: bronze JSON table (snapshot_date projection)
        bronze_table = glue.CfnTable(
            self,
            "FacilitiesBronzeJson",
            catalog_id=self.account,
            database_name=GLUE_DB_NAME,
            table_input=glue.CfnTable.TableInputProperty(
                name="bronze_facilities_json",
                description="Raw facilities NDJSON (partitioned by snapshot_date)",
                table_type="EXTERNAL_TABLE",
                parameters={
                    "classification": "json",
                    "typeOfData": "file",
                    "projection.enabled": "true",
                    "projection.snapshot_date.type": "date",
                    "projection.snapshot_date.format": "yyyy-MM-dd",
                    "projection.snapshot_date.range": "2024-01-01,NOW",
                    "storage.location.template": f"s3://{self.data_bucket.bucket_name}/bronze-raw-ingested-data/batch/${{snapshot_date}}/",
                },
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name="snapshot_date", type="string")
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
                    ],
                ),
            ),
        )
        bronze_table.add_dependency(glue_db)

        # 6) Lambda — Stage 2 (expiring accreditations)
        stage2_fn = _lambda.Function(
            self,
            "Stage2ExpiringLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="handler.handler",
            code=_lambda.Code.from_asset(
                "lambda/stage2_expiring",
                exclude=["**/__pycache__/**", "**/*.pyc", "requirements.txt"],
            ),
            architecture=_lambda.Architecture.X86_64,
            timeout=Duration.minutes(2),
            memory_size=512,
            environment={"DATA_BUCKET": data_bucket.bucket_name, "WINDOW_DAYS": "180"},
            log_retention=logs.RetentionDays.ONE_WEEK,
        )
        # Least-privilege grants
        data_bucket.grant_read(stage2_fn, "bronze-raw-ingested-data/batch/*")
        data_bucket.grant_write(stage2_fn, "python-computed-outputs/expiring-soon/*")
        data_bucket.grant_write(stage2_fn, "python-computed-outputs/quarantine/*")
        data_bucket.grant_write(stage2_fn, "gold-curated-stage2-parquet/*")
        data_key.grant_encrypt_decrypt(stage2_fn)

        # 7) Lambda — Stage 3 (Athena runner) with S3 trigger
        stage3_fn = _lambda.Function(
            self,
            "AthenaS3TriggerFn",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="handler.handler",
            code=_lambda.Code.from_asset(
                "lambda/athena_s3trigger",
                exclude=["**/__pycache__/**", "**/*.pyc"],
            ),
            timeout=Duration.minutes(10),
            memory_size=512,
            architecture=_lambda.Architecture.X86_64,
            environment={
                "DATA_BUCKET": data_bucket.bucket_name,
                "OUTPUT_SCRATCH": f"s3://{data_bucket.bucket_name}/stage3-athena-query-results/_scratch/",
                "KMS_KEY_ARN": data_key.key_arn,
                "ATHENA_DB": GLUE_DB_NAME,
                "ATHENA_WORKGROUP": "primary",
                "ATHENA_TIMEOUT_SECONDS": "540",
                "POLL_INTERVAL_SECONDS": "2.5",
                # Optional: "BRONZE_TABLE": "bronze_facilities_json_np",
            },
            log_retention=logs.RetentionDays.ONE_WEEK,
        )

        # S3 → Lambda (only bronze *.jsonl)
        data_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(stage3_fn),
            s3.NotificationKeyFilter(
                prefix="bronze-raw-ingested-data/batch/", suffix=".jsonl"
            ),
        )

        # Stage-3 IAM: S3 list/read/write + KMS + Athena + Glue read
        stage3_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:ListBucket", "s3:GetBucketLocation"],
                resources=[data_bucket.bucket_arn],
            )
        )
        data_bucket.grant_read(stage3_fn, "bronze-raw-ingested-data/batch/*")
        data_bucket.grant_read_write(stage3_fn, "stage3-athena-query-results/*")
        data_bucket.grant_read_write(stage3_fn, "stage3-athena-parquet-results/*")
        data_key.grant_encrypt_decrypt(stage3_fn)
        stage3_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:StopQueryExecution",
                    "athena:GetQueryResults",
                ],
                resources=["*"],
            )
        )
        glue_catalog_arn = f"arn:aws:glue:{self.region}:{self.account}:catalog"
        glue_db_arn = (
            f"arn:aws:glue:{self.region}:{self.account}:database/{GLUE_DB_NAME}"
        )
        glue_table_arn = (
            f"arn:aws:glue:{self.region}:{self.account}:table/{GLUE_DB_NAME}/*"
        )
        stage3_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetTableVersion",
                    "glue:GetTableVersions",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:BatchGetPartition",
                    "glue:GetDataCatalogEncryptionSettings",
                ],
                resources=[glue_catalog_arn, glue_db_arn, glue_table_arn],
            )
        )

        # Stack outputs for quick copy/paste
        CfnOutput(self, "DataLakeBucketName", value=data_bucket.bucket_name)
        CfnOutput(self, "LogsBucketName", value=logs_bucket.bucket_name)
        CfnOutput(self, "KmsKeyArn", value=data_key.key_arn)
        CfnOutput(self, "GlueDatabaseName", value=GLUE_DB_NAME)
        CfnOutput(self, "Stage2LambdaName", value=stage2_fn.function_name)
        CfnOutput(self, "AthenaS3TriggerFnName", value=stage3_fn.function_name)


# ---- Helpers ----------------------------------------------------------------


def _deny_insecure_transport(bucket: s3.Bucket) -> None:
    """Bucket policy: deny any non-TLS (HTTP) access."""
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
    """Apply project tag plus any provided key/values."""
    Tags.of(resource).add("Project", PROJECT_TAG)
    for k, v in tags.items():
        Tags.of(resource).add(k, v)
