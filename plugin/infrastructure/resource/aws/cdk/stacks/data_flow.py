from aws_cdk import core as cdk
import aws_cdk.aws_glue as glue
import aws_cdk.aws_kinesis as kinesis
import aws_cdk.aws_kinesisfirehose as kinesisfirehose
import aws_cdk.aws_iam as iam
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_kms as kms

from plugin.domain.manifest import DataPipeline, Table, TableTrigger
from plugin.utils.cdk import fields_to_columns
from plugin.infrastructure.resource.aws.cdk.stacks.helpers.file import get_policy, get_role
from plugin.infrastructure.resource.aws.cdk.stacks.helpers.get_resource import get_bucket_raw, get_firehose, get_kinesis_stream
from plugin.utils.file import interpolate_json_template
from plugin.utils.string import kebab, snake_case


class DataFlow(cdk.Stack):
    """
    TODO
    """

    def create_database(self, name: str):
        database = glue.Database(self, f"database-{name}", database_name=name)
        return database

    def create_registry(self, name: str):
        glue.CfnRegistry(
            self,
            f"registry-{name}",
            name=name,
            description="",
            tags=[
                cdk.CfnTag(
                    key="database",
                    value=name)
            ]
        )

    def create_schema(self, name: str, table: Table):
        cfn_schema_registry = glue.CfnSchema(
            self,
            f"schema-{table.name}",
            compatibility="BACKWARD",
            data_format="AVRO",
            name=table.name,
            schema_definition=table.schema.definition_to_json,
            description=table.description,
            registry=glue.CfnSchema.RegistryProperty(
                name=name
            )
        )

        return cfn_schema_registry

    def create_kinesis(self, name: str, schema_name: str):
        return kinesis.CfnStream(
            self,
            name,
            shard_count=1,
            name=name,
            retention_period_hours=24,
            tags=[cdk.CfnTag(
                key="schema",
                value=schema_name
            )]
        )

    def create_role_job_glue(self, data_pipeline: DataPipeline, schema_name: str):
        k_database_name = kebab(data_pipeline.database.name)
        k_schema_name = kebab(schema_name)
        bucket_raw = s3.Bucket.from_bucket_arn(
            self, "import-bucket-raw", data_pipeline.arn_bucket_source)
        bucket_clean = s3.Bucket.from_bucket_arn(
            self, "import-bucket-clean", data_pipeline.arn_bucket_target)
        role = get_role("OsDataPolicyRoleGlueJobs")
        policy_json = interpolate_json_template(
            get_policy("OsDataPolicyGlueJobs"),
            {
                "DatabaseName": snake_case(k_database_name),
                "TableName": schema_name,
                "AwsRegion": self.region,
                "AwsAccount": self.account,
                "BucketRaw": bucket_raw.bucket_name,
                "BucketClean": bucket_clean.bucket_name,
                "BucketPath": bucket_raw.bucket_name,
                "BucketEventsLogs": f"{self.account}-{k_database_name}-data-schema-logs",
                "BucketEventsStaging": f"{self.account}-{k_database_name}-data-schema-assets"
            }
        )
        policy_document = iam.PolicyDocument.from_json(policy_json)
        return iam.CfnRole(
            self,
            f"{k_database_name}-{k_schema_name}-job-glue",
            role_name=f"{k_database_name}-{k_schema_name}-role-pipelines",
            assume_role_policy_document=role,
            policies=[iam.CfnRole.PolicyProperty(
                policy_document=policy_document,
                policy_name=f"{k_database_name}-{k_schema_name}-policy-pipelines"
            )]
        )

    def create_firehose_role_policy(self, database_name: str, schema_name: str):
        role_name = f"{database_name}-{schema_name}-role-firehose"
        policy_name = f"{database_name}-{schema_name}-firehose-integration"
        role = iam.Role(
            self,
            f"role-{role_name}",
            assumed_by=iam.ServicePrincipal(
                "firehose.amazonaws.com"),
            role_name=role_name
        )
        bucket = get_bucket_raw(cdk, self, "import-bucket-rw", database_name)
        kinesis_stream = get_kinesis_stream(
            cdk, self, "import-kinesis", database_name)
        firehose_ds = get_firehose(
            self, "import-firehose", database_name, schema_name)
        policy_json = interpolate_json_template(
            get_policy("OsDataPolicyFirehoseStream"),
            {
                "AwsRegion": cdk.Stack.of(self).region,
                "AwsAccount": cdk.Stack.of(self).account,
                "BucketRaw": bucket.bucket_name,
                "KinesisName": kinesis_stream.stream_name,
                "FirehoseName": firehose_ds.delivery_stream_name
            }
        )
        policy_document = iam.PolicyDocument.from_json(policy_json)
        policy = iam.CfnPolicy(
            self,
            f"policy-{policy_name}",
            policy_name=policy_name,
            policy_document=policy_document,
            roles=[role.role_name]
        )
        return policy

    def create_kinesis_delivery_stream(self, database_name: str, arn_bucket_source: str, schema_name: str, kms = {}):
        name = kebab(f'{database_name}-{schema_name}')
        stream_name = kebab(f"{database_name}-{schema_name}")
        firehose = kinesisfirehose.CfnDeliveryStream(
                self,
                f"kinesis-delivery-stream-{name}",
                delivery_stream_name=f"{database_name}-{schema_name}-delivery",
                kinesis_stream_source_configuration=kinesisfirehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(
                    kinesis_stream_arn=f'arn:aws:kinesis:{cdk.Stack.of(self).region}:{cdk.Stack.of(self).account}:stream/{stream_name}-kinesis',
                    role_arn=f'arn:aws:iam::{cdk.Stack.of(self).account}:role/{name}-role-firehose',
                    ),
                extended_s3_destination_configuration=kinesisfirehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                    bucket_arn=arn_bucket_source,
                    role_arn=f'arn:aws:iam::{cdk.Stack.of(self).account}:role/{name}-role-firehose',
                    buffering_hints=kinesisfirehose.CfnDeliveryStream.BufferingHintsProperty(
                        interval_in_seconds=128,
                        size_in_m_bs=64
                        ),
                    compression_format='UNCOMPRESSED',
                    cloud_watch_logging_options=kinesisfirehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                        enabled=True,
                        log_group_name=f'/aws/kinesisfirehose/{name}-delivery',
                        log_stream_name='DestinationDelivery'
                        ),
                    encryption_configuration=kinesisfirehose.CfnDeliveryStream.EncryptionConfigurationProperty(
                        kms_encryption_config=kinesisfirehose.CfnDeliveryStream.KMSEncryptionConfigProperty(
                            awskms_key_arn=kms.attr_arn
                            ),
                        ),
                    error_output_prefix='error/',
                    prefix='!{partitionKeyFromQuery:dataproduct}/!{partitionKeyFromQuery:schema}/!{partitionKeyFromQuery:version}/!{partitionKeyFromQuery:year}/!{partitionKeyFromQuery:month}/!{partitionKeyFromQuery:day}/',  # pylint: disable=line-too-long
                    processing_configuration=kinesisfirehose.CfnDeliveryStream.ProcessingConfigurationProperty(
                        enabled=True,
                        processors=[
                            kinesisfirehose.CfnDeliveryStream.ProcessorProperty(
                                type='RecordDeAggregation',
                                parameters=[
                                    kinesisfirehose.CfnDeliveryStream.ProcessorParameterProperty(
                                        parameter_name='SubRecordType',
                                        parameter_value='JSON'
                                )
                            ]
                        ),
                        kinesisfirehose.CfnDeliveryStream.ProcessorProperty(
                            type='MetadataExtraction',
                            parameters=[
                                kinesisfirehose.CfnDeliveryStream.ProcessorParameterProperty(
                                    parameter_name='MetadataExtractionQuery',
                                    parameter_value='{schema:.schema_name,version:.schema_version,year:.event_time | (. / 1000 | strftime("%Y")),month:.event_time | (. / 1000 | strftime("%m")),day:.event_time | (. / 1000 | strftime("%d")),dataproduct:.data_product}' # pylint: disable=line-too-long
                                ),
                                kinesisfirehose.CfnDeliveryStream.ProcessorParameterProperty(
                                    parameter_name='JsonParsingEngine',
                                    parameter_value='JQ-1.6'
                                )
                            ]
                        ),
                        kinesisfirehose.CfnDeliveryStream.ProcessorProperty(
                            type='AppendDelimiterToRecord',
                            parameters=[
                                kinesisfirehose.CfnDeliveryStream.ProcessorParameterProperty(
                                    parameter_name='Delimiter',
                                    parameter_value='\\n'
                                )
                            ]
                        )
                    ]
                ),
                data_format_conversion_configuration=kinesisfirehose.CfnDeliveryStream.DataFormatConversionConfigurationProperty(
                    enabled=False),
                dynamic_partitioning_configuration=kinesisfirehose.CfnDeliveryStream.DynamicPartitioningConfigurationProperty(
                    enabled=True,
                    retry_options=kinesisfirehose.CfnDeliveryStream.RetryOptionsProperty(
                        duration_in_seconds=60
                    )
                ),
                s3_backup_mode='Disabled'
            ))
        firehose.add_property_override(
            "DeliveryStreamType", "KinesisStreamAsSource")
        return firehose

    def create_database_table(self, data: DataPipeline, table: Table):
        database_name = snake_case(data.database.name)
        bucket = s3.Bucket.from_bucket_arn(
            self, "import-bucket-target", data.arn_bucket_target)
        columns = fields_to_columns(table.fields)

        return glue.CfnTable(
            self,
            kebab(f"table-{database_name}-{table.name}"),
            catalog_id=self.account,
            database_name=database_name,
            table_input=glue.CfnTable.TableInputProperty(
                name=snake_case(table.name),
                description=table.description,
                parameters={
                    "EXTERNAL": "TRUE",
                    "parquet.compression": "SNAPPY",
                    "classification": "parquet"
                },
                partition_keys=[
                    glue.CfnTable.ColumnProperty(
                        name="_version",
                        type="int"
                    ),
                    glue.CfnTable.ColumnProperty(
                        name="_date",
                        type="date"
                    )
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=columns,
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    location=f"s3://{bucket.bucket_name}/{snake_case(database_name)}/{snake_case(table.name)}/",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        name=kebab(f"{table.name}-stream"),
                        parameters={
                            "serialization.format": 1
                        },
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    )
                ),
                table_type="EXTERNAL_TABLE"
            )
        )

    def create_glue_jobs(self, bucket_source: str, bucket_target: str, database_name: str, schema_name: str):
        return glue.CfnJob(
            self,
            f"glue-job-{database_name}-{schema_name}-clean",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{cdk.Stack.of(self).account}-{database_name}-data-schema-assets/datacleansing.py"
            ),
            role=f"arn:aws:iam::{cdk.Stack.of(self).account}:role/{database_name}-{schema_name}-role-pipelines",
            default_arguments={
                "--job-language": "python",
                "--enable-spark-ui": "true",
                "--additional-python-modules": "boto3>=1.19,avro==1.11.0",
                "--conf": "spark.sql.sources.partitionOverwriteMode=dynamic",
                "--enable-glue-datacatalog": "",
                "--enable-metrics": "",
                "--job-bookmark-option": "job-bookmark-disable",
                "--spark-event-logs-path": f"s3://{cdk.Stack.of(self).account}-{database_name}-data-schema-logs",
                "--enable-continuous-cloudwatch-log": "true",
                "--aws_region": cdk.Stack.of(self).region,
                "--database": snake_case(database_name),
                "--name": f"{database_name}-{schema_name}-clean",
                "--packages": "org.apache.spark:spark-avro_2.12:3.1.1",
                "--region": cdk.Stack.of(self).region,
                "--py-files": f"s3://{cdk.Stack.of(self).account}-{database_name}-data-schema-assets/pipelines.zip",
                "--extra-py-files": f"s3://{cdk.Stack.of(self).account}-{database_name}-data-schema-assets/pipelines.zip",
                "--extra-jars": f"s3://{cdk.Stack.of(self).account}-{database_name}-data-schema-assets/spark-avro_2.12-3.1.1.jar",
                "--bucket_source": bucket_source,
                "--path_staging": f"s3://{cdk.Stack.of(self).account}-{database_name}-data-schema-assets/{snake_case(database_name)}/{snake_case(schema_name)}",
                "--bucket_staging": f"s3://{cdk.Stack.of(self).account}-{database_name}-data-schema-assets/tmp",
                "--bucket_target": bucket_target,
                "--schema": snake_case(schema_name)
            },
            description="an example Python Shell job",
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            glue_version="3.0",
            max_retries=1,
            name=f"{database_name}-{schema_name}-clean",
            number_of_workers=10,
            timeout=2880,
            worker_type="Standard"
        )

    def create_glue_trigger(self, database_name: str, schema_name: str, trigger: TableTrigger):
        if trigger == 'ON_DEMAND':
            return glue.CfnTrigger(
                self,
                f"glue-trigger-{database_name}-{schema_name}-clean",
                actions=[glue.CfnTrigger.ActionProperty(
                    job_name=f"{database_name}-{schema_name}-clean"
                )],
                type=trigger.type,
                description=f"trigger {trigger.type}",
                name=f"{database_name}-{schema_name}-clean-trigger"
            )

        return glue.CfnTrigger(
            self,
            f"glue-trigger-{database_name}-{schema_name}-clean",
            actions=[glue.CfnTrigger.ActionProperty(
                job_name=f"{database_name}-{schema_name}-clean"
            )],
            type=trigger.type,
            schedule=trigger.cron,
            description=f"trigger {trigger.type}",
            name=f"{database_name}-{schema_name}-clean-trigger"
        )


    def create_bucket_assets(self, name: str, kms={}):
        bucket = s3.CfnBucket(
            self,
            id=f"dp-{name}",
            bucket_name=name,
            bucket_encryption=s3.CfnBucket.BucketEncryptionProperty(
                server_side_encryption_configuration=[
                    s3.CfnBucket.ServerSideEncryptionRuleProperty(
                        server_side_encryption_by_default=s3.CfnBucket.
                        ServerSideEncryptionByDefaultProperty(
                            sse_algorithm="aws:kms",
                            kms_master_key_id=cdk.Fn.ref(kms.logical_id)
                        )
                    )
                ]
            ),
        )
        bucket.apply_removal_policy(
            default=cdk.RemovalPolicy.DESTROY
        )


    def create_kms(self, name: str, user_arn: str):
        policy_json = interpolate_json_template(
                get_policy("OsPolicyKMS"),
                {
                    "AwsAccount": cdk.Stack.of(self).account,
                    "UserArn": user_arn
                    }
                )
        return kms.CfnKey(self, f"{name}", 
                key_policy=policy_json,
                enabled=True,
                key_spec="SYMMETRIC_DEFAULT",
                multi_region=False)

