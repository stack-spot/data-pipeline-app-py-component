import string
import random
import pytest
from aws_cdk import (core, assertions)
from unittest import (mock)
from plugin.domain.manifest import Manifest, Table, Schema, Field
from plugin.infrastructure.resource.aws.cdk.stacks.data_flow import DataFlow


class TestCdkDataFlow:

    @pytest.fixture(autouse=True)
    def cdk_app(self):
        self.app = core.App()
        self.stack_name = f"create-datapipeline-stack-{self.__random_string(letter=string.ascii_letters,size=10)}"
        self.stack = DataFlow(self.app, self.stack_name)

    @staticmethod
    def __random_string(letter, size: int):
        return ''.join(random.choice(letter) for _ in range(size))

    @pytest.fixture(autouse=True)
    def plugin_manifest(self):
        self.manifest = Manifest({
            "data-pipeline": {
                "region": "us-east-1",
                "arn_bucket_source": "arn:aws:s3:::xxxxxx-raw-flowers",
                "arn_bucket_target": "arn:aws:s3:::xxxxxx-clean-flowers",
                "database": {
                    "name": self.__random_string(letter=string.ascii_letters.lower(), size=18),
                    "schemas": {
                        "path": "./template",
                        "tables": []
                    }
                }
            }
        })

        self.table = Table(
            name=self.__random_string(
                letter=string.ascii_letters.lower(), size=18),
            description="doc",
            schema=Schema(
                definition={'type': 'record', 'name': 'foo_bar', 'doc': 'Foo bar', 'fields': [
                    {'name': 'foo', 'type': ['null', 'string'], 'doc': 'bar', 'default': None}]}
            ),
            fields=[
                Field(
                    name="foo",
                    type="string",
                    description="bar"
                )
            ]
        )

    def test_cdk_database(self):
        name = self.__random_string(
            letter=string.ascii_letters.lower(), size=18)
        self.stack.create_database(name)
        stack_template = self.app.synth().get_stack_artifact(self.stack_name).template
        for key, _ in stack_template["Resources"].items():
            logical_id = key
        template_base = {
            "Resources": {
                f"{logical_id}": {
                    "Type": "AWS::Glue::Database",
                    "Properties": {
                        "CatalogId": {
                            "Ref": "AWS::AccountId"
                        },
                        "DatabaseInput": {
                            "Name": f"{name}"
                        }
                    }
                }
            }
        }
        template = assertions.Template.from_stack(self.stack)
        template.template_matches(template_base)

    def test_cdk_schema_registry(self):
        name = self.__random_string(
            letter=string.ascii_letters.lower(), size=8)
        schema = self.stack.create_schema(name, self.table)
        logical_id = self.stack.get_logical_id(schema)
        template_base = {
            "Resources": {
                f"{logical_id}": {
                    "Type": "AWS::Glue::Schema",
                    "Properties": {
                        "Compatibility": "BACKWARD",
                        "DataFormat": "AVRO",
                        "Name": self.table.name,
                        "SchemaDefinition": self.table.schema.definition_to_json,
                        "Description": self.table.description,
                        "Registry": {
                            "Name": f"{name}"
                        }
                    }
                }
            }
        }
        template = assertions.Template.from_stack(self.stack)
        template.template_matches(template_base)

    @mock.patch("plugin.infrastructure.resource.aws.cdk.stacks.data_flow.interpolate_json_template", return_value={"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["glue:GetTable"], "Resource": "*"}]})
    @mock.patch("plugin.infrastructure.resource.aws.cdk.stacks.data_flow.get_role", return_value={"Version": "2012-10-17", "Statement": [{"Sid": "", "Effect": "Allow", "Principal": {"Service": ["glue.amazonaws.com"]}, "Action": "sts:AssumeRole"}]})
    def test_cdk_role_job_glue(self, mock_role, mock_policy):
        name = self.__random_string(
            letter=string.ascii_letters.lower(), size=18)
        glue = self.stack.create_role_job_glue(
            self.manifest.data_pipeline, name)
        logical_id = self.stack.get_logical_id(glue)
        template_base = {
            "Resources": {
                f"{logical_id}": {
                    "Type": "AWS::IAM::Role",
                    "Properties": {
                        "AssumeRolePolicyDocument": {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Sid": "",
                                    "Effect": "Allow",
                                    "Principal": {
                                        "Service": [
                                            "glue.amazonaws.com"
                                        ]
                                    },
                                    "Action": "sts:AssumeRole"
                                }
                            ]
                        },
                        "Policies": [
                            {
                                "PolicyDocument": {
                                    "Statement": [
                                        {
                                            "Action": "glue:GetTable",
                                            "Effect": "Allow",
                                            "Resource": "*"
                                        }
                                    ],
                                    "Version": "2012-10-17"
                                },
                                "PolicyName": f"{self.manifest.data_pipeline.database.name}-{name}-policy-pipelines"
                            }
                        ],
                        "RoleName": f"{self.manifest.data_pipeline.database.name}-{name}-role-pipelines"
                    }
                }
            }
        }
        template = assertions.Template.from_stack(self.stack)
        template.template_matches(template_base)

    @mock.patch("plugin.infrastructure.resource.aws.cdk.stacks.data_flow.interpolate_json_template", return_value={"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["firehose:*"], "Resource": "*"}]})
    def test_cdk_firehose_role_policy(self, mock_policy):
        name = self.__random_string(
            letter=string.ascii_letters.lower(), size=18)
        self.stack.create_firehose_role_policy(
            self.manifest.data_pipeline.database.name, name)
        template = assertions.Template.from_stack(self.stack)
        template.has_resource("AWS::IAM::Role", {
            "Properties": {"RoleName": f"{self.manifest.data_pipeline.database.name}-{name}-role-firehose"}
        })

    def test_cdk_kinesis_delivery_stream(self):
        name = self.__random_string(
            letter=string.ascii_letters.lower(), size=18)
        kms = self.stack.create_kms("test", "arn::user")
        self.stack.create_kinesis_delivery_stream(
                self.manifest.data_pipeline.database.name, self.manifest.data_pipeline.arn_bucket_source, name, kms=kms)
        template = assertions.Template.from_stack(self.stack)
        template.has_resource("AWS::KinesisFirehose::DeliveryStream", {
            "Properties": {"DeliveryStreamName": f"{self.manifest.data_pipeline.database.name}-{name}-delivery",
                           "ExtendedS3DestinationConfiguration": {"Prefix": "!{partitionKeyFromQuery:dataproduct}/!{partitionKeyFromQuery:schema}/!{partitionKeyFromQuery:version}/!{partitionKeyFromQuery:year}/!{partitionKeyFromQuery:month}/!{partitionKeyFromQuery:day}/"}}
        })

    def test_cdk_create_database_table(self):
        self.stack.create_database_table(
            self.manifest.data_pipeline, self.table)
        template = assertions.Template.from_stack(self.stack)
        template.has_resource("AWS::Glue::Table", {
            "Properties": {
                "DatabaseName": self.manifest.data_pipeline.database.name,
                "TableInput": {
                    "Description": self.table.description,
                    "Name": self.table.name,
                    "Parameters": {
                        "EXTERNAL": "TRUE",
                        "parquet.compression": "SNAPPY",
                        "classification": "parquet"
                    },
                    "PartitionKeys": [
                        {
                            "Name": "_version",
                            "Type": "int"
                        },
                        {
                            "Name": "_date",
                            "Type": "date"
                        }
                    ],
                    "StorageDescriptor": {
                        "Columns": [
                            {
                                "Name": "event_time",
                                "Type": "timestamp"
                            },
                            {
                                "Name": "event_id",
                                "Type": "int"
                            },
                            {
                                "Name": "foo",
                                "Type": "string"
                            }
                        ],
                        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "Location": f"s3://xxxxxx-clean-flowers/{self.manifest.data_pipeline.database.name}/{self.table.name}/",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        "SerdeInfo": {
                            "Name": f"{self.table.name}-stream",
                            "Parameters": {
                                "serialization.format": 1
                            },
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                        }
                    },
                    "TableType": "EXTERNAL_TABLE"
                }
            }
        })
