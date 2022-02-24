import string
import json
import random
import pytest
from aws_cdk import (core, assertions)
from unittest import (mock)
from plugin.domain.manifest import Manifest, Table
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
        self.manifest_taxo = Table(name=self.__random_string(
            letter=string.ascii_letters.lower(), size=18),  manipulated="false", fields=[])

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

    @mock.patch("plugin.infrastructure.resource.aws.cdk.stacks.data_flow.get_schema_definition", return_value=json.dumps({
        "type": "record",
        "name": "schema_mock",
        "doc": "mock desc",
        "fields": []
    }))
    def test_cdk_schema_registry(self, mock_schema):
        name = self.__random_string(
            letter=string.ascii_letters.lower(), size=18)
        schema = self.stack.create_schema_registry(name, "_", f"table_{name}")
        logical_id = self.stack.get_logical_id(schema)
        template_base = {
            "Resources": {
                f"{logical_id}": {
                    "Type": "AWS::Glue::Schema",
                    "Properties": {
                        "Compatibility": "BACKWARD",
                        "DataFormat": "AVRO",
                        "Name": f"table_{name}",
                        "SchemaDefinition": "{\"type\": \"record\", \"name\": \"schema_mock\", \"doc\": \"mock desc\", \"fields\": []}",
                        "Description": "",
                        "Registry": {
                            "Name": f"{name}"
                        }
                    }
                }
            }
        }
        template = assertions.Template.from_stack(self.stack)
        template.template_matches(template_base)

    @mock.patch("plugin.infrastructure.resource.aws.cdk.stacks.data_flow.read_avro_schema", return_value={
        "type": "record",
        "name": "schema_mock",
        "doc": "mock desc",
        "fields": []
    })
    def test_cdk_schema_registry_table(self, mock_schema):
        name = self.__random_string(
            letter=string.ascii_letters.lower(), size=18)
        table = self.__random_string(
            letter=string.ascii_letters.lower(), size=18)
        schema = self.stack.create_schema_registry_table(
            name, "_", table)
        logical_id = self.stack.get_logical_id(schema)
        template_base = {
            "Resources": {
                f"{logical_id}": {
                    "Type": "AWS::Glue::Schema",
                    "Properties": {
                        "Compatibility": "BACKWARD",
                        "DataFormat": "AVRO",
                        "Name": f"{table}_table",
                        "SchemaDefinition": "{\"type\": \"record\", \"name\": \"schema_mock\", \"doc\": \"mock desc\", \"fields\": [{\"name\": \"event_time\", \"type\": [\"null\", {\"type\": \"int\", \"logicalType\": \"date\"}]}, {\"name\": \"event_id\", \"type\": \"int\"}]}",
                        "Description": "",
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
        self.stack.create_kinesis_delivery_stream(
            self.manifest.data_pipeline.database.name, self.manifest.data_pipeline.arn_bucket_source, name)
        template = assertions.Template.from_stack(self.stack)
        template.has_resource("AWS::KinesisFirehose::DeliveryStream", {
            "Properties": {"DeliveryStreamName": f"{self.manifest.data_pipeline.database.name}-{name}-delivery",
                           "ExtendedS3DestinationConfiguration": {"Prefix": "!{partitionKeyFromQuery:dataproduct}/!{partitionKeyFromQuery:schema}/!{partitionKeyFromQuery:version}/!{partitionKeyFromQuery:year}/!{partitionKeyFromQuery:month}/!{partitionKeyFromQuery:day}/"}}
        })

    def test_cdk_create_database_table(self):
        self.stack.create_database_table(
            self.manifest.data_pipeline, self.manifest_taxo)
        template = assertions.Template.from_stack(self.stack)
        template.has_resource("AWS::Glue::Table", {
            "Properties": {"DatabaseName": self.manifest.data_pipeline.database.name,
                           "TableInput": {
                               "Name": self.manifest_taxo.name,
                               "StorageDescriptor": {
                                   "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                                   "Location": f"s3://xxxxxx-clean-flowers/{self.manifest.data_pipeline.database.name}/{self.manifest_taxo.name}/",
                                   "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                                   "SchemaReference": {
                                       "SchemaId": {
                                           "RegistryName": self.manifest.data_pipeline.database.name,
                                           "SchemaName": f"{self.manifest_taxo.name}_table"
                                       },
                                       "SchemaVersionNumber": 1
                                   }}}}})
