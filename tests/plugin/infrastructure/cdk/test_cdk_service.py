import string
import json
import random
import pytest
from aws_cdk import (core, assertions)
from unittest import (mock, TestCase)
from plugin.domain.manifest import Manifest, Table
from plugin.infrastructure.resource.aws.cdk.engine.main import CDKEngine
from plugin.infrastructure.resource.aws.cdk import services


class TestCdkServices(TestCase, CDKEngine):

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

    
    @mock.patch("plugin.infrastructure.resource.aws.cdk.stacks.data_flow.read_avro_schema", return_value={
        "type": "record",
        "name": "schema_mock",
        "doc": "mock desc",
        "fields": []
    })
    @mock.patch("plugin.infrastructure.resource.aws.cdk.stacks.data_flow.get_schema_definition", return_value=json.dumps({
        "type": "record",
        "name": "schema_mock",
        "doc": "mock desc",
        "fields": []
    }))
    @mock.patch("plugin.infrastructure.resource.aws.cdk.engine.helpers.is_created_resource", autospec=True)
    @mock.patch("plugin.infrastructure.resource.aws.cdk.engine.helpers.boto3", autospec=True)
    def test_cdk_deploy_dataschema_pipeline(self, mock_boto3, mock_created_resource, mock_get_schema, mock_read):
        mock_created_resource.side_effect = [False, True]
        services.create_dataschema_pipeline(self, self.manifest_taxo, self.manifest.data_pipeline)
