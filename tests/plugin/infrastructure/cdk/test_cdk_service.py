import string
import random
import pytest
from unittest import (mock, TestCase)
from plugin.domain.manifest import Manifest, Table, Field, Schema
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

    @mock.patch("plugin.infrastructure.resource.aws.cdk.engine.helpers.is_created_resource", autospec=True)
    @mock.patch("plugin.infrastructure.resource.aws.cdk.engine.helpers.boto3", autospec=True)
    def test_cdk_deploy_dataschema_pipeline(self, mock_boto3, mock_created_resource):
        mock_created_resource.side_effect = [False, True]
        services.create_dataschema_pipeline(
            self, self.table, self.manifest.data_pipeline, "arn:aws:iam::user_arn")
