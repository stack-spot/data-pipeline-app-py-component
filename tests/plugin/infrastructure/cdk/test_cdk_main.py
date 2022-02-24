import string
import json
import random
import pytest
from unittest import (mock)
from plugin.domain.manifest import Manifest, Table
from plugin.infrastructure.resource.aws.cdk.main import AwsCdk


class TestCdkMain:

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
                    "name": "default",
                    "schemas": {
                        "path": "./template",
                        "tables": []
                    }
                }
            }
        })
        self.manifest_taxo = Table(name=self.__random_string(
            letter=string.ascii_letters.lower(), size=18),  manipulated="false", fields=[])

    @mock.patch("plugin.infrastructure.resource.aws.cdk.main.create_dataschema_registry_database", return_value=None)
    @mock.patch("plugin.infrastructure.resource.aws.cdk.main.create_dataschema_pipeline", return_value=None)
    @mock.patch("plugin.infrastructure.resource.aws.cdk.main.SDK.check_schema_version", return_value=False)
    def test_cdk_apply_datapipeline_stack(self, mock_check_schema, mock_cdk_create_pipeline, mock_cdk_create_registry_database):
        cdk_stack = AwsCdk()
        cdk_stack.apply_datapipeline_stack(self.manifest.data_pipeline)


    @mock.patch("plugin.infrastructure.resource.aws.cdk.main.SDK.check_bucket", side_effect=[True, True])
    @mock.patch("plugin.infrastructure.resource.aws.cdk.main.SDK.account_id", new_callable=mock.PropertyMock, return_value="123456")
    def test_cdk_create_assets(self, mock_account_id, mock_check_bucket):
        cdk_stack = AwsCdk()
        cdk_stack.create_assets(self.manifest.data_pipeline)