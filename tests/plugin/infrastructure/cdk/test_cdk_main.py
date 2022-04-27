import string
import random
import pytest
from unittest import (mock)
from plugin.domain.manifest import Manifest, Table, Schema, Field
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

    @mock.patch("plugin.infrastructure.resource.aws.cdk.main.add_classifications", return_value=None)
    @mock.patch("plugin.infrastructure.resource.aws.cdk.main.create_dataschema_registry_database", return_value=None)
    @mock.patch("plugin.infrastructure.resource.aws.cdk.main.create_dataschema_pipeline", return_value=None)
    @mock.patch("plugin.infrastructure.resource.aws.cdk.main.SDK.check_schema_version", return_value=False)
    @mock.patch("plugin.infrastructure.resource.aws.cdk.main.SDK.user_arn", new_callable=mock.PropertyMock, return_value="arn:user")
    def test_cdk_apply_datapipeline_stack(self, mock_user_arn, mock_check_schema, mock_cdk_create_pipeline, mock_cdk_create_registry_database, mock_add_classifications):
        cdk_stack = AwsCdk()
        cdk_stack.apply_datapipeline_stack(self.manifest.data_pipeline)

    @mock.patch("plugin.infrastructure.resource.aws.cdk.main.SDK.check_bucket", side_effect=[True, True])
    @mock.patch("plugin.infrastructure.resource.aws.cdk.main.SDK.account_id", new_callable=mock.PropertyMock, return_value="123456")
    @mock.patch("plugin.infrastructure.resource.aws.cdk.main.SDK.user_arn", new_callable=mock.PropertyMock, return_value="arn:user")
    def test_cdk_create_assets(self, mock_account_id, mock_check_bucket, mock_user_arn):
        cdk_stack = AwsCdk()
        cdk_stack.create_assets(self.manifest.data_pipeline)
