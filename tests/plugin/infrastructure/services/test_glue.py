import json
import boto3
from botocore.stub import Stubber
from utils.file import read_avro_schema
from plugin.domain.manifest import Manifest
from plugin.infrastructure.resource.aws.services.glue.interface import GlueResourceInterface
from plugin.infrastructure.resource.aws.services.glue.service import GlueResource
import pytest
import random
import string
from unittest import (mock, TestCase)


class MockGlueResource(GlueResourceInterface):

    def __init__(self) -> None:
        pass

    def check_schema_version(self, registry_name: str, schema_name: str) -> bool:
        return super().check_schema_version(registry_name, schema_name)


class S3ResourceTest(TestCase):

    @mock.patch("plugin.infrastructure.resource.aws.services.glue.service.boto3", autospec=True)
    def setUp(self, mock_boto3) -> None:
        self.mock_session = mock_boto3.Session.return_value
        self.mock_glue = self.mock_session.client.return_value
        self.glue_resource = GlueResource("us-east-1")
        self.glue_client = boto3.client(
            "glue", region_name="us-east-1")
        self.stubber = Stubber(self.glue_client)
        self.stubber.activate()

    @staticmethod
    def __random_string(letter, size: int):
        return ''.join(random.choice(letter) for _ in range(size))

    @pytest.fixture(autouse=True)
    def plugin_manifest(self):
        self.manifest = Manifest("template/manifest.yaml")

    def test_not_implemented_error(self):
        with pytest.raises(NotImplementedError):
            name = self.__random_string(
                letter=string.ascii_letters.lower(),
                size=18)
            self.manifest.data_pipeline.database.name = f"{name}-test"

            glue = MockGlueResource()

            glue.check_schema_version(
                registry_name=self.manifest.data_pipeline.database.name,
                schema_name=self.manifest.data_pipeline.database.schemas.tables[0].name
            )

    def mock_check_schema_version_validity(self, path: str, table_name: dict):
        response = {
            'Valid': True,
        }

        schema_definition = read_avro_schema(f"{path}/{table_name}.avsc")

        expected_params = {
            'DataFormat': 'AVRO',
            'SchemaDefinition': json.dumps(schema_definition)
        }

        self.stubber.add_response(
            'check_schema_version_validity', response, expected_params)

        mocked_response = self.glue_client.check_schema_version_validity(
            DataFormat='AVRO',
            SchemaDefinition=json.dumps(schema_definition)
        )

        return mocked_response

    @mock.patch("plugin.infrastructure.resource.aws.services.glue.service.boto3", autospec=True)
    def test_succesfull_check_schema_version_validity(self, mock_boto3_glue):

        path = self.manifest.data_pipeline.database.schemas.path
        table_name = self.manifest.data_pipeline.database.schemas.tables[0].name

        session = mock_boto3_glue.Session.return_value
        mock_glue = session.client.return_value

        mock_glue.check_schema_version_validity.side_effect = [
            self.mock_check_schema_version_validity(
                path=path, table_name=table_name)
        ]

        schema_definition = read_avro_schema(f"{path}/{table_name}.avsc")

        glue = GlueResource("us-east-1")
        glue.check_schema_version_validity(
            data_format="AVRO",
            schema_definition=json.dumps(schema_definition)
        )
