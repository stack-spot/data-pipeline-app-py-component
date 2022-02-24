from botocore.stub import Stubber
from botocore.session import Session
import pytest
import string

from datetime import datetime
import boto3
import random

from plugin.infrastructure.resource.aws.services.glue.service import GlueResource
from unittest import (mock)


class TestServiceGlue:
    
    @staticmethod
    def __random_string(letter, size: int):
        return ''.join(random.choice(letter) for _ in range(size))

    def test_glue_check_schema_version_validity(self):
        client = boto3.Session().client("glue")
        with mock.patch("plugin.infrastructure.resource.aws.services.glue.service.boto3.Session.client") as mock_client:
            mock_client.return_value = client
            stubber = Stubber(client)
            expected_params = {
                'DataFormat': 'AVRO',
                'SchemaDefinition': '{}'
            }

            response = {
                'Valid': True
            }

            stubber.add_response(
                'check_schema_version_validity', response, expected_params)
            stubber.activate()
            rep = GlueResource(
                'us-east-1').check_schema_version_validity('AVRO', '{}')
            stubber.deactivate()
            assert rep == True

    def test_glue_client_error_check_schema_version_validity(self):
        client = boto3.Session().client("glue")
        with mock.patch("plugin.infrastructure.resource.aws.services.glue.service.boto3.Session.client") as mock_client:
            mock_client.return_value = client
            stubber = Stubber(client)
            stubber.add_client_error("check_schema_version_validity",
                                     service_error_code="Forbidden",
                                     service_message="Forbidden",
                                     http_status_code=403)
            stubber.activate()
            rep = GlueResource(
                'us-east-1').check_schema_version_validity('AVRO', '{}')
            stubber.deactivate()
            assert rep == False

    def test_glue_get_last_schema_version(self):
        client = boto3.Session().client("glue")
        with mock.patch("plugin.infrastructure.resource.aws.services.glue.service.boto3.Session.client") as mock_client:
            mock_client.return_value = client
            stubber = Stubber(client)
            expected_params = {
                'SchemaId': {
                    "RegistryName": 'registry',
                    "SchemaName": 'table'
                },
                'SchemaVersionNumber': {
                    "LatestVersion": True
                }
            }

            response = {
                'SchemaVersionId': self.__random_string(letter=string.ascii_letters,size=36),
                'SchemaDefinition': '{}',
                'DataFormat': 'AVRO',
                'VersionNumber': 1,
                'Status': 'AVAILABLE',
            }

            stubber.add_response(
                'get_schema_version', response, expected_params)
            stubber.activate()
            rep = GlueResource(
                'us-east-1').get_last_schema_version('registry', 'table')
            stubber.deactivate()
            assert rep == 1
