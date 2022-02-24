import boto3
from botocore.stub import Stubber
from plugin.domain.manifest import Manifest
from plugin.infrastructure.resource.aws.services.s3.interface import S3Interface, S3ResourceInterface
from plugin.infrastructure.resource.aws.services.s3.service import S3Resource
import pytest
import random
import string
from unittest import (mock, TestCase)


class MockS3Resource(S3Interface, S3ResourceInterface):

    def __init__(self) -> None:
        pass

    def upload_object(self, path: str, bucket_name: str, package: str):
        return super().upload_object(path=path, bucket_name=bucket_name, package=package)

    def check_bucket(self, bucket_name: str) -> bool:
        return super().check_bucket(bucket_name=bucket_name)

    def check_bucket_object(self, object_key: str, bucket_name: str) -> bool:
        return super().check_bucket_object(object_key=object_key, bucket_name=bucket_name)


class S3ResourceTest(TestCase):

    @mock.patch("plugin.infrastructure.resource.aws.services.s3.service.boto3", autospec=True)
    def setUp(self, mock_boto3) -> None:
        self.mock_session = mock_boto3.Session.return_value
        self.mock_lf = self.mock_session.client.return_value
        self.s3_resource = S3Resource()
        self.s3_client = boto3.client(
            "s3", region_name="us-east-1")
        self.stubber = Stubber(self.s3_client)
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

            s3 = MockS3Resource()

            s3.check_bucket(
                bucket_name=self.manifest.data_pipeline.database.name)

        with pytest.raises(NotImplementedError):
            name = self.__random_string(
                letter=string.ascii_letters.lower(),
                size=18)
            self.manifest.data_pipeline.database.name = f"{name}-test"

            s3 = MockS3Resource()

            s3.check_bucket_object(
                object_key="my-key", bucket_name=self.manifest.data_pipeline.database.name)

        with pytest.raises(NotImplementedError):
            name = self.__random_string(
                letter=string.ascii_letters.lower(),
                size=18)
            self.manifest.data_pipeline.database.name = f"{name}-test"

            s3 = MockS3Resource()

            s3.upload_object(path="path/to/my/file",
                             bucket_name="my-bucket", package="my-package")
