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

    # def test_successfully_add_classification_to_field(self):

    #     my_field = Field(
    #         name="field_name",
    #         access_level="partial",
    #         data_classification="business",
    #         pii="true",
    #         private="false",
    #         type="int"
    #     )
    #     db_name = "db_name"
    #     tb_name = "tb_name"

    #     self.mock_lf.add_lf_tags_to_resource.side_effect = [
    #         self.mock_add_lf_tags_to_field(
    #             catalog_id="123456789",
    #             tag_key="pii",
    #             tag_value="valid_value",
    #             db_name=db_name,
    #             tb_name=tb_name,
    #             _field=my_field
    #         )
    #     ]

    #     resource_response = self.lakeformation_resource._LakeFormationResource__add_classification_to_field(
    #         catalog_id="123456789",
    #         database_name=db_name,
    #         table_name=tb_name,
    #         field=my_field
    #     )

    #     assert "Failures" in resource_response
    #     assert len(resource_response["Failures"]) == 0

    # def test_fail_add_classification_to_field(self):

    #     # internal_error = self.get_exception_from_add_lf_tags_to_resource(
    #     #     exception="InternalError")
    #     # self.mock_lf.add_lf_tags_to_resource.side_effect = [internal_error]

    #     my_field = Field(
    #         name="field_name",
    #         access_level="partial",
    #         data_classification="business",
    #         pii="true",
    #         private="false",
    #         type="int"
    #     )
    #     db_name = "db_name"
    #     tb_name = "tb_name"

    #     self.mock_lf.add_lf_tags_to_resource.side_effect = [
    #         self.mock_add_lf_tags_to_field(
    #             catalog_id="123456789",
    #             tag_key="pii",
    #             tag_value="valid_value",
    #             db_name=db_name,
    #             tb_name=tb_name,
    #             _field=my_field,
    #             fail=True
    #         )
    #     ]

    #     resource_response = self.lakeformation_resource._LakeFormationResource__add_classification_to_field(
    #         catalog_id="123456789",
    #         database_name=db_name,
    #         table_name=tb_name,
    #         field=my_field
    #     )

    #     assert "Failures" in resource_response
    #     assert len(resource_response["Failures"]) > 0

    # def test_successfully_add_classification_to_table(self):

    #     db_name = "db_name"
    #     tb_name = "tb_name"

    #     my_field = Field(
    #         name="field_name",
    #         access_level="partial",
    #         data_classification="business",
    #         pii="true",
    #         private="false",
    #         type="int"
    #     )

    #     my_table = Table(
    #         name=tb_name,
    #         access_level="partial",
    #         data_classification="business",
    #         manipulated="true",
    #         pii="true",
    #         private="false",
    #         fields=[my_field]
    #     )

    #     self.mock_lf.add_lf_tags_to_resource.side_effect = [
    #         self.mock_add_lf_tags_to_field(
    #             catalog_id="123456789",
    #             tag_key="pii",
    #             tag_value="valid_value",
    #             db_name=db_name,
    #             tb_name=tb_name,
    #             _field=my_field
    #         ),
    #         self.mock_add_lf_tags_to_table(
    #             catalog_id="123456789",
    #             tag_key="pii",
    #             tag_value="valid_value",
    #             db_name=db_name,
    #             _table=my_table
    #         )
    #     ]

    #     resource_response = self.lakeformation_resource._LakeFormationResource__add_classifications_to_table(
    #         catalog_id="123456789",
    #         database_name=db_name,
    #         table=my_table
    #     )

    #     assert "Failures" in resource_response
    #     assert len(resource_response["Failures"]) == 0

    # def test_fail_add_classification_to_table(self):

    #     db_name = "db_name"
    #     tb_name = "tb_name"

    #     my_field = Field(
    #         name="field_name",
    #         access_level="partial",
    #         data_classification="business",
    #         pii="true",
    #         private="false",
    #         type="int"
    #     )

    #     my_table = Table(
    #         name=tb_name,
    #         access_level="partial",
    #         data_classification="business",
    #         manipulated="true",
    #         pii="true",
    #         private="false",
    #         fields=[my_field]
    #     )

    #     self.mock_lf.add_lf_tags_to_resource.side_effect = [
    #         self.mock_add_lf_tags_to_field(
    #             catalog_id="123456789",
    #             tag_key="pii",
    #             tag_value="valid_value",
    #             db_name=db_name,
    #             tb_name=tb_name,
    #             _field=my_field
    #         ),
    #         self.mock_add_lf_tags_to_table(
    #             catalog_id="123456789",
    #             tag_key="pii",
    #             tag_value="valid_value",
    #             db_name=db_name,
    #             _table=my_table,
    #             fail=True
    #         )
    #     ]

    #     resource_response = self.lakeformation_resource._LakeFormationResource__add_classifications_to_table(
    #         catalog_id="123456789",
    #         database_name=db_name,
    #         table=my_table
    #     )

    #     assert "Failures" in resource_response
    #     assert len(resource_response["Failures"]) > 0

    # def test_succesfully_add_all_classifications(self):

    #     db_name = "db_name"
    #     tb_name = "tb_name"

    #     my_field = Field(
    #         name="field_name",
    #         access_level="partial",
    #         data_classification="business",
    #         pii="true",
    #         private="false",
    #         type="int"
    #     )

    #     my_table = Table(
    #         name=tb_name,
    #         access_level="partial",
    #         data_classification="business",
    #         manipulated="true",
    #         pii="true",
    #         private="false",
    #         fields=[my_field]
    #     )

    #     self.mock_lf.add_lf_tags_to_resource.side_effect = [
    #         self.mock_add_lf_tags_to_field(
    #             catalog_id="123456789",
    #             tag_key="pii",
    #             tag_value="valid_value",
    #             db_name=db_name,
    #             tb_name=tb_name,
    #             _field=my_field
    #         ),
    #         self.mock_add_lf_tags_to_table(
    #             catalog_id="123456789",
    #             tag_key="pii",
    #             tag_value="valid_value",
    #             db_name=db_name,
    #             _table=my_table
    #         )
    #     ]

    #     lf = LakeFormation()

    #     lf.add_classifications_to_resources(
    #         "123456789", self.manifest.data_pipeline.database, "us-east-1")
