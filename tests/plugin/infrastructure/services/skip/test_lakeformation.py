import boto3
from botocore.stub import Stubber
from plugin.domain.manifest import Database, Table, Field, Manifest
from plugin.infrastructure.resource.aws.services.lakeformation.interface import LakeFormationResourceInterface
from plugin.infrastructure.resource.aws.services.lakeformation.service import LakeFormationResource
import pytest
import random
import string
from unittest import (mock, TestCase)


class MockLakeFormationResource(LakeFormationResourceInterface):

    def __init__(self) -> None:
        pass

    def add_classifications_to_resources(self, account_id: str, database: Database) -> None:
        return super().add_classifications_to_resources(account_id, database)


class LakeFormationResourceTest(TestCase):

    @mock.patch("plugin.infrastructure.resource.aws.services.lakeformation.service.boto3", autospec=True)
    def setUp(self, mock_boto3) -> None:
        self.mock_session = mock_boto3.Session.return_value
        self.mock_lf = self.mock_session.client.return_value
        self.lakeformation_resource = LakeFormationResource("us-east-1")
        self.lf_client = boto3.client(
            "lakeformation", region_name="us-east-1")
        self.stubber = Stubber(self.lf_client)
        self.stubber.activate()

    @staticmethod
    def __random_string(letter, size: int):
        return ''.join(random.choice(letter) for _ in range(size))

    @pytest.fixture(autouse=True)
    def plugin_manifest(self):
        self.manifest = Manifest("template/manifest.yaml")

    def mock_add_lf_tags_to_field(self, catalog_id: str, tag_key: str, tag_value: str, db_name: str, tb_name: str, _field: Field, fail: bool = False):

        if fail:
            response = {
                'Failures': [
                    {
                        'LFTag': {
                            'CatalogId': catalog_id,
                            'TagKey': tag_key,
                            'TagValues': [
                                tag_value,
                            ]
                        },
                        'Error': {
                            'ErrorCode': 'EntityNotFoundException',
                            'ErrorMessage': 'Tag or tag value does not exist.'
                        }
                    },
                ]
            }
        else:
            response = {
                'Failures': []
            }

        expected_params = {
            "CatalogId": catalog_id,
            "Resource": {
                'TableWithColumns': {
                    'CatalogId': catalog_id,
                    'DatabaseName': db_name,
                    'Name': tb_name,
                    'ColumnNames': [
                        _field.name
                    ]
                }
            },
            "LFTags": [
                {
                    'CatalogId': catalog_id,
                    'TagKey': tag_key,
                    'TagValues': [
                        tag_value
                    ]
                }
            ]
        }

        self.stubber.add_response(
            'add_lf_tags_to_resource', response, expected_params)

        mocked_response = self.lf_client.add_lf_tags_to_resource(
            CatalogId=catalog_id,
            Resource={
                'TableWithColumns': {
                    'CatalogId': catalog_id,
                    'DatabaseName': db_name,
                    'Name': tb_name,
                    'ColumnNames': [
                        _field.name
                    ]
                }
            },
            LFTags=[
                {
                    'CatalogId': catalog_id,
                    'TagKey': tag_key,
                    'TagValues': [
                        tag_value
                    ]
                }
            ]
        )

        return mocked_response

    def mock_add_lf_tags_to_table(self, catalog_id: str, tag_key: str, tag_value: str, db_name: str, _table: Table, fail: bool = False):

        if fail:
            response = {
                'Failures': [
                    {
                        'LFTag': {
                            'CatalogId': catalog_id,
                            'TagKey': tag_key,
                            'TagValues': [
                                tag_value,
                            ]
                        },
                        'Error': {
                            'ErrorCode': 'EntityNotFoundException',
                            'ErrorMessage': 'Tag or tag value does not exist.'
                        }
                    },
                ]
            }
        else:
            response = {
                'Failures': []
            }

        expected_params = {
            "CatalogId": catalog_id,
            "Resource": {
                'TableWithColumns': {
                    'CatalogId': catalog_id,
                    'DatabaseName': db_name,
                    'Name': _table.name
                }
            },
            "LFTags": [
                {
                    'CatalogId': catalog_id,
                    'TagKey': tag_key,
                    'TagValues': [
                        tag_value
                    ]
                }
            ]
        }

        self.stubber.add_response(
            'add_lf_tags_to_resource', response, expected_params)

        mocked_response = self.lf_client.add_lf_tags_to_resource(
            CatalogId=catalog_id,
            Resource={
                'TableWithColumns': {
                    'CatalogId': catalog_id,
                    'DatabaseName': db_name,
                    'Name': _table.name
                }
            },
            LFTags=[
                {
                    'CatalogId': catalog_id,
                    'TagKey': tag_key,
                    'TagValues': [
                        tag_value
                    ]
                }
            ]
        )

        return mocked_response

    def get_exception_from_add_lf_tags_to_resource(self, exception: str, catalog_id: str):
        self.stubber.add_client_error("add_lf_tags_to_resource",
                                      service_error_code=exception,
                                      service_message=exception,
                                      http_status_code=404)
        try:
            self.lf_client.add_lf_tags_to_resource(
                CatalogId=catalog_id,
                Resource={
                    'TableWithColumns': {
                        'CatalogId': catalog_id,
                        'DatabaseName': "database_name",
                        'Name': "table_name",
                        'ColumnNames': [
                            "field.name"
                        ]
                    }
                },
                LFTags=[
                    {
                        'CatalogId': catalog_id,
                        'TagKey': "tag_key",
                        'TagValues': [
                            "tag_value"
                        ]
                    }
                ]
            )
        except Exception as error:
            return error

    def test_not_implemented_error(self):
        with pytest.raises(NotImplementedError):
            name = self.__random_string(
                letter=string.ascii_letters.lower(),
                size=18)
            self.manifest.data_pipeline.database.name = f"{name}-test"

            lakeformation = MockLakeFormationResource()
            lakeformation.add_classifications_to_resources(
                "123456789", self.manifest.data_pipeline.database)

    def test_successfully_add_classification_to_field(self):

        my_field = Field(
            name="field_name",
            access_level="partial",
            data_classification="business",
            pii="true",
            private="false",
            type="int"
        )
        db_name = "db_name"
        tb_name = "tb_name"

        self.mock_lf.add_lf_tags_to_resource.side_effect = [
            self.mock_add_lf_tags_to_field(
                catalog_id="123456789",
                tag_key="pii",
                tag_value="valid_value",
                db_name=db_name,
                tb_name=tb_name,
                _field=my_field
            )
        ]

        resource_response = self.lakeformation_resource._LakeFormationResource__add_classification_to_field(
            catalog_id="123456789",
            database_name=db_name,
            table_name=tb_name,
            field=my_field
        )

        assert "Failures" in resource_response
        assert len(resource_response["Failures"]) == 0

    def test_fail_add_classification_to_field(self):

        # internal_error = self.get_exception_from_add_lf_tags_to_resource(
        #     exception="InternalError")
        # self.mock_lf.add_lf_tags_to_resource.side_effect = [internal_error]

        my_field = Field(
            name="field_name",
            access_level="partial",
            data_classification="business",
            pii="true",
            private="false",
            type="int"
        )
        db_name = "db_name"
        tb_name = "tb_name"

        self.mock_lf.add_lf_tags_to_resource.side_effect = [
            self.mock_add_lf_tags_to_field(
                catalog_id="123456789",
                tag_key="pii",
                tag_value="valid_value",
                db_name=db_name,
                tb_name=tb_name,
                _field=my_field,
                fail=True
            )
        ]

        resource_response = self.lakeformation_resource._LakeFormationResource__add_classification_to_field(
            catalog_id="123456789",
            database_name=db_name,
            table_name=tb_name,
            field=my_field
        )

        assert "Failures" in resource_response
        assert len(resource_response["Failures"]) > 0

    def test_successfully_add_classification_to_table(self):

        db_name = "db_name"
        tb_name = "tb_name"

        my_field = Field(
            name="field_name",
            access_level="partial",
            data_classification="business",
            pii="true",
            private="false",
            type="int"
        )

        my_table = Table(
            name=tb_name,
            access_level="partial",
            data_classification="business",
            manipulated="true",
            pii="true",
            private="false",
            fields=[my_field]
        )

        self.mock_lf.add_lf_tags_to_resource.side_effect = [
            self.mock_add_lf_tags_to_field(
                catalog_id="123456789",
                tag_key="pii",
                tag_value="valid_value",
                db_name=db_name,
                tb_name=tb_name,
                _field=my_field
            ),
            self.mock_add_lf_tags_to_table(
                catalog_id="123456789",
                tag_key="pii",
                tag_value="valid_value",
                db_name=db_name,
                _table=my_table
            )
        ]

        resource_response = self.lakeformation_resource._LakeFormationResource__add_classifications_to_table(
            catalog_id="123456789",
            database_name=db_name,
            table=my_table
        )

        assert "Failures" in resource_response
        assert len(resource_response["Failures"]) == 0

    def test_fail_add_classification_to_table(self):

        db_name = "db_name"
        tb_name = "tb_name"

        my_field = Field(
            name="field_name",
            access_level="partial",
            data_classification="business",
            pii="true",
            private="false",
            type="int"
        )

        my_table = Table(
            name=tb_name,
            access_level="partial",
            data_classification="business",
            manipulated="true",
            pii="true",
            private="false",
            fields=[my_field]
        )

        self.mock_lf.add_lf_tags_to_resource.side_effect = [
            self.mock_add_lf_tags_to_field(
                catalog_id="123456789",
                tag_key="pii",
                tag_value="valid_value",
                db_name=db_name,
                tb_name=tb_name,
                _field=my_field
            ),
            self.mock_add_lf_tags_to_table(
                catalog_id="123456789",
                tag_key="pii",
                tag_value="valid_value",
                db_name=db_name,
                _table=my_table,
                fail=True
            )
        ]

        resource_response = self.lakeformation_resource._LakeFormationResource__add_classifications_to_table(
            catalog_id="123456789",
            database_name=db_name,
            table=my_table
        )

        assert "Failures" in resource_response
        assert len(resource_response["Failures"]) > 0

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
