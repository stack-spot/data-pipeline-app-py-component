from plugin.domain.manifest import Database, Field, Table
from .interface import LakeFormationResourceInterface
from botocore.client import ClientError
import boto3
from plugin.utils.logging import logger


class LakeFormationResource(LakeFormationResourceInterface):
    """
    TO DO

    Args:
        LakeFormationResourceInterface ([type]): [description]
    """

    def __init__(self, region: str):
        session = boto3.Session()
        self.lakeformation_client = session.client(
            "lakeformation", region_name=region)

    @staticmethod
    def __verify_response(response: dict, action: str):

        if response is not None and "Failures" in response and len(response["Failures"]) > 0:

            failures = [
                {
                    "tag_key": failure["LFTag"]["TagKey"],
                    "tag_values": failure["LFTag"]["TagValues"],
                    "error_message": failure["Error"]["ErrorMessage"]
                } for failure in response["Failures"]
            ]

            error_response = {
                'Error': {
                    'Code': "InternalError",
                    'Message': ''.join(
                        f'\n{num} - {failure["error_message"]}\n\ttag_key: {failure["tag_key"]}\
                                \n\ttag_values: {failure["tag_values"]}' for num, failure in enumerate(failures, start=1)
                    )
                }

            }

            raise ClientError(
                error_response, action)

    def add_classifications_to_resources(self, account_id: str, database: Database):

        for table in database.schemas.tables:
            try:
                response = self.__add_classifications_to_table(
                    catalog_id=account_id,
                    database_name=database.name,
                    table=table
                )

                self.__verify_response(
                    response=response, action="AddLFTagsToResource")

            except ClientError as e:
                logger.error(
                    "There was one or more errors while adding classifictaion to table '%s'.\n%s", table.name, e)

    def __add_classifications_to_table(self, catalog_id: str, database_name: str, table: Table) -> dict:

        for _field in table.fields:

            try:

                response = self.__add_classification_to_field(
                    catalog_id=catalog_id,
                    database_name=database_name,
                    table_name=table.name,
                    field=_field
                )

                self.__verify_response(
                    response=response, action="AddLFTagsToResource")

            except ClientError as e:
                logger.error(
                    "There was one or more errors while adding classifictaion to field '%s'.\n%s", _field.name, e)

        classifications = {
            tag_key: getattr(table, tag_key) for tag_key in table.tag_keys if getattr(table, tag_key) != ''
        }
        logger.info("Adding classifications %s to table '%s'",
                    classifications, table.name)

        return self.lakeformation_client.add_lf_tags_to_resource(
            CatalogId=catalog_id,
            Resource={
                'Table': {
                    'CatalogId': catalog_id,
                    'DatabaseName': database_name,
                    'Name': table.name
                }
            },
            LFTags=[
                {
                    'CatalogId': catalog_id,
                    'TagKey': tag_key,
                    'TagValues': [
                        getattr(table, tag_key)
                    ]
                } for tag_key in table.tag_keys if getattr(table, tag_key) != ''
            ]
        )

    def __add_classification_to_field(self, catalog_id: str, database_name: str, table_name: str, field: Field) -> dict:

        classifications = {
            tag_key: getattr(field, tag_key) for tag_key in field.tag_keys if getattr(field, tag_key) != ''
        }

        logger.info("Adding classifications %s to field '%s' from table '%s'",
                    classifications, field.name, table_name)

        return self.lakeformation_client.add_lf_tags_to_resource(
            CatalogId=catalog_id,
            Resource={
                'TableWithColumns': {
                    'CatalogId': catalog_id,
                    'DatabaseName': database_name,
                    'Name': table_name,
                    'ColumnNames': [
                        field.name
                    ]
                }
            },
            LFTags=[
                {
                    'CatalogId': catalog_id,
                    'TagKey': tag_key,
                    'TagValues': [
                        getattr(field, tag_key)
                    ]
                } for tag_key in field.tag_keys if getattr(field, tag_key) != ''
            ]
        )
