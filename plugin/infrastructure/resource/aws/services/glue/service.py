from __future__ import annotations
import boto3
from typing_extensions import Literal
from plugin.domain.exceptions import InvalidSchemaVersionException
from plugin.domain.model import Table
from plugin.utils.cdk import fields_to_columns
from plugin.utils.string import kebab, snake_case
from .interface import GlueResourceInterface
from botocore.client import ClientError
from plugin.utils.logging import logger


class GlueResource(GlueResourceInterface):
    """
    TO DO

    Args:
        GlueResourceInterface ([type]): [description]
    """

    def __init__(self, region: str):
        session = boto3.Session()
        self.glue_client = session.client("glue", region_name=region)

    def check_schema_version_validity(self, data_format: Literal["AVRO", "JSON"], schema_definition: str) -> bool:
        try:
            response = self.glue_client.check_schema_version_validity(
                DataFormat=data_format,
                SchemaDefinition=schema_definition
            )
            return response.get('Valid', False)
        except ClientError as e:
            logger.info("Invalid schema version.\n%s", e)
            return False

    def get_last_schema_version(self, registry_name: str, table_name: str) -> int | None:
        try:
            response = self.glue_client.get_schema_version(
                SchemaId={
                    "RegistryName": registry_name,
                    "SchemaName": table_name
                },
                SchemaVersionNumber={
                    "LatestVersion": True
                }
            )

            return response["VersionNumber"] if "VersionNumber" in response else None
        except ClientError:
            logger.info(
                "Could not get latest version from schema '%s' of registry '%s'.", table_name, registry_name)
        return None

    def check_schema_version(self, registry_name: str, schema_name: str) -> bool:
        try:
            logger.info(
                "Checking if there is any version of schema '%s' in registry '%s'.", schema_name, registry_name)
            response = self.get_last_schema_version(registry_name, schema_name)
            return response is not None
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundExcetpion':
                return False
            logger.error(e)
        return False

    def check_database_exists(self, catalog_id: str, database_name: str) -> bool:
        try:
            response = self.glue_client.get_database(
                CatalogId=catalog_id,
                Name=database_name
            )

            if response is not None and 'Database' in response:
                if response['Database'].get('Name', None) == database_name and response['Database'].get('CatalogId', None) == catalog_id:
                    return True
                return False
        except ClientError:
            return False
        return False

    def check_registry_exists(self, registry_name: str) -> bool:

        try:
            response = self.glue_client.get_registry(
                RegistryId={
                    'RegistryName': registry_name
                }
            )
            if response.get('Status', None) == "AVAILABLE" and response.get('RegistryName', None) == registry_name:
                return True
            return False
        except ClientError:
            return False

    def update_schema_registry(self, database_name: str, table: Table) -> dict:
        try:
            schema_definition = table.schema.definition_to_json

            if not self.check_schema_version_validity(data_format="AVRO", schema_definition=schema_definition):
                raise InvalidSchemaVersionException(
                    f"Invalid schema version '{table.name}.avsc'.")

            response = self.glue_client.register_schema_version(
                SchemaId={
                    'SchemaName': table.name,
                    'RegistryName': database_name
                },
                SchemaDefinition=schema_definition
            )
            if ("Status" in response and response["Status"] == "FAILURE"):
                logger.error(
                    "An error occurred while trying to registering new version to schema '%s'.", table.name)
            return response
        except ClientError:
            logger.error(
                "An error occurred while trying to registering new version to schema '%s'.", table.name)
        return {}

    def delete_schema_version(self, database_name: str, table_name: str, version: str) -> dict:
        try:
            logger.info(
                "Deleting version '%s' of schema '%s'.", version, table_name)
            response = self.glue_client.delete_schema_versions(
                SchemaId={
                    'SchemaName': table_name,
                    'RegistryName': database_name
                },
                Versions=version
            )

            return response
        except ClientError:
            logger.error(
                "An error occurred while trying to delete version '%s' to schema '%s'.", version, table_name)
        return {}

    def update_table(self, account_id: str, bucket_name: str, database_name: str, table: Table) -> dict:
        try:
            logger.info(
                "Updating table '%s'. . .", table.name)

            columns = fields_to_columns(table.fields)

            response = self.glue_client.update_table(
                CatalogId=account_id,
                DatabaseName=database_name,
                TableInput={
                    "Name": table.name,
                    "Description": "",
                    "Parameters": {
                        "EXTERNAL": "TRUE",
                        "parquet.compression": "SNAPPY",
                        "classification": "parquet"
                    },
                    "PartitionKeys": [
                        {
                            "Name": "_version",
                            "Type": "int",
                        },
                        {
                            "Name": "_date",
                            "Type": "date",
                        },
                    ],
                    "StorageDescriptor": {
                        'Columns': [
                            {
                                'Name': column.name,
                                'Type': column.type,
                                'Comment': column.comment
                            } for column in columns
                        ],
                        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        "Location": f"s3://{bucket_name}/{snake_case(database_name)}/{snake_case(table.name)}/",
                        "SerdeInfo": {
                            "Name": kebab(f"{table.name}-stream"),
                            "Parameters": {
                                    "serialization.format": "1"
                            },
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        }
                    },
                    "TableType": "EXTERNAL_TABLE",
                }
            )

            return response
        except ClientError as e:
            logger.error(
                "An error occurred while trying to update the version of '%s' table in the '%s' database\n%s", database_name, table.name, e)
        return {}
