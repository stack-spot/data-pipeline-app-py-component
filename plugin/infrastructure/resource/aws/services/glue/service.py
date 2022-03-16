from __future__ import annotations
import json
from typing_extensions import Literal
from plugin.domain.exceptions import InvalidSchemaVersionException
from plugin.utils.file import get_schema_definition, read_avro_schema
from .interface import GlueResourceInterface
from botocore.client import ClientError
import boto3
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
        except ClientError as e:
            logger.info(
                "Could not get latest version from schema '%s' of registry '%s'.\n%s", table_name, registry_name, e)
        return None

    def check_schema_version(self, registry_name: str, table_name: str) -> bool:
        try:
            response = self.get_last_schema_version(registry_name, table_name)
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

    def update_schema_registry(self, path: str, database_name: str, table_name: str) -> dict:
        try:
            if table_name.endswith("_table"):
                avro_schema = read_avro_schema(
                    f"{path}/{table_name.removesuffix('_table')}.avsc")

                avro_schema["fields"].extend([
                    {
                        "name": "event_time",
                        "type": ["null", "long"],
                        "logicalType": "timestamp-millis",
                        "default": None
                    },
                    {
                        "name": "event_id",
                        "type": "int"
                    }
                ])

                schema_definition = json.dumps(avro_schema)
            else:
                schema_definition = get_schema_definition(path, table_name)

            if not self.check_schema_version_validity(data_format="AVRO", schema_definition=schema_definition):
                raise InvalidSchemaVersionException(
                    f"Invalid schema version '{table_name}.avsc'.")

            response = self.glue_client.register_schema_version(
                SchemaId={
                    'SchemaName': table_name,
                    'RegistryName': database_name
                },
                SchemaDefinition=schema_definition
            )
            if ("Status" in response and response["Status"] == "FAILURE"):
                logger.error(
                    "An error occurred while trying to registering new version to schema '%s'.", table_name)

            return response
        except ClientError:
            logger.error(
                "An error occurred while trying to registering new version to schema '%s'.", table_name)
        return {}

    def delete_schema_version(self, database_name: str, table_name: str, version: str) -> dict:
        try:
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

    def update_table(self, account_id: str, bucket_name: str, database_name: str, table_name: str) -> dict:
        try:
            response = self.glue_client.update_table(
                CatalogId=account_id,
                DatabaseName=database_name,
                TableInput={
                    "Name": table_name,
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
                        "SchemaReference": {
                            "SchemaId": {
                                "RegistryName": database_name,
                                "SchemaName": f"{table_name}_table"
                            },
                            "SchemaVersionNumber": self.get_last_schema_version(database_name, f"{table_name}_table"),
                        },
                        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        "Location": f"s3://{bucket_name}/{database_name}/{table_name}/",
                        "SerdeInfo": {
                            "Name": f"{table_name}-stream",
                            "Parameters": {
                                    "serialization.format": "1"
                            },
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        }
                    },
                    "TableType": "EXTERNAL_TABLE",
                }
            )

            if ("Status" in response and response["Status"] == "FAILURE"):
                logger.error(
                    "An error occurred while trying to update the version of '%s' table in the '%s' database", database_name, table_name)
        except ClientError as e:
            logger.error(
                "An error occurred while trying to update the version of '%s' table in the '%s' database\n%s", database_name, table_name, e)
        return {}
