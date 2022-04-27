from plugin.domain.model import Table
from plugin.utils.logging import logger
from .service import GlueResource


class Glue:
    """
    TO DO
    """

    @staticmethod
    def check_schema_version(registry_name: str, schema_name: str, region: str) -> bool:
        glue = GlueResource(region)
        return glue.check_schema_version(registry_name, schema_name)

    @staticmethod
    def check_database_exists(catalog_id: str, database_name: str, region: str) -> bool:
        glue = GlueResource(region)
        return glue.check_database_exists(catalog_id, database_name)

    @staticmethod
    def check_registry_exists(registry_name: str, region: str) -> bool:
        glue = GlueResource(region)
        return glue.check_registry_exists(registry_name)

    @staticmethod
    def update_schema(account_id: str, bucket_name: str, database_name: str, table: Table, region: str):
        glue = GlueResource(region)
        _schema = glue.update_schema_registry(database_name, table)

        if ("Status" in _schema and _schema["Status"] == "FAILURE"):
            glue.delete_schema_version(
                database_name=database_name, table_name=table.name, version=str(_schema["VersionNumber"]))            
        else:
            response = glue.update_table(
                account_id=account_id,
                bucket_name=bucket_name,
                database_name=database_name,
                table=table
            )

            if ("Status" in response and response["Status"] == "FAILURE"):
                logger.error("An error occurred while trying to update the version of '%s' table in the '%s' database\n%s", database_name, table.name, e)

        return _schema
