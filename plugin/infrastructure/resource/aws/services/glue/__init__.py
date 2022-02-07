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
    def update_schema_table(account_id: str, bucket_name: str, database_name: str, path: str, table_name: str, region: str):
        glue = GlueResource(region)
        _schema = glue.update_schema_registry(path, database_name, table_name)
        _schema_table = glue.update_schema_registry(
            path, database_name, f"{table_name}_table")

        if ("Status" in _schema and _schema["Status"] == "FAILURE") or ("Status" in _schema_table and _schema_table["Status"] == "FAILURE"):
            glue.delete_schema_version(
                database_name=database_name, table_name=table_name, version=str(_schema["VersionNumber"]))
            glue.delete_schema_version(
                database_name=database_name, table_name=f"{table_name}_table", version=str(_schema_table["VersionNumber"]))
        else:
            glue.update_table(
                account_id, bucket_name, database_name, table_name)

        return _schema, _schema_table
