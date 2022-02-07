from .service import Glue as GlueService


class Glue:
    """
    TO DO
    """

    @staticmethod
    def filter_list_schema_version(schema: str, registry: str, region: str) -> list:
        glue = GlueService(region=region)
        response = glue.list_schema_version(schema, registry)
        list_versions = []
        for _schema in response['Schemas']:
            list_versions.append(_schema['VersionNumber'])
        list_versions.sort()
        return list_versions

    @staticmethod
    def get_schema_version(schema: str, registry: str, version: int, region: str) -> list:
        glue = GlueService(region=region)
        response = glue.get_schema_version(schema, registry, version)
        return response['SchemaDefinition']
