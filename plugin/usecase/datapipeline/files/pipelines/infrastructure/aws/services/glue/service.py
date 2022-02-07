from .interface import GlueInterface
import boto3


class Glue(GlueInterface):
    """
    TO DO

    Args:
        GlueInterface ([type]): [description]
    """

    def __init__(self, **kwargs) -> None:
        self.session = boto3.session.Session()
        self.glue = self.session.client(
            'glue', region_name=kwargs.get('region', "us-east-1"))

    def list_schema_version(self, schema: str, registry: str) -> dict:
        return self.glue.list_schema_versions(
            SchemaId={
                'SchemaName': schema,
                'RegistryName': registry
            }
        )

    def get_schema_version(self, schema: str, registry: str, version: int) -> dict:
        return self.glue.get_schema_version(
            SchemaId={
                'SchemaName': schema,
                'RegistryName': registry
            },
            SchemaVersionNumber={
                'VersionNumber': version
            }
        )
