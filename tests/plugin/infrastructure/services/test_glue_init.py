from plugin.domain.model import Field, Schema, Table
from plugin.infrastructure.resource.aws.services.glue import Glue as glue
from unittest import (mock)
import pytest


class TestGlueInit:

    @pytest.fixture(autouse=True)
    def plugin_manifest(self):

        self.table = Table(
            name="test_glue_init",
            description="doc",
            schema=Schema(
                definition={'type': 'record', 'name': 'foo_bar', 'doc': 'Foo bar', 'fields': [
                    {'name': 'foo', 'type': ['null', 'string'], 'doc': 'bar', 'default': None}]}
            ),
            fields=[
                Field(
                    name="foo",
                    type="string",
                    description="bar"
                )
            ]
        )

    @mock.patch("plugin.infrastructure.resource.aws.services.glue.GlueResource.check_schema_version", return_value={})
    def test_glue_init_check_schema_version(self, _):
        glue().check_schema_version("registry", "schema", "us-east-1")

    @mock.patch("plugin.infrastructure.resource.aws.services.glue.GlueResource.check_database_exists", return_value=True)
    def test_glue_init_check_database_exists(self, _):
        glue().check_database_exists("id", "database", "us-east-1")

    @mock.patch("plugin.infrastructure.resource.aws.services.glue.GlueResource.check_registry_exists", return_value=True)
    def test_glue_init_check_registry_exists(self, _):
        glue().check_registry_exists("registry", "us-east-1")

    @mock.patch("plugin.infrastructure.resource.aws.services.glue.GlueResource.update_table", return_value={})
    @mock.patch("plugin.infrastructure.resource.aws.services.glue.GlueResource.update_schema_registry", return_value={"Status": "SUCCESS", "VersionNumber": 1})
    def test_glue_init_update_schema(self, _registry, _table):
        _schema = glue().update_schema("123456", "xxxxxx-raw-flowers",
                                       "database", self.table, "us-east-1")
        assert _schema == {"Status": "SUCCESS", "VersionNumber": 1}

    @mock.patch("plugin.infrastructure.resource.aws.services.glue.GlueResource.delete_schema_version", return_value={})
    @mock.patch("plugin.infrastructure.resource.aws.services.glue.GlueResource.update_schema_registry", return_value={"Status": "FAILURE", "VersionNumber": 2})
    def test_glue_init_failed_update_schema(self, _registry, _table):
        _schema = glue().update_schema("123456", "xxxxxx-raw-flowers",
                                       "database", self.table, "us-east-1")
        assert _schema == {"Status": "FAILURE", "VersionNumber": 2}
