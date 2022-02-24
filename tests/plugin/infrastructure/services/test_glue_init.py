from plugin.infrastructure.resource.aws.services.glue import Glue as glue
from unittest import (mock)

class TestGlueInit:

   @mock.patch("plugin.infrastructure.resource.aws.services.glue.GlueResource.check_schema_version", return_value={})
   def test_glue_init_check_schema_version(self, _):
      glue().check_schema_version("registry", "schema", "us-east-1")
   
   @mock.patch("plugin.infrastructure.resource.aws.services.glue.GlueResource.check_database_exists", return_value=True)
   def test_glue_init_check_database_exists(self, _):
      glue().check_database_exists("id", "database", "us-east-1")
   
   @mock.patch("plugin.infrastructure.resource.aws.services.glue.GlueResource.check_registry_exists", return_value=True)
   def test_glue_init_check_registry_exists(self, _):
      glue().check_registry_exists("registry", "us-east-1")
   
   @mock.patch("plugin.infrastructure.resource.aws.services.glue.GlueResource.update_table", return_value=None)
   @mock.patch("plugin.infrastructure.resource.aws.services.glue.GlueResource.update_schema_registry", return_value={"Status":"SUCCESS", "VersionNumber": 1})
   def test_glue_init_update_schema_table(self, _registry, _table):
      _schema, _schema_table = glue().update_schema_table("123456", "xxxxxx-raw-flowers", "database", "/dir/file.txt", "table", "us-east-1")
      assert _schema == {"Status":"SUCCESS", "VersionNumber": 1} 
      assert _schema_table == {"Status":"SUCCESS", "VersionNumber": 1} 
      
   @mock.patch("plugin.infrastructure.resource.aws.services.glue.GlueResource.delete_schema_version", return_value=None)
   @mock.patch("plugin.infrastructure.resource.aws.services.glue.GlueResource.update_schema_registry", return_value={"Status":"FAILURE", "VersionNumber": 2})
   def test_glue_init_failed_update_schema_table(self, _registry, _table):
      _schema, _schema_table = glue().update_schema_table("123456", "xxxxxx-raw-flowers", "database", "/dir/file.txt", "table", "us-east-1")
      assert _schema == {"Status":"FAILURE", "VersionNumber": 2} 
      assert _schema_table == {"Status":"FAILURE", "VersionNumber": 2} 