from plugin.domain.manifest import DataPipeline
from plugin.utils.string import kebab, snake_case
from .stacks import Stack
from .services import (
    add_classifications,
    contains_schema,
    create_dataschema_pipeline,
    update_dataschema_pipeline,
    create_dataschema_registry_database)
from .engine.main import CDKEngine
from plugin.utils.logging import logger
from plugin.infrastructure.resource.aws.services.main import SDK
from plugin.infrastructure.resource.interface import DataPipelineCloudInterface


class AwsCdk(CDKEngine, DataPipelineCloudInterface):
    """
    TODO
    """

    def apply_datapipeline_stack(self, data_pipeline: DataPipeline):
        cloud_service = SDK()
        user_arn = cloud_service.user_arn

        create_dataschema_registry_database(self, data_pipeline)
        s_registry_name = snake_case(data_pipeline.database.name)

        schemas_to_create = [
            _table.name if not cloud_service.check_schema_version(
                registry_name=s_registry_name,
                schema_name=_table.name,
                region=data_pipeline.region
            ) else None for _table in data_pipeline.database.schemas.tables
        ]


        for _table in data_pipeline.database.schemas.tables:  # type: ignore

            if contains_schema(schemas_to_create, _table.name):
                logger.info("Creating Schema '%s'.", _table.name)
                create_dataschema_pipeline(self, _table, data_pipeline, user_arn)

            else:
                logger.info("Checking evolution of Schema '%s'.", _table.name)
                update_dataschema_pipeline(_table, data_pipeline)

        add_classifications(self, data_pipeline)

    def create_assets(self, data_pipeline: DataPipeline):
        self.new_app()
        cloud_service = SDK()
        user_arn = cloud_service.user_arn
        kname = kebab(data_pipeline.database.name)
        name = f"{cloud_service.account_id}-{kname}-data-schema"
        stack_name = kebab(f"create-{kname}-data-schema-datapipeline-assets")
        stack = Stack(self.app, stack_name)


        bucket_logs_exist = cloud_service.check_bucket(f"{name}-logs")
        bucket_assets_exist = cloud_service.check_bucket(f"{name}-assets")

        kms = stack.create_kms(f"data-flow-kms-{name}", user_arn)

        if not bucket_logs_exist:
            stack.create_bucket_assets(f"{name}-logs", kms)
        else:
            logger.info("Bucket %s-logs already exists.", name)

        if not bucket_assets_exist:
            stack.create_bucket_assets(f"{name}-assets", kms)
        else:
            logger.info("Bucket %s-assets already exists.", name)

        if not bucket_logs_exist or not bucket_assets_exist:
            self.deploy(stack_name, data_pipeline.region)
        print(f"{stack_name} deployed")
        return {
            "assets_path": f"{name}-assets",
            "logs_path": f"{name}-logs"
        }
