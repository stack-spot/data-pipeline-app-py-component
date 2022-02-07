from plugin.domain.manifest import DataPipeline, Table
from plugin.infrastructure.resource.aws.cdk.stacks import Stack
from plugin.infrastructure.resource.aws.services.main import SDK
from plugin.utils.string import get_bucket_name_by_arn
from plugin.utils.logging import logger


def create_dataschema_pipeline(scope, table: Table, data_pipeline: DataPipeline):
    scope.new_app()
    stack_name = f'create-{data_pipeline.database.name}-{table.name}-pipeline'.replace(
        '_', '-')
    stack = Stack(scope.app, stack_name)
    database_name = data_pipeline.database.name

    stack_schema_registry = stack.create_schema_registry(
        name=database_name,
        path=data_pipeline.database.schemas.path,
        table_name=table.name)

    stack_schema_registry_table = stack.create_schema_registry_table(
        name=database_name,
        path=data_pipeline.database.schemas.path,
        table_name=table.name)

    bucket_target = get_bucket_name_by_arn(data_pipeline.arn_bucket_target)
    bucket_source = get_bucket_name_by_arn(data_pipeline.arn_bucket_source)
    stack_kinesis = stack.create_kinesis(
        f"{data_pipeline.database.name}-{table.name}-kinesis", table.name)

    stack.create_role_job_glue(data_pipeline, table.name)

    firehose_role_policy = stack.create_firehose_role_policy(
        database_name, table.name)
    firehose_role_policy.add_depends_on(stack_kinesis)

    stack_firehose = stack.create_kinesis_delivery_stream(
        database_name, data_pipeline.arn_bucket_source, table.name)
    stack_firehose.add_depends_on(firehose_role_policy)

    stack_table = stack.create_database_table(data_pipeline, table)
    stack_table.add_depends_on(stack_schema_registry)
    stack_table.add_depends_on(stack_schema_registry_table)
    stack_table.add_depends_on(stack_firehose)

    stack_glue = stack.create_glue_jobs(
        bucket_source, bucket_target, database_name, table.name)
    stack_glue.add_depends_on(stack_firehose)

    stack_trigger = stack.create_glue_trigger(database_name, table.name)
    stack_trigger.add_depends_on(stack_glue)

    scope.deploy(stack_name, data_pipeline.region)
    print(f"{stack_name} deployed")


def update_dataschema_pipeline(table: Table, data_pipeline: DataPipeline):
    cloud_service = SDK()
    bucket_name = get_bucket_name_by_arn(data_pipeline.arn_bucket_target)
    database_name = data_pipeline.database.name
    region = data_pipeline.region

    _schema, _schema_table = cloud_service.update_schema_table(
        cloud_service.account_id,
        bucket_name,
        database_name,
        data_pipeline.database.schemas.path,
        table.name,
        region)

    if "VersionNumber" in _schema and "VersionNumber" in _schema_table:
        logger.info(
            "Schema Version: '%s', Table Version: '%s'",
            _schema['VersionNumber'],
            _schema_table['VersionNumber']
        )


def get_index_by_name(source: list[str], name: str):
    index_by_name = next(
        (index for (index, l) in enumerate(source) if l == name), None)
    return index_by_name


def contains_schema(schemas: list, schema_name: str):
    index = get_index_by_name(source=schemas, name=schema_name)
    return False if index is None else bool(index+1)


def create_dataschema_registry_database(scope, data_pipeline: DataPipeline):
    scope.new_app()
    cloud_service = SDK()
    stack_name = f'create-{data_pipeline.database.name}-database-registry'.replace(
        '_', '-')
    stack = Stack(scope.app, stack_name)

    database_exists = cloud_service.check_database_exists(
        catalog_id=cloud_service.account_id,
        database_name=data_pipeline.database.name,
        region=data_pipeline.region
    )

    registry_exists = cloud_service.check_registry_exists(
        registry_name=data_pipeline.database.name,
        region=data_pipeline.region
    )

    if not database_exists:
        logger.info(
            "Database '%s' does not exists. Creating...", data_pipeline.database.name)
        stack.create_database(data_pipeline.database.name)
    else:
        logger.info("Database '%s' already exists.",
                    data_pipeline.database.name)

    if not registry_exists:
        logger.info(
            "Registry '%s' does not exists. Creating...", data_pipeline.database.name)
        stack.create_registry(data_pipeline.database.name)
    else:
        logger.info("Registry '%s' already exists.",
                    data_pipeline.database.name)

    if not database_exists or not registry_exists:
        scope.deploy(stack_name, data_pipeline.region)
        print(f"{stack_name} deployed")


def add_classifications(scope, data_pipeline: DataPipeline):
    scope.new_app()
    cloud_service = SDK()
    stack_name = f'add-{data_pipeline.database.name}-taxonomy'.replace(
        '_', '-')
    stack = Stack(scope.app, stack_name)

    stack.add_classifications_to_resources(cloud_service,
                                           data_pipeline.database,
                                           data_pipeline.region)

    # self.deploy(stack_name, data_pipeline.region)
    print(f"{stack_name} deployed")
