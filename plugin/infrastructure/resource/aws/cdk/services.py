from plugin.domain.manifest import DataPipeline, Table, TableTrigger
from plugin.infrastructure.resource.aws.cdk.stacks import Stack
from plugin.infrastructure.resource.aws.services.main import SDK
from plugin.utils.string import get_bucket_name_by_arn, kebab, snake_case
from plugin.utils.logging import logger


def create_dataschema_pipeline(scope, table: Table, data_pipeline: DataPipeline, user_arn: str):
    scope.new_app()
    s_database_name = snake_case(data_pipeline.database.name)
    s_table_name = snake_case(table.name)
    k_database_name = kebab(data_pipeline.database.name)
    k_table_name = kebab(table.name)
    stack_name = f"create-{k_database_name}-{k_table_name}-pipeline"
    stack = Stack(scope.app, stack_name)
    trigger = TableTrigger(
        type='ON_DEMAND'
    ) if data_pipeline.database.trigger is None else data_pipeline.database.trigger

    
    kms = stack.create_kms(f"data-flow-kms-{table.name}", user_arn)

    stack_schema_registry = stack.create_schema(
        name=s_database_name,
        table=table
    )

    bucket_target = get_bucket_name_by_arn(data_pipeline.arn_bucket_target)
    bucket_source = get_bucket_name_by_arn(data_pipeline.arn_bucket_source)
    stack_kinesis = stack.create_kinesis(
        f"{k_database_name}-{k_table_name}-kinesis", s_table_name)

    stack.create_role_job_glue(data_pipeline, s_table_name)

    firehose_role_policy = stack.create_firehose_role_policy(
        k_database_name, k_table_name)
    firehose_role_policy.add_depends_on(stack_kinesis)

    stack_firehose = stack.create_kinesis_delivery_stream(
        k_database_name, data_pipeline.arn_bucket_source, k_table_name, kms)
    stack_firehose.add_depends_on(firehose_role_policy)

    stack_table = stack.create_database_table(data_pipeline, table)
    stack_table.add_depends_on(stack_schema_registry)
    stack_table.add_depends_on(stack_firehose)

    stack_glue = stack.create_glue_jobs(
        bucket_source, bucket_target, k_database_name, k_table_name)
    stack_glue.add_depends_on(stack_firehose)

    stack_trigger = stack.create_glue_trigger(
        k_database_name, k_table_name, trigger)
    stack_trigger.add_depends_on(stack_glue)

    scope.deploy(stack_name, data_pipeline.region)
    print(f"{stack_name} deployed")


def update_dataschema_pipeline(table: Table, data_pipeline: DataPipeline):
    cloud_service = SDK()
    bucket_name = get_bucket_name_by_arn(data_pipeline.arn_bucket_target)
    s_database_name = snake_case(data_pipeline.database.name)
    region = data_pipeline.region

    _schema = cloud_service.update_schema(
        cloud_service.account_id,
        bucket_name,
        s_database_name,
        table,
        region
    )

    if "VersionNumber" in _schema:
        logger.info(
            "Schema Version: '%s'",
            _schema['VersionNumber']
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
    s_database_name = snake_case(data_pipeline.database.name)
    stack_name = kebab(f"create-{s_database_name}-database-registry")
    stack = Stack(scope.app, stack_name)

    database_exists = cloud_service.check_database_exists(
        catalog_id=cloud_service.account_id,
        database_name=s_database_name,
        region=data_pipeline.region
    )

    registry_exists = cloud_service.check_registry_exists(
        registry_name=s_database_name,
        region=data_pipeline.region
    )

    if not database_exists:
        logger.info(
            "Database '%s' does not exists. Creating...", s_database_name)
        stack.create_database(s_database_name)
    else:
        logger.info("Database '%s' already exists.",
                    s_database_name)

    if not registry_exists:
        logger.info(
            "Schema registry '%s' does not exists. Creating...", s_database_name)
        stack.create_registry(s_database_name)
    else:
        logger.info("Schema registry '%s' already exists.", s_database_name)

    if not database_exists or not registry_exists:
        scope.deploy(stack_name, data_pipeline.region)
        print(f"{stack_name} deployed")


def add_classifications(scope, data_pipeline: DataPipeline):
    scope.new_app()
    cloud_service = SDK()
    stack_name = kebab(f"add-{data_pipeline.database.name}-taxonomy")
    stack = Stack(scope.app, stack_name)

    stack.add_classifications_to_resources(cloud_service,
                                           data_pipeline.database,
                                           data_pipeline.region)

    # self.deploy(stack_name, data_pipeline.region)
    print(f"{stack_name} deployed")
