from pipelines.utils.logging import logger
from pipelines.infrastructure.spark.engine import SparkPipelineEngine
from pipelines.infrastructure.aws.services.main import SDK
from pipelines.domain.exceptions import DataframesNotFound, DataframesCreateFailed
from pyspark.sql import DataFrame
from datetime import date, timedelta
import json

logger = logger(__name__)


class DataPipeline:
    """
    TODO
    """

    def __init__(self, app_name: str) -> None:
        logger.info('Data Pipeline | __init__')
        self.spark = SparkPipelineEngine(app_name)

    def __read_s3_create_list_df(self, bucket_source: str, path: str, schema: dict, version: int, dfs: list, region: str) -> list:
        cloud_service = SDK()
        if cloud_service.folder_exists_and_not_empty(bucket_source, path, region):
            try:
                logger.info(
                    f'Data Pipeline | Create Dataframe | Found files in {bucket_source} from path {path}')
                logger.info(
                    f'Data Pipeline | Create Dataframe | Creating Datafram from Schema Avro Version {version}v')
                # pylint: disable=protected-access
                dfs.append(self.spark._session.read.schema(schema).option("multiLine", "false").option("mode", "PERMISSIVE").json(
                    f's3://{bucket_source}/{path}/*'))
                return dfs
            except Exception as err:  # pylint: disable=broad-except
                DataframesCreateFailed(
                    logger, f'Dataframe Creation Failed Using S3 path: {bucket_source} Error: {err}')
        else:
            logger.warning(
                f'Data Pipeline | Create Dataframe | Dont Found files in {bucket_source} from path {path}')
            return dfs
        return []

    def create_dataframe_from_bucket_and_registry(self, bucket_source: str, schema_name: str, registry_name: str, date_time: date, region: str) -> DataFrame:
        cloud_service = SDK()
        dfs = []
        date_time = date_time.strftime("%Y/%m/%d")
        date_time_yesterday = (
            date.today() - timedelta(days=1)).strftime("%Y/%m/%d")
        logger.info(
            f'Data Pipeline | Create Dataframe | Look up schema {schema_name}')
        logger.info(
            f'Data Pipeline | Create Dataframe | Searching in the registry {registry_name}')
        for version in cloud_service.filter_list_schema_version(schema_name, registry_name, region):
            dfs_version = []
            logger.info(
                f'Data Pipeline | Create Dataframe | Found the schema {schema_name} in version {version}v')
            schema_registry = json.loads(cloud_service.get_schema_version(
                schema_name, registry_name, version, region))
            logger.info(
                'Data Pipeline | Create Dataframe | Creating the schema for Event')
            event_schema_avro = {
                "name": "EventSchema",
                "type": "record",
                "namespace": "com.acme.avro",
                "fields": [
                    {
                        "name": "event_data",
                        "type": {
                            "name": "event_data",
                            "type": "record",
                            "fields": schema_registry['fields']
                        }
                    },
                    {
                        "name": "event_time",
                        "type": ["null", "long"],
                        "default": None
                    },
                    {
                        "name": "event_id",
                        "type": "int"
                    },
                    {
                        "name": "schema_version",
                        "type": "string"
                    },
                    {
                        "name": "schema_name",
                        "type": "string"
                    },
                    {
                        "name": "data_product",
                        "type": "string"
                    }
                ]
            }
            logger.info(
                f'Data Pipeline | Create Dataframe | Checking if the folder exists and if its not empty in {bucket_source}')
            logger.info(
                f'Data Pipeline | Create Dataframe | Create Schema from Avro Version {version}v')
            # pylint: disable=protected-access
            df_avro = self.spark._session.read.format("avro").option(
                "avroSchema", json.dumps(event_schema_avro)).load()
            dfs_version = self.__read_s3_create_list_df(
                bucket_source, f'{registry_name}/{schema_name}/{version}/{date_time_yesterday}', df_avro.schema, version, dfs_version, region)
            dfs_version = self.__read_s3_create_list_df(
                bucket_source, f'{registry_name}/{schema_name}/{version}/{date_time}', df_avro.schema, version, dfs_version, region)
            if not dfs_version:
                logger.warning(
                    f'Data Pipeline | Create Dataframe | Dont Found files in {bucket_source}/{registry_name}/{schema_name}/{version}/[{date_time_yesterday}, {date_time}]')
                continue
            logger.info(
                'Data Pipeline | Create Dataframe | Create Dataframe Empty')
            df = self.spark._session.createDataFrame([], df_avro.schema)
            df = df.select("event_id", "event_time", "event_data.*")
            logger.info(
                'Data Pipeline | Create Dataframe | Unifying the Dataframes for the latest Schema version')
            for dataframes in dfs_version:
                dataframes = dataframes.select(
                    "event_id", "event_time", "event_data.*")
                df = df.unionByName(dataframes, allowMissingColumns=True)
            dfs.append({
                'SchemaVersion': version,
                'DataFrame': df
            })

        if not dfs:
            raise DataframesNotFound(
                logger,
                f'Dont Found files in {bucket_source}/{registry_name}/{schema_name}/*/[{date_time_yesterday}, {date_time}] and because of that it was not possible to create Dataframe'
            )
        logger.info('Data Pipeline | Create Dataframe | Success')
        return dfs

    @staticmethod
    def write_dataframe_by_patition_key(dataframe: DataFrame, bucket_target: str, path: str, *col):
        logger.info(
            f'Data Pipeline | Write by Partition | Performing writing using partition key {col} in format Parquet')
        dataframe.coalesce(10).write.partitionBy(*col).mode(
            "overwrite").parquet(f's3://{bucket_target}/{path}')
        logger.info('Data Pipeline | Write by Partition | Success')

    @staticmethod
    def execution_query_repair_metadata(query: str, database: str, path_staging: str,  region: str):
        cloud_service = SDK()
        logger.info(
            'Data Pipeline | Execution Query in Database | Performing repair of metadata in database with partition keys')
        cloud_service.run_query_execution(
            query, database, path_staging, region)
        logger.info('Data Pipeline | Execution Query in Database | Success')
