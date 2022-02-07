import warnings
from datetime import date
from pyspark.sql import DataFrame
from pipelines.utils.args import SparkArgs
from pipelines.utils.logging import logger
from pipelines.usecase.data_pipeline.main import DataPipeline
from pipelines.usecase.data_clean.main import DataClean

logger = logger(__name__)


def main() -> None:
    """Dedicated function manages data pipeline execution."""
    logger.info('Main | Starting Job')

    args = SparkArgs(['name', 'schema', 'database', 'bucket_source',
                     'bucket_target', 'bucket_staging', 'region'])

    dp = DataPipeline(app_name='DataCleanPipeline')

    dfs = dp.create_dataframe_from_bucket_and_registry(
        bucket_source=args.bucket_source,
        schema_name=args.schema,
        registry_name=args.database,
        date_time=date.today(),
        region=args.region)
    versions = []
    for dataframe in dfs:
        df = dataframe['DataFrame']
        version = dataframe['SchemaVersion']

        df = clean(df, version)

        dp.write_dataframe_by_patition_key(
            df, args.bucket_target, f'{args.database}/{args.schema}/', '_version', '_date')
        versions.append(version)

    logger.info(
        f'Main | Processing finished and written to storage {args.bucket_source}  Versions: {versions}')

    dp.execution_query_repair_metadata(
        f'MSCK REPAIR TABLE {args.schema}', args.database, args.bucket_staging,  args.region)


def clean(datafram: DataFrame, version: int):
    """Dedicated function to apply data cleaning rules."""

    df_clean = DataClean(datafram)
    df_clean.df_drop_duplicates(["event_time", "event_id"])
    df_clean.df_add_date_partition(version)
    df = df_clean.return_dataframe
    return df


if __name__ == '__main__':
    warnings.filterwarnings("ignore", message="numpy.dtype size changed")
    warnings.filterwarnings("ignore", message="numpy.ufunc size changed")
    main()
