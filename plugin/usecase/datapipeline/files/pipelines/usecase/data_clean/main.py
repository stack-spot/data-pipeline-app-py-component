from pipelines.utils.logging import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_unixtime, lit


logger = logger(__name__)


class DataClean:
    """
    TODO
    """

    def __init__(self, dataframe: DataFrame) -> None:
        logger.info('Data Clean | __init__ ')
        self.df = dataframe
        self.checkpoint # pylint: disable=pointless-statement


    @property
    def rollback(self):
        self.df = self.dfb
        logger.info('Data Pipeline | Rollback | Success')

    @property
    def checkpoint(self):
        self.dfb = self.df # pylint: disable=attribute-defined-outside-init
        logger.info('Data Pipeline | Checkpoint | Success')

    @property
    def return_dataframe(self) -> DataFrame:
        return self.df

    def df_drop_duplicates(self, drop_cols: list):
        logger.info(
            f'Data Pipeline | Drop Duplicates | Remove duplicates referring to keys {drop_cols}')
        self.df = self.df.dropDuplicates(drop_cols)
        logger.info('Data Pipeline | Drop Duplicates | Success')

    def df_add_date_partition(self, version: int):
        logger.info(
            'Data Pipeline | Add Partition | Add Patition Key _version and _date')
        self.df = self.df.withColumn(
            "_date", from_unixtime(col("event_time")/1000, "yyyy-MM-dd"))
        self.df = self.df.withColumn("_version", lit(version))
        logger.info('Data Pipeline | Add Partition | Success')
