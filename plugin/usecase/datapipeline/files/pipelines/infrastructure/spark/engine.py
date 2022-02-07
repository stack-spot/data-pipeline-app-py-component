from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession


class SparkPipelineEngine:
    """
    TODO
    """

    def __init__(self, app_name: str):
        self._conf = SparkConf().set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        self._context = SparkContext(conf=self._conf)
        self._session = SparkSession.builder.appName(app_name).getOrCreate()
