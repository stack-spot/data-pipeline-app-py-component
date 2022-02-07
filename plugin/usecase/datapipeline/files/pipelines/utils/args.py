from awsglue.utils import getResolvedOptions
import sys


class SparkArgs:
    """
    TODO
    """

    def __init__(self, list_args: list):
        self.__dict__.update(getResolvedOptions(sys.argv, list_args))
