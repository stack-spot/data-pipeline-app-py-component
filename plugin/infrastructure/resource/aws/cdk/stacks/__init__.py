from aws_cdk import core as cdk

from .data_flow import DataFlow
from .taxonomy import Taxonomy


class Stack(DataFlow, Taxonomy):
    """
    TO DO
    Args:
        DataFlow ([type]): [description]
    """

    def __init__(self, scope: cdk.Construct,
                 construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
