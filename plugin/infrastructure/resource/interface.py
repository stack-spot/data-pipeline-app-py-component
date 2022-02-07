from abc import ABCMeta, abstractmethod
from plugin.domain.manifest import DataPipeline


class DataPipelineCloudInterface(metaclass=ABCMeta):
    """
    A Data Pipeline stack is an infrastructure base that creates an application
    to streaming data in a structured and/or semi-structured way, this plugin
    has the following features:

        Data Flow
    """
    @abstractmethod
    def apply_datapipeline_stack(self, data_pipeline: DataPipeline):
        """
        TO DO
        """
        raise NotImplementedError

    @abstractmethod
    def create_assets(self, data_pipeline: DataPipeline) -> dict:
        """
        TO DO
        """
        raise NotImplementedError
