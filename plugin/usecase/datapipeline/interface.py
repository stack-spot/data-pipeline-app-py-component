from abc import ABCMeta, abstractmethod
from plugin.domain.manifest import DataPipeline


class DataPipelineInterface(metaclass=ABCMeta):
    """
    A Data Pipeline stack is an infrastructure base that creates an application
    to stream data in a structured and/or semi-structured way, this plugin
    has the following features:

        Data Flow
    """
    @abstractmethod
    def apply(self, data_pipeline: DataPipeline):
        raise NotImplementedError
