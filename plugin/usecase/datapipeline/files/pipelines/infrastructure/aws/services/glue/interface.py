from abc import ABCMeta, abstractmethod


class GlueInterface(metaclass=ABCMeta):
    """
    TO DO
    """
    
    @abstractmethod
    def list_schema_version(self, schema: str, registry: str) -> dict:
        raise NotImplementedError

    @abstractmethod
    def get_schema_version(self, schema: str, registry: str, version: int) -> dict:
        raise NotImplementedError