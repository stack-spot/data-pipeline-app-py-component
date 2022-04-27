from abc import ABCMeta, abstractmethod


class GlueResourceInterface(metaclass=ABCMeta):
    """
    TO DO
    """

    @abstractmethod
    def check_schema_version(self, registry_name: str, schema_name: str) -> bool:
        raise NotImplementedError
