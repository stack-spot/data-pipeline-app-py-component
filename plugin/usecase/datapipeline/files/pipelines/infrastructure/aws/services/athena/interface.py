from abc import ABCMeta, abstractmethod


class AthenaInterface(metaclass=ABCMeta):
    """
    TO DO
    """
    
    @abstractmethod
    def run_query_execution(self, query: str, database: str, path_staging: str) -> dict:
        raise NotImplementedError

