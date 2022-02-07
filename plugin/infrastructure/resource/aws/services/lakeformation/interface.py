from abc import ABCMeta, abstractmethod
from plugin.domain.manifest import Database


class LakeFormationResourceInterface(metaclass=ABCMeta):
    """
    TO DO
    """

    @abstractmethod
    def add_classifications_to_resources(self, account_id: str, database: Database) -> None:
        raise NotImplementedError
