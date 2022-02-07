from abc import ABCMeta, abstractmethod


class S3Interface(metaclass=ABCMeta):
    """
    TO DO
    """

    @abstractmethod
    def folder_exists_and_not_empty(self, bucket: str, path: str) -> dict:
        raise NotImplementedError


class S3ResourceInterface(metaclass=ABCMeta):
    """
    TO DO
    """

    @abstractmethod
    def upload_object(self, path: str, bucket_name: str, package: str):
        raise NotImplementedError
