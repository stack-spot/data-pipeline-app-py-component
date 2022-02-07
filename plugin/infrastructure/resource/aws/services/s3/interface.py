from abc import ABCMeta, abstractmethod


class S3Interface(metaclass=ABCMeta):
    """
    TO DO
    """
    @abstractmethod
    def check_bucket(self, bucket_name: str) -> bool:
        raise NotImplementedError


class S3ResourceInterface(metaclass=ABCMeta):
    """
    TO DO
    """

    @abstractmethod
    def upload_object(self, path: str, bucket_name: str, package: str):
        raise NotImplementedError

    @abstractmethod
    def check_bucket_object(self, object_key: str, bucket_name: str) -> bool:
        raise NotImplementedError
