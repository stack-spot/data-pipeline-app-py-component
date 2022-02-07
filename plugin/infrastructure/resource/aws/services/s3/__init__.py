from .service import S3Resource


class S3:
    """
    TO DO
    """
    @staticmethod
    def upload_object(path: str, bucket_name: str, package: str) -> None:
        s3 = S3Resource()
        s3.upload_object(path, bucket_name, package)

    @staticmethod
    def check_bucket(bucket_name: str) -> bool:
        s3 = S3Resource()
        return s3.check_bucket(bucket_name=bucket_name)

    @staticmethod
    def check_bucket_object(object_key: str, bucket_name: str) -> bool:
        s3 = S3Resource()
        return s3.check_bucket_object(object_key, bucket_name)
