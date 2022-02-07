from .service import S3Resource, S3 as S3Service


class S3:
    """
    TO DO
    """
    @staticmethod
    def folder_exists_and_not_empty(bucket: str, path: str, region: str) -> bool:
        s3 = S3Service(region=region)
        reponse = s3.folder_exists_and_not_empty(bucket, path)
        return 'Contents' in reponse

        
    @staticmethod
    def upload_object(path: str, bucket_name: str, package: str) -> None:
        s3 = S3Resource()
        s3.upload_object(path, bucket_name, package)