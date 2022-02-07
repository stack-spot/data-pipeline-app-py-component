from .interface import S3ResourceInterface, S3Interface
from botocore.client import ClientError
import boto3


class S3(S3Interface):
    """
    TO DO

    Args:
        S3ResourceInterface ([type]): [description]
    """

    def __init__(self, **knargs) -> None:
        self.session = boto3.session.Session()
        self.s3 = self.session.client('s3', region_name=knargs.get('region', "us-east-1"))

    def folder_exists_and_not_empty(self, bucket: str, path: str) -> dict:
        if not path.endswith('/'):
            path = path + '/'
        return self.s3.list_objects(Bucket=bucket, Prefix=path, Delimiter='/', MaxKeys=1)


class S3Resource(S3ResourceInterface):
    """
    TO DO

    Args:
        S3ResourceInterface ([type]): [description]
    """

    def __init__(self):
        self.s3 = boto3.resource('s3')

    def upload_object(self, path: str, bucket_name: str, package: str):
        try:
            self.s3.meta.client.upload_file(
                path,
                bucket_name,
                package)
        except ClientError as err:
            print('exp: ', err)
