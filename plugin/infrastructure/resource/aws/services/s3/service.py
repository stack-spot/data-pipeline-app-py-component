from .interface import S3ResourceInterface, S3Interface
from botocore.client import ClientError
import boto3


class S3Resource(S3ResourceInterface, S3Interface):
    """
    TO DO

    Args:
        S3ResourceInterface ([type]): [description]
        S3Interface ([type]): [description]
    """

    def __init__(self):
        self.s3 = boto3.resource('s3')

    def upload_object(self, path: str, bucket_name: str, package: str):
        object_exists = self.check_bucket_object(
            object_key=package, bucket_name=bucket_name)

        if not object_exists:
            try:
                self.s3.meta.client.upload_file(
                    path,
                    bucket_name,
                    package)
            except ClientError as err:
                print('exp: ', err)

    def check_bucket(self, bucket_name: str) -> bool:
        try:
            self.s3.meta.client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as err:
            error_code = int(err.response['Error']['Code'])
            if error_code == 403:
                print(f"Access to Private Bucket {bucket_name} is Forbidden!")
                return True
            if error_code == 404:
                print(f"Bucket {bucket_name} Does Not Exist!")
                return False
            return False

    def check_bucket_object(self, object_key: str, bucket_name: str) -> bool:
        try:
            self.s3.meta.client.head_object(Key=object_key, Bucket=bucket_name)
            return True
        except ClientError as err:
            error_code = int(err.response['Error']['Code'])
            if error_code == 403:
                print(f"Access to Private Bucket {bucket_name} is Forbidden!")
                return True
            if error_code == 404:
                return False
            return False
