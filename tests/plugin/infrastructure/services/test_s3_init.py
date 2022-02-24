from plugin.infrastructure.resource.aws.services.s3 import S3 as s3
from unittest import (mock)

class TestS3Init:

    @mock.patch("plugin.infrastructure.resource.aws.services.s3.S3Resource.upload_object", return_value=None)
    def test_s3_init_upload_object(self, mock_s3_upload):
       s3().upload_object("/dir/file.txt", "xxxxxx-flowers-assets", "texto.txt")
    
    @mock.patch("plugin.infrastructure.resource.aws.services.s3.S3Resource.check_bucket", return_value=True)
    def test_s3_init_check_bucket(self, mock_s3_check):
       s3().check_bucket("xxxxxx-flowers-assets")
    
    @mock.patch("plugin.infrastructure.resource.aws.services.s3.S3Resource.check_bucket_object", return_value=True)
    def test_s3_init_check_bucket_object(self, mock_s3_check):
       s3().check_bucket_object("key", "xxxxxx-flowers-assets")
