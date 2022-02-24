from botocore.stub import Stubber
from botocore.session import Session
import pytest
import boto3
from plugin.infrastructure.resource.aws.services.s3.service import S3Resource
from unittest import (mock)


class TestServiceS3:

    @pytest.mark.parametrize("key,bucket_name", [("key","bucket"), ("flower","xxx-raw-flower")])
    def test_s3_check_bucket_object(self, key, bucket_name):     
        s3 = boto3.resource('s3')
        with mock.patch("plugin.infrastructure.resource.aws.services.s3.service.boto3.resource") as mock_client:
            mock_client.return_value = s3
            stubber = Stubber(s3.meta.client)
            expected_params = {
            'Bucket': bucket_name,
            'Key': key,
            }

            response = {
                'ContentLength': 10,
                'ContentType': 'utf-8',
                'ResponseMetadata': {
                    'Bucket': bucket_name,
                }
            }
            stubber.add_response('head_object', response, expected_params)
            stubber.activate()
            rep = S3Resource().check_bucket_object(key, bucket_name)
            stubber.deactivate()
            assert rep == True
            
    def test_s3_client_error_403_check_bucket_object(self):     
        s3 = boto3.resource('s3')
        with mock.patch("plugin.infrastructure.resource.aws.services.s3.service.boto3.resource") as mock_client:
            mock_client.return_value = s3
            stubber = Stubber(s3.meta.client)
            stubber.add_client_error("head_object",
                                 service_error_code=403,
                                 service_message="Forbidden",
                                 http_status_code=403)
            stubber.activate()
            rep = S3Resource().check_bucket_object("error", "bucker_error")
            stubber.deactivate()
            assert rep == True

    def test_s3_client_error_404_check_bucket_object(self):     
        s3 = boto3.resource('s3')
        with mock.patch("plugin.infrastructure.resource.aws.services.s3.service.boto3.resource") as mock_client:
            mock_client.return_value = s3
            stubber = Stubber(s3.meta.client)
            stubber.add_client_error("head_object",
                                 service_error_code=404,
                                 service_message="Forbidden",
                                 http_status_code=404)
            stubber.activate()
            rep = S3Resource().check_bucket_object("error", "bucker_error")
            stubber.deactivate()
            assert rep == False
    
    def test_s3_client_error_check_bucket_object(self):     
        s3 = boto3.resource('s3')
        with mock.patch("plugin.infrastructure.resource.aws.services.s3.service.boto3.resource") as mock_client:
            mock_client.return_value = s3
            stubber = Stubber(s3.meta.client)
            stubber.add_client_error("head_object",
                                 service_error_code=500,
                                 service_message="Forbidden",
                                 http_status_code=500)
            stubber.activate()
            rep = S3Resource().check_bucket_object("error", "bucker_error")
            stubber.deactivate()
            assert rep == False
        
    @pytest.mark.parametrize("bucket_name", ["bucket", "xxx-raw-flower"])
    def test_s3_check_bucket(self, bucket_name):     
        s3 = boto3.resource('s3')
        with mock.patch("plugin.infrastructure.resource.aws.services.s3.service.boto3.resource") as mock_client:
            mock_client.return_value = s3
            stubber = Stubber(s3.meta.client)
            expected_params = {
            'Bucket': bucket_name
            }
            stubber.add_response('head_bucket', {}, expected_params)
            stubber.activate()
            rep = S3Resource().check_bucket(bucket_name)
            stubber.deactivate()
            assert rep == True
            
    def test_s3_client_error_403_check_bucket(self):     
        s3 = boto3.resource('s3')
        with mock.patch("plugin.infrastructure.resource.aws.services.s3.service.boto3.resource") as mock_client:
            mock_client.return_value = s3
            stubber = Stubber(s3.meta.client)
            stubber.add_client_error("head_bucket",
                                 service_error_code=403,
                                 service_message="Forbidden",
                                 http_status_code=403)
            stubber.activate()
            rep = S3Resource().check_bucket("bucket_error")
            stubber.deactivate()
            assert rep == True
    
    def test_s3_client_error_404_check_bucket(self):     
        s3 = boto3.resource('s3')
        with mock.patch("plugin.infrastructure.resource.aws.services.s3.service.boto3.resource") as mock_client:
            mock_client.return_value = s3
            stubber = Stubber(s3.meta.client)
            stubber.add_client_error("head_bucket",
                                 service_error_code=404,
                                 service_message="Forbidden",
                                 http_status_code=404)
            stubber.activate()
            rep = S3Resource().check_bucket("bucket_error")
            stubber.deactivate()
            assert rep == False
    
    def test_s3_client_error_check_bucket(self):     
        s3 = boto3.resource('s3')
        with mock.patch("plugin.infrastructure.resource.aws.services.s3.service.boto3.resource") as mock_client:
            mock_client.return_value = s3
            stubber = Stubber(s3.meta.client)
            stubber.add_client_error("head_bucket",
                                 service_error_code=500,
                                 service_message="Forbidden",
                                 http_status_code=500)
            stubber.activate()
            rep = S3Resource().check_bucket("bucket_error")
            stubber.deactivate()
            assert rep == False
            
    def test_s3_upload_object(self):     
        with mock.patch("plugin.infrastructure.resource.aws.services.s3.service.S3Resource.check_bucket_object") as mock_check_bucket_object:
            mock_check_bucket_object.return_value = True
            S3Resource().upload_object("/dir/file.txt", "xxxxxx-flowers-assets", "texto.txt")
    
    # def test_s3_not_exist_object(self):
    #     with mock.patch("plugin.infrastructure.resource.aws.services.s3.service.S3Resource.check_bucket_object") as mock_check_bucket_object:
    #         mock_check_bucket_object.return_value = False
    #         with mock.patch("plugin.infrastructure.resource.aws.services.s3.service.boto3.meta.client.upload_file") as mock_client:
    #             mock_client.return_value = None
    #             S3Resource().upload_object("/dir/file.txt", "xxxxxx-flowers-assets", "texto.txt")
