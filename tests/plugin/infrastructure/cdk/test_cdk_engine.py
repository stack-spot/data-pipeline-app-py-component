import boto3
from datetime import datetime
import json
import pytest
from uuid import uuid4
from botocore.stub import Stubber
from unittest import (mock, TestCase)

from plugin.infrastructure.resource.aws.cdk.stacks import Stack
from plugin.infrastructure.resource.aws.cdk.engine.main import CDKEngine, ClientError, cdk


class CDKEngineTest(TestCase, CDKEngine):

    @mock.patch("plugin.infrastructure.resource.aws.cdk.engine.helpers.boto3", autospec=True)
    def setUp(self, mock_boto3) -> None:
        self.mock_session = mock_boto3.Session.return_value
        self.mock_cf = self.mock_session.client.return_value
        self.cf_client = boto3.client(
            "cloudformation", region_name="us-east-1")
        self.stubber = Stubber(self.cf_client)
        self.stubber.activate()
        return super().setUp()

    def mock_update_stack(self, stack_name: str, stack_template: dict):
        response = {
            'StackId': uuid4().__str__()
        }

        expected_params = {
            'StackName': stack_name,
            'TemplateBody': json.dumps(stack_template),
            'Capabilities': ['CAPABILITY_IAM', 'CAPABILITY_NAMED_IAM']
        }

        self.stubber.add_response('update_stack', response, expected_params)

        mocked_response = self.cf_client.update_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(stack_template),
            Capabilities=['CAPABILITY_IAM', 'CAPABILITY_NAMED_IAM']
        )

        return mocked_response

    def mock_create_stack(self, stack_name: str, stack_template: str):
        response = {
            'StackId': uuid4().__str__()
        }

        expected_params = {
            'StackName': stack_name,
            'TemplateBody': json.dumps(stack_template),
            'OnFailure': 'ROLLBACK',
            'Capabilities': ['CAPABILITY_IAM', 'CAPABILITY_NAMED_IAM']
        }

        self.stubber.add_response('create_stack', response, expected_params)

        mocked_response = self.cf_client.create_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(stack_template),
            OnFailure='ROLLBACK',
            Capabilities=['CAPABILITY_IAM', 'CAPABILITY_NAMED_IAM']
        )

        return mocked_response

    def mock_describe_stack_events(self, stack_name: str, stack_template: dict):

        resource_id = list(stack_template["Resources"].keys())[0]

        events_finish = {
            "StackEvents": [
                {
                    "StackId": uuid4().__str__(),
                    "EventId": "1",
                    "StackName": stack_name,
                    "LogicalResourceId": resource_id,
                    "PhysicalResourceId": resource_id,
                    "ResourceType": ".",
                    "Timestamp": datetime(2021, 1, 1),
                    "ResourceStatus": "CREATE_COMPLETE",
                    "ResourceStatusReason": "...",
                    "ResourceProperties": ".",
                    "ClientRequestToken": "."
                },
            ],
            "NextToken": "string"
        }

        self.stubber.add_response("describe_stack_events", events_finish, {
            "StackName": stack_name})

        mocked_response = self.cf_client.describe_stack_events(
            StackName=stack_name
        )

        return mocked_response

    def mock_stack_template(self):
        return {
            "Resources": {
                "dp123456789123rawelectronics": {
                    "Type": "AWS::S3::Bucket",
                    "Properties": {
                        "BucketEncryption": {
                            "ServerSideEncryptionConfiguration": [
                                  {
                                      "ServerSideEncryptionByDefault": {
                                          "SSEAlgorithm": "AES256"
                                      }
                                  }
                            ]
                        },
                        "BucketName": "123456789123-raw-electronics",
                        "LifecycleConfiguration": {
                            "Rules": [
                                {
                                      "Id": "default_trusted",
                                      "Status": "Enabled",
                                      "Transitions": [
                                          {
                                              "StorageClass": "STANDARD_IA",
                                              "TransitionInDays": 30
                                          },
                                          {
                                              "StorageClass": "GLACIER",
                                              "TransitionInDays": 60
                                          }
                                      ]
                                }
                            ]
                        },
                        "PublicAccessBlockConfiguration": {
                            "BlockPublicAcls": "true",
                            "BlockPublicPolicy": "true",
                            "IgnorePublicAcls": "true",
                            "RestrictPublicBuckets": "true"
                        }
                    },
                    "UpdateReplacePolicy": "Delete",
                    "DeletionPolicy": "Delete"
                }
            }
        }

    def get_exception_from_update_stack(self, exception: str, stack_name: str, stack_template: str):
        self.stubber.add_client_error("update_stack",
                                      service_error_code=exception,
                                      service_message=exception,
                                      http_status_code=404)
        try:
            self.cf_client.update_stack(
                StackName=stack_name,
                TemplateBody=json.dumps(stack_template),
                Capabilities=['CAPABILITY_IAM', 'CAPABILITY_NAMED_IAM']
            )
        except Exception as error:
            return error

    def get_exception_from_create_stack(self, exception: str, stack_name: str, stack_template: str):
        self.stubber.add_client_error("create_stack",
                                      service_error_code=exception,
                                      service_message=exception,
                                      http_status_code=404)
        try:
            self.cf_client.create_stack(
                StackName=stack_name,
                TemplateBody=json.dumps(stack_template),
                OnFailure='ROLLBACK',
                Capabilities=['CAPABILITY_IAM', 'CAPABILITY_NAMED_IAM']
            )
        except Exception as error:
            return error

    def get_exception_from_describe_stack_events(self, exception: str, stack_name: str):
        self.stubber.add_client_error("describe_stack_events",
                                      service_error_code=exception,
                                      service_message=exception,
                                      http_status_code=404)
        try:
            self.cf_client.describe_stack_events(
                StackName=stack_name
            )
        except Exception as error:
            return error

    def test_cdk_engine_app(self):
        cdk_engine = CDKEngine()

        assert isinstance(cdk_engine.app, cdk.App)

    def test_new_app(self):
        self.new_app()

    def test_update_stack(self):

        stack_template = self.mock_stack_template()

        my_stack = "my-stack"

        self.mock_cf.update_stack.side_effect = [
            self.mock_update_stack(
                stack_name=my_stack,
                stack_template=stack_template
            )
        ]

        self.mock_cf.describe_stack_events.side_effect = [
            self.mock_describe_stack_events(
                stack_name=my_stack,
                stack_template=stack_template
            )
        ]

        self._CDKEngine__update(
            self.mock_cf, my_stack, stack_template)

    @mock.patch("plugin.infrastructure.resource.aws.cdk.engine.helpers.is_created_resource", autospec=True)
    @mock.patch("plugin.infrastructure.resource.aws.cdk.engine.helpers.boto3", autospec=True)
    def test_deploy_stack(self, mock_boto3, mock_created_resource):

        self.new_app()
        my_stack = "my-stack"
        stack = Stack(self.app, my_stack)

        stack.create_bucket_assets(
            name="my-bucket-to-put-assets")

        self.mock_cf.create_stack.side_effect = [
            self.mock_create_stack(
                stack_name=my_stack,
                stack_template=self.mock_stack_template()
            )
        ]

        self.mock_cf.describe_stack_events.side_effect = [
            self.mock_describe_stack_events(
                stack_name=my_stack,
                stack_template=self.mock_stack_template()
            )
        ]

        mock_created_resource.side_effect = [False, True]

        self.deploy(stack_name=my_stack, region="us-east-1")

    def test_raises_error_when_updating_stack(self):

        stack_template = self.mock_stack_template()

        my_stack = "my-stack"

        for exception in [
            "InsufficientCapabilitiesException",
                "TokenAlreadyExistsException"]:

            self.mock_cf.update_stack.side_effect = [
                self.get_exception_from_update_stack(
                    exception=exception,
                    stack_name=my_stack,
                    stack_template=stack_template
                )
            ]

            self.mock_cf.describe_stack_events.side_effect = [
                self.mock_describe_stack_events(
                    stack_name=my_stack,
                    stack_template=stack_template
                )
            ]

            with pytest.raises(ClientError):
                self._CDKEngine__update(self.mock_cf, my_stack, stack_template)

    @mock.patch("plugin.infrastructure.resource.aws.cdk.engine.helpers.boto3", autospec=True)
    def test_raises_already_exists_exception_when_deploy_stack(self, mock_boto3):

        mock_session = mock_boto3.Session.return_value
        mock_cf = mock_session.client.return_value

        self.new_app()
        my_stack = "my-stack"
        stack = Stack(self.app, my_stack)

        stack.create_bucket_assets(
            name="my-bucket-to-put-assets")

        mock_cf.create_stack.side_effect = [
            self.get_exception_from_create_stack(
                exception="AlreadyExistsException",
                stack_name=my_stack,
                stack_template=self.mock_stack_template()
            )
        ]

        mock_cf.describe_stack_events.side_effect = [
            self.get_exception_from_describe_stack_events(
                exception="AlreadyExistsException",
                stack_name=my_stack
            )
        ]

        with pytest.raises(ClientError):
            self.deploy(stack_name=my_stack, region="us-east-1")

    @mock.patch("plugin.infrastructure.resource.aws.cdk.engine.helpers.boto3", autospec=True)
    def test_raises_unexpected_exception_when_deploy_stack(self, mock_boto3):

        mock_session = mock_boto3.Session.return_value
        mock_cf = mock_session.client.return_value

        self.new_app()
        my_stack = "my-stack"
        stack = Stack(self.app, my_stack)

        stack.create_bucket_assets(
            name="my-bucket-to-put-assets")

        mock_cf.create_stack.side_effect = [
            self.get_exception_from_create_stack(
                exception="UnexpectedException",
                stack_name=my_stack,
                stack_template=self.mock_stack_template()
            )
        ]

        mock_cf.describe_stack_events.side_effect = [
            self.get_exception_from_describe_stack_events(
                exception="AlreadyExistsException",
                stack_name=my_stack
            )
        ]

        with pytest.raises(ClientError):
            self.deploy(stack_name=my_stack, region="us-east-1")
