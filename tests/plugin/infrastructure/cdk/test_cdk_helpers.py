import string, json
import random
import pytest
from aws_cdk import (core as cdk, assertions)
from unittest import (mock, TestCase)
from plugin.domain.manifest import Manifest
from plugin.infrastructure.resource.aws.cdk.stacks.helpers import get_resource


class Stack(cdk.Stack):
    
    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
    def return_obj(self):
        return self


class TestCDKHelpers:

    @pytest.fixture(autouse=True)
    def _cdk_app(self):
        self.app = cdk.App()
        self.stack_name = f"create-datapipeline-stack-{self.__random_string(letter=string.ascii_letters,size=10)}"
        self.cdk = Stack(self.app, self.stack_name).return_obj()

    @staticmethod
    def __random_string(letter, size: int):
        return ''.join(random.choice(letter) for _ in range(size))

    @pytest.mark.parametrize("input", ["bucket", "storage", "datalake"])
    def test_get_name_bucket_raw(self, input):
        account = cdk.Stack.of(self.cdk).account
        partition = cdk.Stack.of(self.cdk).partition
        s3 = get_resource.get_bucket_raw(cdk, self.cdk, "import-bucket-rw", input)
        assert s3.bucket_name == f"{account}-raw-{input}"
        assert s3.bucket_arn == f"arn:{partition}:s3:::{account}-raw-{input}"
    
    @pytest.mark.parametrize("input", ["stream", "queue", "message"])
    def test_get_name_kinesis_stream(self, input):
        account = cdk.Stack.of(self.cdk).account
        region = cdk.Stack.of(self.cdk).region
        kinesis = get_resource.get_kinesis_stream(cdk, self.cdk, "import-kinesis-stream", input)
        assert kinesis.stream_name == f"{input}-*"
        assert kinesis.stream_arn == f"arn:aws:kinesis:{region}:{account}:stream/{input}-*"
        

    @pytest.mark.parametrize("datalake,schema", [("datalake","schema"), ("data_product","schema")])
    def test_get_name_firehose_delivery(self, datalake, schema):
        account = cdk.Stack.of(self.cdk).account
        region = cdk.Stack.of(self.cdk).region
        partition = cdk.Stack.of(self.cdk).partition
        firehose = get_resource.get_firehose(self.cdk, "import-kinesis-delivery",  datalake, schema)
        assert firehose.delivery_stream_name == f"{datalake}-{schema}-delivery"
        assert firehose.delivery_stream_arn == f"arn:{partition}:firehose:{region}:{account}:deliverystream/{datalake}-{schema}-delivery"