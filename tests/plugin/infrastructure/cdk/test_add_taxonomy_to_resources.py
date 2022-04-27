import string
import random
import pytest
from aws_cdk import (core, assertions)
from unittest import (mock, TestCase)
from plugin.domain.manifest import Manifest
from plugin.infrastructure.resource.aws.cdk.stacks.taxonomy import Taxonomy


class AddTaxonomy(TestCase):

    @pytest.fixture(autouse=True)
    def cdk_app(self):
        self.app = core.App()
        self.stack_name = f"create-datapipeline-stack-{self.__random_string(letter=string.ascii_letters,size=10)}"
        self.stack = Taxonomy(self.app, self.stack_name)

    @staticmethod
    def __random_string(letter, size: int):
        return ''.join(random.choice(letter) for _ in range(size))

    @pytest.fixture(autouse=True)
    def plugin_manifest(self):
        self.manifest = Manifest({
            "data-pipeline": {
                "region": "us-east-1",
                "arn_bucket_source": "arn:aws:s3:::328531315820-raw-flowers",
                "arn_bucket_target": "arn:aws:s3:::328531315820-clean-flowers",
                "database": {
                    "name": "flowers",
                    "trigger":{
                      "type": "ON_DEMAND"
                    },
                    "schemas": {
                        "path": "./template",
                        "tables": []
                    }
                }
            }
        })

    @mock.patch('plugin.infrastructure.resource.aws.cdk.stacks.taxonomy.SDK', autospec=True)
    def test_if_add_taxonomy_works(self, mock_cloud_service):
        name = self.__random_string(
            letter=string.ascii_letters.lower(),
            size=18)
        self.manifest.data_pipeline.database.name = f"{name}-test"

        mock_cloud_service.account_id = "123456789"

        self.stack.add_classifications_to_resources(
            mock_cloud_service, self.manifest.data_pipeline.database, self.manifest.data_pipeline.region)
