import string
import random
import pytest
from unittest import (mock, TestCase)
from plugin.usecase.datapipeline.main import DataPipelineInterface, DataPipelineUseCase, DataPipeline, plugin_path
from plugin.domain.manifest import Manifest


class MockDataPipelineUseCase(DataPipelineInterface):

    def __init__(self) -> None:
        pass

    def apply(self, data_pipeline: DataPipeline) -> None:
        return super().apply(data_pipeline)


class ApplyDataPipelineTest(TestCase):

    @staticmethod
    def __random_string(letter, size: int):
        return ''.join(random.choice(letter) for _ in range(size))

    @pytest.fixture(autouse=True)
    def plugin_manifest(self):
        self.manifest = Manifest("template/manifest.yaml")

    @mock.patch("plugin.infrastructure.resource.aws.cdk.main.AwsCdk", autospec=True)
    @mock.patch("plugin.usecase.datapipeline.main.SDK", autospec=True)
    def test_if_apply_datapipeline_works(self, mock_service, mock_aws_cdk):
        name = self.__random_string(
            letter=string.ascii_letters.lower(),
            size=18)
        self.manifest.data_pipeline.database.name = f"{name}-test"
        mock_cloud_cdk = mock_aws_cdk.return_value
        mock_cloud_service = mock_service.return_value
        assets = mock_cloud_cdk.create_assets(self.manifest.data_pipeline).return_value = {
            "assets_path": f"{name}-assets",
            "logs_path": f"{name}-logs"
        }
        mock_cloud_service.upload_object(
            f"{plugin_path}/usecase/datapipeline/files/clean.py",
            assets["assets_path"], "datacleansing.py").return_value = None
        result = DataPipelineUseCase(cloud=mock_cloud_cdk).apply(
            data_pipeline=self.manifest.data_pipeline)
        assert result == True

    def test_not_implemented_error(self):
        with pytest.raises(NotImplementedError):
            name = self.__random_string(
                letter=string.ascii_letters.lower(),
                size=18)
            self.manifest.data_pipeline.database.name = f"{name}-test"

            mock_use_case = MockDataPipelineUseCase()
            mock_use_case.apply(self.manifest.data_pipeline)
