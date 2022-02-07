from plugin.infrastructure.resource.interface import DataPipelineCloudInterface
from plugin.usecase.datapipeline.interface import DataPipelineInterface
from plugin.infrastructure.resource.aws.services.main import SDK
from plugin.domain.manifest import DataPipeline
from plugin import plugin_path


class DataPipelineUseCase(DataPipelineInterface):
    """
    TODO
    """
    cloud: DataPipelineCloudInterface

    def __init__(self, cloud: DataPipelineCloudInterface) -> None:
        self.cloud = cloud

    def apply(self, data_pipeline: DataPipeline):
        cloud_service = SDK()
        assets = self.cloud.create_assets(data_pipeline)
        cloud_service.upload_object(
            f"{plugin_path}/usecase/datapipeline/files/main.py",
            assets["assets_path"], "datacleansing.py")
        cloud_service.upload_object(
            f"{plugin_path}/usecase/datapipeline/files/spark-avro_2.12-3.1.1.jar",
            assets["assets_path"], "spark-avro_2.12-3.1.1.jar")
        cloud_service.upload_object(f"{plugin_path}/usecase/datapipeline/files/pipelines.zip",
                                    assets["assets_path"], "pipelines.zip")
    
        self.cloud.apply_datapipeline_stack(data_pipeline)
        return True
