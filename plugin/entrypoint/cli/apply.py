import click
from plugin.usecase.datapipeline.main import DataPipelineUseCase
from plugin.infrastructure.resource.aws.cdk.main import AwsCdk
from plugin.domain.manifest import Manifest


@click.group()
def apply():
    pass # We just need a click.group to create our command


@apply.command('data-pipeline')
@click.option('-f', '--file', 'path')
def apply_datapipeline(path: str):
    manifest = Manifest(manifest=path)
    DataPipelineUseCase(AwsCdk()).apply(manifest.data_pipeline)
