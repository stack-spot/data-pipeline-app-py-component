from aws_cdk import core as cdk
from .helpers import get_client_cloudformation, create_stack, update_stack
from plugin.utils.logging import logger
from botocore.exceptions import ClientError


class CDKEngine():
    """
    TODO
    """
    app: cdk.App

    def __init__(self) -> None:
        self.app = cdk.App()

    def new_app(self) -> None:
        self.app = cdk.App()

    @staticmethod
    def __update(cf, stack_name: str, stack_template: str):
        try:
            update_stack(cf, stack_name, stack_template)
            
        except ClientError as e:
            logger.error("Unexpected error: %s", e)
            raise ClientError(e.response,
                              'InternalServerErrorException') from e

    def deploy(self, stack_name: str, region: str):
        stack_template = self.app.synth().get_stack_artifact(stack_name).template
        cf = get_client_cloudformation(region)
        try:
            stack_name = f"stackspot-skynet-{stack_name}"
            #print(json.dumps(stack_template, indent=2))
            create_stack(cf, stack_name, stack_template)
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                logger.warning("AlreadyExistsException: %s", stack_name)
                self.__update(cf, stack_name, stack_template)
            else:
                logger.error("Unexpected error: %s", e)
                raise ClientError(
                    e.response, 'InternalServerErrorException') from e
