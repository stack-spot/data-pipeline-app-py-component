import boto3
import sys
from botocore.exceptions import UnauthorizedSSOTokenError
from botocore.client import BaseClient, ClientError
from pipelines.utils.logging import logger
from .s3 import S3
from .glue import Glue
from .athena import Athena

logger = logger(__name__)


class SDK(S3, Glue, Athena):
    """
    TODO
    """
    client: BaseClient

    @property
    def account_id(self):
        try:
            client = boto3.client('sts')
            return client.get_caller_identity().get('Account')
        except UnauthorizedSSOTokenError as sso_error:
            logger.error(sso_error.fmt)
            sys.exit(1)
        except ClientError as err:
            logger.error(err)
            sys.exit(1)
