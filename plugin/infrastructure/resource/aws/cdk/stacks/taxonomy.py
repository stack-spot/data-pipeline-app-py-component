from aws_cdk import core as cdk
from plugin.domain.manifest import Database
from plugin.infrastructure.resource.aws.services.main import SDK


class Taxonomy(cdk.Stack):
    """
    TODO
    """

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

    @staticmethod
    def add_classifications_to_resources(cloudservice: SDK, database: Database, region: str):
        """
        At the time this code was committed, the feature of add lf-tags to resources using CloudFormation/CDK has not yet been implemented.
        There is already an open issue (https://github.com/aws-cloudformation/cloudformation-coverage-roadmap/issues/920)
        and, for now, we will use the boto3 SDK and as soon as the feature is available for CDK, the migration will be performed.
        """
        cloudservice.add_classifications_to_resources(cloudservice.account_id, database, region)
