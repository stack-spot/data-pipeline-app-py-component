from .interface import AthenaInterface
import boto3


class Athena(AthenaInterface):
    """
    TO DO

    Args:
        AthenaInterface ([type]): [description]
    """

    def __init__(self, **knargs) -> None:
        self.session = boto3.session.Session()
        self.athena = self.session.client(
            'athena', region_name=knargs.get('region', "us-east-1"))

    def run_query_execution(self, query: str, database: str, path_staging: str) -> dict:
        return self.athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={
                'OutputLocation': f'{path_staging}/athenaoutput/'},
        )
