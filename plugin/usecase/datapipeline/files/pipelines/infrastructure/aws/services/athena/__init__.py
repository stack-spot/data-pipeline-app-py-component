from .service import Athena as AthenaService


class Athena:
    """
    TO DO
    """

    @staticmethod
    def run_query_execution(query: str, database: str, path_staging: str, region: str) -> dict:
        athena = AthenaService(region=region)
        return athena.run_query_execution(query, database, path_staging)
