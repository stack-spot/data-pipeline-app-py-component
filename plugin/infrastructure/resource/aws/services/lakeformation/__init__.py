from plugin.domain.manifest import Database
from .service import LakeFormationResource


class LakeFormation:
    """
    TO DO
    """
    @staticmethod
    def add_classifications_to_resources(account_id: str, database: Database, region: str) -> None:
        lakeformation = LakeFormationResource(region)
        lakeformation.add_classifications_to_resources(account_id, database)
