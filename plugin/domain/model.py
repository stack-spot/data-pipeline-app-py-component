from __future__ import annotations
import json
from dataclasses import dataclass, field
from .helpers.schema import get_hive_schema_from_avro
from .validation import ValidationManifest as val_


@dataclass
class Taxonomy:
    """
    TODO
    """

    access_level: str = "partial"
    data_classification: str = ""
    pii: str = "false"
    private: str = "false"

    def __post_init__(self):
        self._is_valid_pii()
        self._is_valid_private()
        self._is_valid_data_classification()

    @property
    def tag_keys(self) -> list:
        return list(getattr(self.__class__.__bases__[0], '__annotations__', {}).keys())

    def _is_valid_pii(self):
        if self.pii is not None and self.pii not in {"true", "false"}:
            raise ValueError(
                f"'pii' must be boolean type (true/false) not '{self.pii}'")

    def _is_valid_private(self):
        if self.private is not None and self.private not in {"true", "false"}:
            raise ValueError(
                f"'private' must be boolean type (true/false) not '{self.private}'")

    def _is_valid_data_classification(self):
        if self.data_classification is not None and self.data_classification not in {"business", "technical", ""}:
            raise ValueError(
                f"'data_classification' must be 'business' or 'technical' or '' not '{self.data_classification}'")


@dataclass
class Field(Taxonomy):
    """
    TODO
    """

    name: str = ""
    type: str = ""
    description: str = ""


@dataclass
class TableTrigger:
    """
    TODO
    """

    type: str = "ON_DEMAND"
    cron: str = ""

    def __post_init__(self):
        val_.checking_the_type(self)
        val_.checking_vars_type(
            self, type="str")


@dataclass
class Schema:
    """
    TODO
    """

    definition: dict = field(default_factory=dict)

    @property
    def definition_to_json(self) -> str:
        return json.dumps(self.definition, indent=2)

    @property
    def to_hive_struct(self) -> str:
        return get_hive_schema_from_avro(avro_schema=self.definition_to_json)


@dataclass
class Table(Taxonomy):
    """
    TODO
    """

    schema: Schema | None = None
    description: str = ""
    name: str = ""
    manipulated: str = "false"
    fields: list[Field] = field(default_factory=list)

    def __post_init__(self):
        self._is_valid_manipulated()

    @property
    def tag_keys(self) -> list:
        return Taxonomy.tag_keys.fget(self) + ['manipulated']

    def _is_valid_manipulated(self):
        if self.manipulated is not None and self.manipulated not in ["true", "false"]:
            raise ValueError(
                f"'manipulated' must be boolean type (true/false) not '{self.manipulated}'")


@dataclass
class Schemas:
    """
    TODO
    """

    path: str = "./schemas"
    tables: list[Table] = field(default_factory=list)


@dataclass
class Database:
    """
    TODO
    """

    name: str
    schemas: Schemas
    trigger: TableTrigger | None = None


@dataclass(frozen=True)
class DataPipeline:
    """
    TODO
    """

    arn_bucket_source: str
    arn_bucket_target: str
    database: Database
    region: str = "us-east-1"
