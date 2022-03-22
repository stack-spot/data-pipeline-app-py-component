from __future__ import annotations
from dataclasses import dataclass, field
from plugin.utils.file import get_filenames, read_yaml, read_avro_schema
from plugin.utils.string import convert_arn_to_kebab


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
        if self.pii is not None and self.pii not in ["true", "false"]:
            raise ValueError(
                f"'pii' must be boolean type (true/false) not '{self.pii}'")

    def _is_valid_private(self):
        if self.private is not None and self.private not in ["true", "false"]:
            raise ValueError(
                f"'private' must be boolean type (true/false) not '{self.private}'")

    def _is_valid_data_classification(self):
        if self.data_classification is not None and self.data_classification not in ["business", "technical", ""]:
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
class Table(Taxonomy):
    """
    TODO
    """

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

    @property
    def get_struct_schema(self):
        self.fields.append(
            Field(name="event_time", type="timestamp", access_level="partial"))
        self.fields.append(
            Field(name="event_id", type="int", access_level="partial"))
        return self.fields


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


@dataclass
class DataPipeline:
    """
    TODO
    """
    arn_bucket_source: str
    arn_bucket_target: str
    database: Database
    region: str = "us-east-1"


@dataclass
class Manifest:
    """
    TODO
    """
    data_pipeline: DataPipeline

    def __init__(self, manifest) -> None:
        if isinstance(manifest, str):
            manifest_dict = read_yaml(manifest)
            self.__build_domain(manifest_dict)
        elif isinstance(manifest, dict):
            self.__build_domain(manifest)
        else:
            raise TypeError

    def __build_domain(self, manifest_dict) -> None:
        dir_path = Schemas.path 

        avro_schemas_list = get_filenames(
            directory=dir_path, glob_pattern="*.avsc")

        schemas = manifest_dict["data-pipeline"]["database"].get("schemas", {})

        input_tables = list(schemas.get("tables", []))

        tables = self.__build_tables(
            path_to_schemas=dir_path,
            avro_schemas_list=avro_schemas_list,
            input_manifest_tables=input_tables
        )

        data_pipeline = self.__build_manifest(
            input_manifest=manifest_dict["data-pipeline"],
            tables=tables
        )

        self.data_pipeline = DataPipeline(**data_pipeline)

    @staticmethod
    def __define_access_level(pii: str, private: str) -> str:
        if pii == "true" and private == "true":
            return "full"

        if pii == "true":
            return "sensitive"

        if private == "true":
            return "restrict"

        return "partial"

    @staticmethod
    def __build_default_boolean(index: int, _list: list, field_type: str) -> bool:

        if index is None:
            return False

        if field_type in _list[index]:
            return _list[index][field_type]
        return False

    @staticmethod
    def __build_data_classification(index: int, _list: list) -> str:

        if index is None:
            return ""

        if "data_classification" in _list[index]:
            return _list[index]["data_classification"]
        return ""

    @staticmethod
    def __build_manifest(input_manifest: dict, tables: list[Table]) -> dict:
        return {
            "region": input_manifest["region"],
            "arn_bucket_source": convert_arn_to_kebab(input_manifest["arn_bucket_source"]),
            "arn_bucket_target": convert_arn_to_kebab(input_manifest["arn_bucket_target"]),
            "database": Database(
                name=input_manifest["database"]["name"],
                schemas=Schemas(
                    tables=tables
                )
            )
        }

    def __build_tables(self, path_to_schemas: str, avro_schemas_list: list, input_manifest_tables: list) -> list[Table]:

        avro_schemas_iterator = iter([
            read_avro_schema(f"{path_to_schemas}/{file_name}") for file_name in avro_schemas_list
        ])

        return [
            self.__build_table(
                avro_schema=next(avro_schemas_iterator),
                input_manifest_tables=input_manifest_tables
            ) for _ in range(len(avro_schemas_list))
        ]

    def __build_table(self, avro_schema: dict, input_manifest_tables: list) -> Table:

        schema_index = self.__contains_schema(
            schemas=input_manifest_tables, schema_name=avro_schema["name"])

        input_table = input_manifest_tables[schema_index] if schema_index is not None else {
        }

        pii = str(self.__build_default_boolean(
            index=schema_index, _list=input_manifest_tables, field_type="pii")).lower()
        private = str(self.__build_default_boolean(
            index=schema_index, _list=input_manifest_tables, field_type="private")).lower()
        manipulated = str(self.__build_default_boolean(
            index=schema_index, _list=input_manifest_tables, field_type="manipulated")).lower()
        data_classification = self.__build_data_classification(
            index=schema_index, _list=input_manifest_tables)

        return Table(
            name=avro_schema["name"],
            access_level=self.__define_access_level(pii, private),
            pii=pii,
            private=private,
            data_classification=data_classification,
            manipulated=manipulated,
            fields=[
                self.__build_field(_field=avro_field, input_table=input_table) for avro_field in avro_schema["fields"]
            ] if "fields" in avro_schema else []
        )

    @staticmethod
    def __get_field_type(_type):
        if isinstance(_type, list):
            _first = [t for t in _type if t != "null"][0]
            return _first
        return _type

    def __build_field(self, _field: dict, input_table: dict) -> Field:
        input_fields = input_table["fields"] if "fields" in input_table else []
        field_index = self.__contains_field(
            fields=input_fields, field_name=_field["name"])

        pii = str(self.__build_default_boolean(
            index=field_index, _list=input_fields, field_type="pii")).lower()
        private = str(self.__build_default_boolean(
            index=field_index, _list=input_fields, field_type="private")).lower()
        data_classification = self.__build_data_classification(
            index=field_index, _list=input_fields)

        return Field(
            name=_field["name"],
            access_level=self.__define_access_level(pii, private),
            pii=pii,
            private=private,
            data_classification=data_classification,
            type=self.__get_field_type(_field["type"]),
            description=_field["doc"] if "doc" in _field else ""
        )

    @staticmethod
    def __get_index_by_name(source: list, name: str) -> int | None:
        index_by_name = next(
            (index for (index, d) in enumerate(source) if d["name"] == name), None)
        return index_by_name

    def __contains_schema(self, schemas: list, schema_name: str):
        return self.__get_index_by_name(source=schemas, name=schema_name)

    def __contains_field(self, fields: list, field_name: str):
        return self.__get_index_by_name(source=fields, name=field_name)
