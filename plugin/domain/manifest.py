from __future__ import annotations
from dataclasses import dataclass
import json
from .helpers.taxonomy import build_classifications
from .helpers.schema import get_fields_types
from .model import Database, DataPipeline, Field, Schema, Schemas, Table, TableTrigger
from plugin.utils.file import get_filenames, read_yaml, read_avro_schema
from plugin.utils.string import convert_arn_to_kebab


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
    def __build_manifest(input_manifest: dict, tables: list[Table]) -> dict:
        return {
            "region": input_manifest["region"],
            "arn_bucket_source": convert_arn_to_kebab(input_manifest["arn_bucket_source"]),
            "arn_bucket_target": convert_arn_to_kebab(input_manifest["arn_bucket_target"]),
            "database": Database(
                name=input_manifest["database"]["name"],
                trigger=TableTrigger(
                    **input_manifest["database"]["trigger"]
                ) if "trigger" in input_manifest["database"] else None,
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

        classifications = build_classifications(
            index=schema_index,
            _list=input_manifest_tables
        )

        fields_types = get_fields_types(schema=json.dumps(avro_schema))

        return Table(
            name=avro_schema["name"],
            description=avro_schema["doc"] if "doc" in avro_schema else "",
            access_level=classifications["access_level"],
            pii=classifications["pii"],
            private=classifications["private"],
            data_classification=classifications["data_classification"],
            manipulated=classifications["manipulated"],
            schema=Schema(
                definition=avro_schema
            ),
            fields=[
                self.__build_field(
                    _field=avro_field,
                    _type=fields_types[avro_field["name"]],
                    input_table=input_table
                ) for avro_field in avro_schema["fields"]
            ] if "fields" in avro_schema else []
        )

    def __build_field(self, _field: dict, _type: str, input_table: dict) -> Field:
        input_fields = input_table["fields"] if "fields" in input_table else []
        field_index = self.__contains_field(
            fields=input_fields, field_name=_field["name"])

        classifications = build_classifications(
            index=field_index, _list=input_fields)

        return Field(
            name=_field["name"],
            access_level=classifications["access_level"],
            pii=classifications["pii"],
            private=classifications["private"],
            data_classification=classifications["data_classification"],
            type=_type,
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
