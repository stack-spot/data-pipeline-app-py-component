from avro.schema import (
    parse,
    RecordSchema,
    Schema,
    PrimitiveSchema,
    ArraySchema,
    MapSchema,
    EnumSchema,
    UnionSchema,
    FixedSchema,
    BytesDecimalSchema,
    FixedDecimalSchema,
    DateSchema,
    TimeMillisSchema,
    TimeMicrosSchema,
    TimestampMillisSchema,
    TimestampMicrosSchema
)


def get_hive_schema_from_avro(avro_schema: str) -> str:
    schema: RecordSchema = parse(avro_schema)

    column_schemas = [
        f"`{_field.name.lower()}` {get_field_type(_field.type)}" for _field in schema.fields
    ]

    return ", ".join(column_schemas)

# pylint: disable=|too-many-return-statements
def get_field_type(avro_schema: Schema) -> str:
    if isinstance(avro_schema, PrimitiveSchema):
        return rename_type_names(avro_schema.type, avro_schema.get_prop("logicalType"))

    if isinstance(avro_schema, ArraySchema):
        items_type = get_field_type(avro_schema.items)
        return f"array<{items_type}>"

    if isinstance(avro_schema, MapSchema):
        values_schema = get_field_type(avro_schema.values)
        return f"map<string,{values_schema}>"

    if isinstance(avro_schema, RecordSchema):
        field_schemas = [
            f"{_field.name.lower()}:{get_field_type(_field.type)}" for _field in avro_schema.fields
        ]

        field_schema_concatenated = ",".join(field_schemas)
        return f"struct<{field_schema_concatenated}>"

    if isinstance(avro_schema, UnionSchema):
        union_schemas_not_null = [
            s for s in avro_schema.schemas if s.type != "null"
        ]

        contains_null_schema = len([
            s for s in avro_schema.schemas if s.type == "null"
        ]) > 0

        if contains_null_schema and len(union_schemas_not_null) == 1:
            return get_field_type(union_schemas_not_null[0])
        if contains_null_schema and len(union_schemas_not_null) == 0:
            raise Exception("union schemas contains only null schema")

        union_schemas = [
            f"member{index}:{get_field_type(schema)}" for (index, schema) in enumerate(union_schemas_not_null)
        ]

        members_concatenated = ",".join(union_schemas)
        return f"struct<{members_concatenated}>"

    if isinstance(avro_schema, (EnumSchema, FixedSchema)):
        return "string"

    if isinstance(avro_schema, (BytesDecimalSchema, FixedDecimalSchema)):
        return f"decimal({avro_schema.precision},{avro_schema.scale})"

    if isinstance(avro_schema, DateSchema):
        return "date"

    if isinstance(avro_schema, TimeMillisSchema):
        return "int"

    if isinstance(avro_schema, TimeMicrosSchema):
        return "long"

    if isinstance(avro_schema, (TimestampMillisSchema, TimestampMicrosSchema)):
        return "timestamp"

    raise Exception(f"unknown avro schema type {avro_schema.type}")


def rename_type_names(_type: str, logicalType: str = "") -> str:
    if _type == "long" and logicalType in {"timestamp-millis", "timestamp-micros"}:
        return "timestamp"

    if _type == "long":
        return "bigint"

    return _type


def get_fields_types(schema: str) -> dict:
    avro_schema: RecordSchema = parse(schema)

    return {
        _field.name.lower(): get_field_type(_field.type) for _field in avro_schema.fields
    }
