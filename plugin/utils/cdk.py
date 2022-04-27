from aws_cdk import aws_glue as glue
from plugin.domain.model import Field


def fields_to_columns(fields: list[Field]) -> list:
    columns = [
        glue.CfnTable.ColumnProperty(
            name="event_time",
            type="timestamp",
            comment="event_time"
        ),
        glue.CfnTable.ColumnProperty(
            name="event_id",
            type="int",
            comment="event_id"
        )
    ]

    for _field in fields:
        columns.append(
            glue.CfnTable.ColumnProperty(
                name=_field.name,
                type=_field.type,
                comment=_field.description
            )
        )

    return columns
