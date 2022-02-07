from aws_cdk import aws_glue as glue


def fields_to_columns(fields):
    columns = []
    for field in fields:
        columns.append(
            glue.CfnTable.ColumnProperty(
                name=field.name,
                type=field.type,
                comment=field.description
            )
        )
    return columns
