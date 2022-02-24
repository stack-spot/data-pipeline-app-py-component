
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_kinesis as kinesis
from aws_cdk import aws_kinesisfirehose as firehose


def get_bucket_raw(cdk, scope, stack_id, name) -> s3.IBucket:
    account = f"{cdk.Stack.of(scope).account}"
    return s3.Bucket.from_bucket_name(scope, stack_id, f"{account}-raw-{name}")


def get_kinesis_stream(cdk, scope, stack_id, dataproduct_name) -> kinesis.IStream:
    account = f"{cdk.Stack.of(scope).account}"
    region = f"{cdk.Stack.of(scope).region}"
    return kinesis.Stream.from_stream_arn(scope, stack_id,
        f"arn:aws:kinesis:{region}:{account}:stream/{dataproduct_name}-*"
    )


def get_firehose(scope, stack_id, dataproduct_name, schema_name) -> firehose.IDeliveryStream:
    return firehose.DeliveryStream.from_delivery_stream_name(scope, stack_id,
        f"{dataproduct_name}-{schema_name}-delivery"
    )

