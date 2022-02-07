def get_bucket_name_by_arn(arn: str):
    bucket = arn.split(":")
    bucket.reverse()
    return bucket[0]


