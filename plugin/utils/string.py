from re import sub


def get_bucket_name_by_arn(arn: str):
    bucket = arn.split(":")
    bucket.reverse()
    return bucket[0]


def kebab(s: str) -> str:
    return '-'.join(
            sub(r"(\s|_|-)+"," ",
                sub(r"[A-Z]{2,}(?=[A-Z][a-z]+[0-9]*|\b)|[A-Z]?[a-z]+[0-9]*|[A-Z]|[0-9]+",
                    lambda mo: ' ' + mo.group(0).lower(), s)).split())


def snake_case(s: str) -> str:
    return '_'.join(
            sub(r"(\s|_|-)+"," ",
                sub(r"[A-Z]{2,}(?=[A-Z][a-z]+[0-9]*|\b)|[A-Z]?[a-z]+[0-9]*|[A-Z]|[0-9]+",
                    lambda mo: ' ' + mo.group(0).lower(), s)).split())
