import hashlib
from typing import Any

import boto3
from botocore.client import BaseClient
from botocore.config import Config

ENDPOINT = "localhost:9000"
ACCESS_KEY = "rustfsadmin"
SECRET_KEY = "rustfsadmin"
REGION = "us-east-1"
BUCKET = "hh-lake"
CSV_KEY = "raw/vacancies_2024.csv"
PARQUET_KEY = "parquet/vacancies"
DICTIONARY_KEY = "parquet/skills.parquet"
ROLE = "Программист, разработчик"

Combo = tuple[str, ...]
Rows = list[tuple[Combo, int]]
Summary = dict[str, Any]


def s3_client() -> BaseClient:
    return boto3.client("s3", endpoint_url="http://" + ENDPOINT, aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY, region_name=REGION,
                        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}))


def canonical_hash(rows: Rows) -> str:
    digest = hashlib.sha256()
    for combo, count in sorted(rows):
        digest.update(("\t".join(combo) + "\t" + str(count) + "\n").encode("utf-8"))
    return digest.hexdigest()
