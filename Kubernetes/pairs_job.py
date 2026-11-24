import csv
import io
import os
import pickle
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

import boto3
from botocore.client import BaseClient
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "MapReduce"))

import mapreduce_job
from common import ACCESS_KEY, BUCKET, REGION, SECRET_KEY, Rows, canonical_hash

ENDPOINT = os.environ.get("S3_ENDPOINT", "localhost:9000")
PARTS = 8
REDUCERS = 4
WORKERS = int(os.environ.get("WORKERS", "4"))
HW3_PAIRS = "results/mapreduce/skills_k2.csv"
MiB = 1024 * 1024


def s3_client() -> BaseClient:
    return boto3.client("s3", endpoint_url="http://" + ENDPOINT, aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY, region_name=REGION,
                        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}))


def fresh_client() -> None:
    mapreduce_job.client = s3_client()


def part_tasks(index: int) -> list[dict[str, Any]]:
    tasks, _ = mapreduce_job.plan_csv(None)
    size = -(-len(tasks) // PARTS)
    chosen = range(index * size, min((index + 1) * size, len(tasks)))

    return [dict(tasks[task], id=task, attempt=1, reducers=REDUCERS, fail=None) for task in chosen]


def memory() -> str:
    peak = Path("/sys/fs/cgroup/memory.peak")

    if not peak.exists():
        return "пик памяти не известен, нет cgroup v2"

    limit = Path("/sys/fs/cgroup/memory.max").read_text().strip()
    limit_text = "без предела" if limit == "max" else str(round(int(limit) / MiB)) + " МиБ"

    return "пик памяти контейнера " + str(round(int(peak.read_text()) / MiB)) + " МиБ, предел " + limit_text


def run_part(run: str, index: int) -> None:
    started = time.perf_counter()
    fresh_client()
    tasks = part_tasks(index)
    print("часть", index, "из", PARTS, "сплиты", tasks[0]["id"], "-", tasks[-1]["id"], "воркеров", WORKERS, "S3",
          ENDPOINT, flush=True)

    pairs: Counter[tuple[str | int, ...]] = Counter()
    rows = 0
    variant = 0

    with ProcessPoolExecutor(WORKERS, initializer=fresh_client) as pool:
        for done, result in enumerate(pool.map(mapreduce_job.map_task, tasks), 1):
            rows += result["stats"]["rows"]
            variant += result["stats"]["variant"]

            for partition in result["partitions"]:
                for combo, count in partition.items():
                    if len(combo) == 2:
                        pairs[combo] += count

            if done % 10 == 0 or done == len(tasks):
                print("готово сплитов", done, "из", len(tasks), flush=True)

    key = "k8s/" + run + "/part-" + str(index) + ".pickle"
    s3_client().put_object(Bucket=BUCKET, Key=key, Body=pickle.dumps(dict(pairs)))
    print("строк", rows, "вакансий Go", variant, "уникальных пар", len(pairs), "за", round(time.perf_counter() - started, 1),
          "с, записал", key)
    print(memory(), flush=True)


def read_pairs(key: str) -> Rows:
    text = s3_client().get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8")

    return [((row[0], row[1]), int(row[2])) for row in list(csv.reader(io.StringIO(text)))[1:]]


def reduce(run: str) -> None:
    s3 = s3_client()
    listing = s3.list_objects_v2(Bucket=BUCKET, Prefix="k8s/" + run + "/part-")
    keys = sorted(item["Key"] for item in listing.get("Contents", []))

    if len(keys) != PARTS:
        raise RuntimeError("частей " + str(len(keys)) + " вместо " + str(PARTS))

    total: Counter[tuple[str, ...]] = Counter()

    for key in keys:
        part = pickle.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        total.update({tuple(str(skill) for skill in combo): count for combo, count in part.items()})

    pairs: Rows = sorted(total.items(), key=lambda item: (-item[1], item[0]))
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["skill_1", "skill_2", "vacancies"])

    for combo, count in pairs:
        writer.writerow(list(combo) + [count])

    key = "k8s/" + run + "/pairs.csv"
    s3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue().encode("utf-8"))

    hw3 = read_pairs(HW3_PAIRS)
    print("частей", len(keys), "уникальных пар", len(pairs), "сумма частот", sum(count for _, count in pairs), "записал", key)
    print("топ-10 пар: здесь | ДЗ 3")

    for rank in range(10):
        print(rank + 1, " + ".join(pairs[rank][0]), pairs[rank][1], "|", " + ".join(hw3[rank][0]), hw3[rank][1])

    print("топ-10 совпадает с ДЗ 3:", "да" if pairs[:10] == hw3[:10] else "нет")
    print("все пары совпадают с ДЗ 3:", "да" if canonical_hash(pairs) == canonical_hash(hw3) else "нет")
    print(memory(), flush=True)


if __name__ == "__main__":
    if sys.argv[1] == "part":
        run_part(sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else int(os.environ["JOB_COMPLETION_INDEX"]))
    else:
        reduce(sys.argv[2])
