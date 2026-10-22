import csv
import io
import itertools
import os
import re
import time
import zlib
from collections import Counter, deque
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from typing import Any

import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from botocore.client import BaseClient
from tqdm.auto import tqdm

from common import (ACCESS_KEY, BUCKET, CSV_KEY, DICTIONARY_KEY, ENDPOINT, PARQUET_KEY, REGION, ROLE, SECRET_KEY, Rows,
                    Summary, canonical_hash, s3_client)

MiB = 1024 * 1024
SPLIT = 64 * MiB
PIECE = 8 * MiB
MAX_ATTEMPTS = 3

Task = dict[str, Any]
Key = tuple[str | int, ...]
Skills = list[str] | list[int]

ROLE_BYTES = ('"' + ROLE + '"').encode("utf-8")
NAME_GO = re.compile(
    "(^|[^a-zа-яё0-9_])(g[oо]([- ]?l[aа]n[gd])?|г[oо]л[aаэе]нг)([^a-zа-яё0-9_]|$)"
    "|(^|[^a-zа-яё0-9_])г[oо][- ](разработчик|программист|developer)"
    "|(разработчик|программист|developer)( на)? г[oо]([^a-zа-яё0-9_]|$)")
SKILL_GO = re.compile(
    "^(язык программирования |программирование на |(отличное )?знание (языка программирования )?)?"
    "(g[oо]([- ]?l[aа]ng)?|г[oо](л[aаэе]нг)?)$")
OTHER_LANGUAGE = re.compile(
    "(^|[^a-zа-яё0-9_])(python|питон|php|java|javascript|typescript|js|ts|node|nodejs|react|vue|angular|"
    "frontend|фронтенд|kotlin|scala|ruby|rust|swift|ios|android|flutter|dart|delphi|perl|elixir|"
    "erlang|haskell|clojure|lua|unity|bitrix|битрикс|abap|sap|1с|1c)([^a-zа-яё0-9_]|$)"
    "|c#|c\\+\\+|\\.net|с#|с\\+\\+")

client: BaseClient | None = None


def s3() -> BaseClient:
    global client
    if client is None:
        client = s3_client()
    return client


def filesystem() -> pafs.S3FileSystem:
    return pafs.S3FileSystem(endpoint_override=ENDPOINT, scheme="http", access_key=ACCESS_KEY,
                             secret_key=SECRET_KEY, region=REGION)


def normalize_skills(raw: str) -> list[str]:
    skills = set()
    for item in raw.lower().split(";"):
        skill = item.strip(" ")
        if skill == "":
            continue
        if SKILL_GO.search(skill):
            skill = "go"
        skills.add(skill)
    return sorted(skills)


def is_go_vacancy(name: str, has_go_skill: bool) -> bool:
    name = name.lower()
    if NAME_GO.search(name):
        return True
    return has_go_skill and not OTHER_LANGUAGE.search(name)


def read_range(first: int, last: int) -> bytes:
    response = s3().get_object(Bucket=BUCKET, Key=CSV_KEY, Range="bytes=" + str(first) + "-" + str(last))
    return response["Body"].read()


def split_blocks(start: int, end: int, size: int) -> Iterator[bytes]:
    position = max(start - 1, 0)
    skip_first = start > 0
    tail = b""
    tail_start = position
    while position < size:
        if position < end:
            stop = min(position + PIECE, end)
        else:
            stop = min(position + 65536, size)
        data = tail + read_range(position, stop - 1)
        base = tail_start
        position = stop
        if skip_first:
            newline = data.find(b"\n")
            if newline < 0:
                tail = b""
                tail_start = position
                continue
            data = data[newline + 1:]
            base = base + newline + 1
            skip_first = False
        if base >= end:
            return
        last_newline = data.rfind(b"\n")
        if last_newline < 0:
            tail = data
            tail_start = base
            continue
        if base + last_newline + 1 >= end:
            yield data[:data.find(b"\n", end - base - 1)]
            return
        yield data[:last_newline]
        tail = data[last_newline + 1:]
        tail_start = base + last_newline + 1
    if tail and tail_start < end:
        yield tail


def maybe_fail(task: Task) -> None:
    if task["fail"] is None or task["fail"] != task["id"] or task["attempt"] > 1:
        return
    os._exit(13)


def map_csv(task: Task, emit: Callable[[Skills], None], stats: dict[str, Any]) -> None:
    first = task["start"] == 0
    for block in split_blocks(task["start"], task["end"], task["size"]):
        lines = block.split(b"\n")
        if first:
            lines = lines[1:]
            first = False
        stats["rows"] += len(lines)
        for line in lines:
            if ROLE_BYTES not in line:
                continue
            row = next(csv.reader([line.decode("utf-8")]))
            if row[task["role"]] != ROLE:
                continue
            skills = normalize_skills(row[task["skills"]])
            if is_go_vacancy(row[task["name"]], "go" in skills):
                emit(skills)
        maybe_fail(task)


def map_parquet(task: Task, emit: Callable[[Skills], None], stats: dict[str, Any]) -> None:
    skills_column = "skill_ids" if task["source"] == "parquet_ids" else "key_skills"
    parquet = pq.ParquetFile(task["path"], filesystem=filesystem())
    table = parquet.read_row_group(task["row_group"], columns=["role", "name", skills_column])
    stats["rows"] += table.num_rows
    developers = table.filter(pc.field("role") == ROLE)
    for name, raw in zip(developers["name"].to_pylist(), developers[skills_column].to_pylist()):
        if task["source"] == "parquet_ids":
            skills = sorted(set(task["go_id"] if skill_id in task["synonyms"] else skill_id for skill_id in raw))
            has_go = task["go_id"] in skills
        else:
            skills = normalize_skills(raw or "")
            has_go = "go" in skills
        if is_go_vacancy(name or "", has_go):
            emit(skills)
    maybe_fail(task)


def map_task(task: Task) -> dict[str, Any]:
    reducers = task["reducers"]
    partitions: list[Counter[Key]] = [Counter() for _ in range(reducers)]
    stats: dict[str, Any] = {"rows": 0, "variant": 0, "with_skills": 0, "emitted": [0, 0, 0, 0], "skills_per_vacancy": Counter()}

    def emit(skills: Skills) -> None:
        stats["variant"] += 1
        stats["skills_per_vacancy"][len(skills)] += 1
        if skills:
            stats["with_skills"] += 1
        for size in (1, 2, 3):
            for combo in itertools.combinations(skills, size):
                partitions[zlib.crc32(repr(combo).encode()) % reducers][combo] += 1
                stats["emitted"][size] += 1

    if task["source"] == "csv":
        map_csv(task, emit, stats)
    else:
        map_parquet(task, emit, stats)
    return {"partitions": [dict(partition) for partition in partitions], "stats": stats}


def reduce_task(partitions: list[dict[Key, int]]) -> dict[Key, int]:
    total: Counter[Key] = Counter()
    for partition in partitions:
        total.update(partition)
    return dict(total)


def plan_csv(limit_bytes: int | None) -> tuple[list[Task], None]:
    size = s3().head_object(Bucket=BUCKET, Key=CSV_KEY)["ContentLength"]
    header = next(csv.reader([read_range(0, 65535).split(b"\n")[0].decode("utf-8")]))
    total = size if limit_bytes is None else min(size, limit_bytes)
    tasks = []
    for start in range(0, total, SPLIT):
        tasks.append({"source": "csv", "start": start, "end": min(start + SPLIT, total), "size": size,
                      "role": header.index("role"), "name": header.index("name"), "skills": header.index("key_skills")})
    return tasks, None


def plan_parquet(source: str) -> tuple[list[Task], dict[int, str] | None]:
    skill_names = None
    extra: dict[str, Any] = {}
    if source == "parquet_ids":
        dictionary = pq.read_table(BUCKET + "/" + DICTIONARY_KEY, filesystem=filesystem(),
                                   columns=["skill_id", "skill"]).to_pydict()
        skill_names = dict(zip(dictionary["skill_id"], dictionary["skill"]))
        extra["go_id"] = dictionary["skill_id"][dictionary["skill"].index("go")]
        extra["synonyms"] = set(skill_id for skill_id, skill in skill_names.items() if SKILL_GO.search(skill))
    tasks = []
    groups_total = 0
    for info in filesystem().get_file_info(pafs.FileSelector(BUCKET + "/" + PARQUET_KEY)):
        metadata = pq.ParquetFile(info.path, filesystem=filesystem()).metadata
        role_index = metadata.schema.names.index("role")
        for group in range(metadata.num_row_groups):
            groups_total += 1
            statistics = metadata.row_group(group).column(role_index).statistics
            if statistics is not None and statistics.null_count == metadata.row_group(group).num_rows:
                continue
            if statistics is not None and statistics.has_min_max and (statistics.min > ROLE or statistics.max < ROLE):
                continue
            tasks.append(dict({"source": source, "path": info.path, "row_group": group}, **extra))
    print("[master] row group всего", groups_total, "с нужной ролью по статистике min/max", len(tasks))
    return tasks, skill_names


def run(source: str = "csv", workers: int = 12, reducers: int = 4, limit_bytes: int | None = None,
        fail: int | None = None, out_prefix: str | None = None) -> tuple[Summary, dict[int, Rows]]:
    started = time.perf_counter()
    if source == "csv":
        tasks, skill_names = plan_csv(limit_bytes)
    else:
        tasks, skill_names = plan_parquet(source)
    events: list[str] = []
    queue = deque(range(len(tasks)))
    attempts: Counter[int] = Counter()
    results: dict[int, dict[str, Any]] = {}
    running: dict[Future, int] = {}
    pool = ProcessPoolExecutor(min(workers, len(tasks)))
    bar = tqdm(total=len(tasks), desc="mapreduce " + source + ", map тасков")

    def note(message: str) -> None:
        events.append(message)
        print("[master] " + message)

    def submit() -> None:
        while queue and len(running) < workers + 2:
            index = queue.popleft()
            attempts[index] += 1
            if attempts[index] > MAX_ATTEMPTS:
                raise RuntimeError("map таск " + str(index) + " не выполнился за " + str(MAX_ATTEMPTS) + " попытки")
            task = dict(tasks[index], id=index, attempt=attempts[index], reducers=reducers, fail=fail)
            running[pool.submit(map_task, task)] = index

    submit()
    while running:
        done, _ = wait(running, return_when=FIRST_COMPLETED)
        lost = []
        for future in done:
            index = running.pop(future)
            try:
                results[index] = future.result()
                bar.update(1)
            except BrokenProcessPool:
                lost.append(index)
            except Exception as error:
                note("map " + str(index) + " упал на попытке " + str(attempts[index]) + " (" + str(error) + "), перезапуск")
                queue.appendleft(index)
        if lost:
            lost = lost + [running.pop(future) for future in list(running)]
            note("умер процесс-воркер, пул пересоздан, перезапуск тасков которые были в работе: " + str(sorted(lost)))
            pool.shutdown(wait=False, cancel_futures=True)
            pool = ProcessPoolExecutor(min(workers, len(tasks)))
            queue.extendleft(sorted(lost, reverse=True))
        submit()
    bar.close()
    pool.shutdown()
    map_seconds = time.perf_counter() - started

    with ProcessPoolExecutor(reducers) as reduce_pool:
        futures = [reduce_pool.submit(reduce_task, [results[index]["partitions"][reducer] for index in sorted(results)])
                   for reducer in range(reducers)]
        counters = [future.result() for future in futures]

    by_size: dict[int, Rows] = {1: [], 2: [], 3: []}
    for counter in counters:
        for parts, count in counter.items():
            if skill_names is None:
                combo = tuple(str(part) for part in parts)
            else:
                combo = tuple(sorted(skill_names[int(part)] for part in parts))
            by_size[len(combo)].append((combo, count))
    for size in by_size:
        by_size[size].sort(key=lambda item: (-item[1], item[0]))

    if out_prefix is not None:
        for size in by_size:
            buffer = io.StringIO()
            writer = csv.writer(buffer, lineterminator="\n")
            writer.writerow(["skill_" + str(position) for position in range(1, size + 1)] + ["vacancies"])
            for combo, count in by_size[size]:
                writer.writerow(list(combo) + [count])
            key = out_prefix + "/skills_k" + str(size) + ".csv"
            s3().put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue().encode("utf-8"))

    stats = [result["stats"] for result in results.values()]
    histogram: Counter[int] = Counter()
    for item in stats:
        histogram.update(item["skills_per_vacancy"])
    summary = {
        "source": source,
        "map_tasks": len(tasks),
        "rows": sum(item["rows"] for item in stats),
        "variant_vacancies": sum(item["variant"] for item in stats),
        "with_skills": sum(item["with_skills"] for item in stats),
        "emitted": {size: sum(item["emitted"][size] for item in stats) for size in (1, 2, 3)},
        "unique": {size: len(by_size[size]) for size in (1, 2, 3)},
        "total_count": {size: sum(count for _, count in by_size[size]) for size in (1, 2, 3)},
        "sha256": {size: canonical_hash(by_size[size]) for size in (1, 2, 3)},
        "skills_per_vacancy": dict(sorted(histogram.items())),
        "retried": {index: count for index, count in attempts.items() if count > 1},
        "events": events,
        "map_s": round(map_seconds, 1),
        "total_s": round(time.perf_counter() - started, 1),
    }
    return summary, by_size
