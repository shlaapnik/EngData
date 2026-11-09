import csv
import io
import json
import logging
import os
import pickle
import sys
import time
import traceback
from collections import Counter, deque
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from datetime import datetime
from pathlib import Path
from typing import Any, TextIO

from botocore.client import BaseClient
from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, KazooException

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "MapReduce"))

import mapreduce_job
from common import BUCKET, Rows, canonical_hash, s3_client

HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
SESSION_TIMEOUT = 10.0
ROOT = "/hw4"
LOGS = Path(__file__).resolve().parent / "logs"
HW3_PAIRS = "results/mapreduce/skills_k2.csv"
WORKERS = 12
REDUCERS = 4
SESSION_STATES = {"CONNECTED": "сессия ZooKeeper активна", "SUSPENDED": "связь с ZooKeeper потеряна",
                  "LOST": "сессия ZooKeeper истекла"}

name = ""
log_file: TextIO | None = None


def log(text: str) -> None:
    line = datetime.now().strftime("%H:%M:%S.%f")[:-3] + " " + name + " " + text
    print(line, flush=True)

    if log_file is not None:
        log_file.write(line + "\n")
        log_file.flush()


def node(run: str, part: str) -> str:
    return ROOT + "/" + run + "/" + part


def checkpoint_prefix(run: str) -> str:
    return "checkpoints/hw4/" + run + "/"


def result_prefix(run: str) -> str:
    return "results/hw4/" + run + "/"


def finished_tasks(s3: BaseClient, run: str) -> set[int]:
    finished = set()

    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=checkpoint_prefix(run)):
        for item in page.get("Contents", []):
            finished.add(int(item["Key"].rsplit("/", 1)[1].split(".")[0]))

    return finished


def read_pairs(s3: BaseClient, key: str) -> Rows:
    text = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8")

    return [((row[0], row[1]), int(row[2])) for row in list(csv.reader(io.StringIO(text)))[1:]]


def pairs_csv(pairs: Rows) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["skill_1", "skill_2", "vacancies"])

    for combo, count in pairs:
        writer.writerow(list(combo) + [count])

    return buffer.getvalue().encode("utf-8")


def claim_epoch(zk: KazooClient, run: str) -> int:
    while True:
        _, stat = zk.get(node(run, "epoch"))

        try:
            return zk.set(node(run, "epoch"), str(stat.version + 1).encode(), version=stat.version).version
        except BadVersionError:
            pass


def save_checkpoint(zk: KazooClient, run: str, value: dict[str, Any]) -> None:
    try:
        zk.set(node(run, "checkpoint"), json.dumps(value).encode())
    except KazooException as error:
        log("чекпоинт в ZooKeeper не записан: " + type(error).__name__)


def count_pairs(zk: KazooClient, s3: BaseClient, run: str, epoch: int) -> Rows:
    tasks, _ = mapreduce_job.plan_csv(None)
    finished = finished_tasks(s3, run)

    if finished:
        log("продолжаю с чекпоинта: готово " + str(len(finished)) + " из " + str(len(tasks)) + " map-задач")
    else:
        log("начинаю расчёт, map-задач " + str(len(tasks)))

    queue = deque(index for index in range(len(tasks)) if index not in finished)
    running: dict[Future[dict[str, Any]], int] = {}
    failures: Counter[int] = Counter()
    logged = len(finished)

    with ProcessPoolExecutor(WORKERS) as pool:
        while True:
            while queue and len(running) < WORKERS + 2:
                index = queue.popleft()

                if index not in finished:
                    task = dict(tasks[index], id=index, attempt=1, reducers=REDUCERS, fail=None)
                    running[pool.submit(mapreduce_job.map_task, task)] = index

            if not running:
                break

            done, _ = wait(running, return_when=FIRST_COMPLETED)

            for future in done:
                index = running.pop(future)

                try:
                    result = future.result()
                except Exception as error:
                    failures[index] += 1

                    if failures[index] == 3:
                        raise

                    log("map-задача " + str(index) + " упала (" + type(error).__name__ + "), перезапуск")
                    queue.appendleft(index)
                    continue

                key = checkpoint_prefix(run) + str(index).zfill(5) + ".pickle"
                s3.put_object(Bucket=BUCKET, Key=key, Body=pickle.dumps(result))

            finished = finished_tasks(s3, run)
            save_checkpoint(zk, run, {"done": len(finished), "total": len(tasks), "coordinator": name, "epoch": epoch})

            if len(finished) - logged >= 25 or (len(finished) == len(tasks) and logged != len(tasks)):
                log("чекпоинт: готово " + str(len(finished)) + " из " + str(len(tasks)))
                logged = len(finished)

    results = []

    for index in range(len(tasks)):
        key = checkpoint_prefix(run) + str(index).zfill(5) + ".pickle"
        results.append(pickle.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()))

    pairs: Rows = []

    for part in range(REDUCERS):
        counter = mapreduce_job.reduce_task([result["partitions"][part] for result in results])
        pairs += [(tuple(str(skill) for skill in combo), count) for combo, count in counter.items() if len(combo) == 2]

    pairs.sort(key=lambda item: (-item[1], item[0]))

    return pairs


def wait_for_session(zk: KazooClient) -> None:
    for attempt in range(150):
        try:
            zk.sync("/")
            return
        except KazooException:
            time.sleep(0.2)


def publish(zk: KazooClient, s3: BaseClient, run: str, epoch: int, fencing: bool, pairs: Rows) -> None:
    key = result_prefix(run) + name + "-epoch" + str(epoch) + ".csv"
    s3.put_object(Bucket=BUCKET, Key=key, Body=pairs_csv(pairs))
    pointer = json.dumps({"key": key, "coordinator": name, "epoch": epoch}).encode()
    wait_for_session(zk)

    try:
        if fencing:
            transaction = zk.transaction()
            transaction.check(node(run, "epoch"), version=epoch)
            transaction.set_data(node(run, "result"), pointer)
            results = transaction.commit()

            for item in results:
                if isinstance(item, Exception):
                    raise item

            version = results[1].version
        else:
            version = zk.set(node(run, "result"), pointer).version
    except BadVersionError:
        current = zk.get(node(run, "epoch"))[1].version
        log("запись результата отклонена: BadVersionError, моя эпоха " + str(epoch) + ", текущая " + str(current))
        return

    log("записал результат " + key + ", версия указателя " + str(version))


def lead(zk: KazooClient, run: str, fencing: bool) -> None:
    s3 = s3_client()
    epoch = claim_epoch(zk, run)
    leader = {"coordinator": name, "epoch": epoch, "pid": os.getpid()}
    zk.create(node(run, "leader"), json.dumps(leader).encode(), ephemeral=True)
    log("стал лидером, эпоха " + str(epoch))

    started = time.perf_counter()
    pairs = count_pairs(zk, s3, run, epoch)
    same = canonical_hash(pairs) == canonical_hash(read_pairs(s3, HW3_PAIRS))
    log("расчёт закончен за " + str(round(time.perf_counter() - started, 1)) + " с, уникальных пар " + str(len(pairs))
        + ", сумма частот " + str(sum(count for _, count in pairs)) + ", совпадает с ДЗ 3: " + ("да" if same else "нет"))

    publish(zk, s3, run, epoch, fencing, pairs)


def on_state(state: str) -> None:
    log(SESSION_STATES.get(state, state))


def main() -> None:
    global name, log_file
    run = sys.argv[1]
    name = sys.argv[2]
    fencing = os.environ.get("FENCING") == "1"

    (LOGS / run).mkdir(parents=True, exist_ok=True)
    log_file = open(LOGS / run / (name + ".log"), "a", encoding="utf-8")
    logging.basicConfig(stream=log_file, level=logging.WARNING, datefmt="%H:%M:%S",
                        format="%(asctime)s.%(msecs)03d " + name + " kazoo: %(message)s")

    zk = KazooClient(hosts=HOSTS, timeout=SESSION_TIMEOUT)
    zk.add_listener(on_state)
    zk.start()
    log("запущен, pid " + str(os.getpid()) + ", тайм-аут сессии " + str(SESSION_TIMEOUT) + " с, защита по эпохе "
        + ("включена" if fencing else "выключена"))
    log("жду выборов")

    try:
        zk.Election(node(run, "election"), name).run(lead, zk, run, fencing)
    except Exception:
        log(traceback.format_exc())
        raise
    finally:
        zk.remove_listener(on_state)
        zk.stop()
        zk.close()
        log("остановлен")
        log_file.close()


if __name__ == "__main__":
    main()
