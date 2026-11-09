# ДЗ 4 по инженерии данных. Марковский Артём, 466636, J3310

- `zookeeper.ipynb` ноутбук со всеми этапами по порядку, команды запускаются из него, их вывод виден текстом
- `compose.yaml` три сервера ZooKeeper 3.9.6 в Docker, каждому монтируется свой `zoo.cfg` и отдельный том под dataDir
- `zk1/conf/zoo.cfg`, `zk2/conf/zoo.cfg`, `zk3/conf/zoo.cfg` конфигурации серверов: строки участников одинаковые, clientPort, порты кворума и dataDir свои
- `coordinator.py` координатор расчёта пар навыков из ДЗ 3: выборы, эпоха, чекпоинт в RustFS, публикация результата с защитой по эпохе и без
- `logs/` журналы координаторов по запускам `no-fencing` и `fencing`
- `zookeeper.html` ноутбук в html, его собирает последняя ячейка ноутбука
- `zookeeper.pdf` отчёт (`zookeeper.ipynb` -> html -> pdf)

Расчёт берёт map и reduce из `../MapReduce/mapreduce_job.py`, а данные из RustFS, который поднимает ноутбук ДЗ 3: бакет `hh-lake`, объект `raw/vacancies_2024.csv`

```bash
uv sync
```

потом открыть `zookeeper.ipynb` в VS Code или Jupyter с ядром из `.venv`, ноутбук сам поднимает ансамбль через `docker compose`
