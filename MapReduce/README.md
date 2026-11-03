# ДЗ 3 по инженерии данных. Марковский Артём, 466636, J3310. Вариант: Программист, разработчик на Go

- `mapreduce.ipynb` ноутбук со всеми этапами по порядку и выводами
- `mapreduce_job.py` свой MapReduce: чтение CSV из S3 сплитами через Range GET, map, combiner, партиции, reduce, перезапуск упавших тасков, режимы для Parquet
- `duckdb_job.py` решение на DuckDB и ETL из CSV в Parquet
- `polars_job.py` решение на Polars
- `common.py` общие настройки S3 (адрес, ключи, бакет, пути к объектам), клиент S3 и контрольный хеш результата
- `out/` графики и замеры
- `mapreduce.html` ноутбук в html, его собирает последняя ячейка ноутбука
- `mapreduce.pdf` отчёт (`mapreduce.ipynb` -> html -> pdf)

Запуск: нужен Docker и файл `../big/vacancies_2024.csv`, ноутбук сам поднимает RustFS на 12 папках-дисках в `../rustfs/` и загружает файл в бакет `hh-lake`

```bash
uv sync
```

потом открыть `mapreduce.ipynb` в VS Code или Jupyter с ядром из `.venv`

