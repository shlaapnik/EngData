import polars as pl
from tqdm.auto import tqdm

from common import (ACCESS_KEY, BUCKET, CSV_KEY, DICTIONARY_KEY, ENDPOINT, PARQUET_KEY, REGION, ROLE, SECRET_KEY,
                    Rows, Summary, canonical_hash)

STORAGE = {"aws_endpoint_url": "http://" + ENDPOINT, "aws_access_key_id": ACCESS_KEY,
           "aws_secret_access_key": SECRET_KEY, "aws_region": REGION, "aws_allow_http": "true"}
CSV = "s3://" + BUCKET + "/" + CSV_KEY
PARQUET = "s3://" + BUCKET + "/" + PARQUET_KEY + "/*.parquet"
DICTIONARY = "s3://" + BUCKET + "/" + DICTIONARY_KEY

NAME_GO = ("(^|[^a-zа-яё0-9_])(g[oо]([- ]?l[aа]n[gd])?|г[oо]л[aаэе]нг)([^a-zа-яё0-9_]|$)"
           "|(^|[^a-zа-яё0-9_])г[oо][- ](разработчик|программист|developer)"
           "|(разработчик|программист|developer)( на)? г[oо]([^a-zа-яё0-9_]|$)")
SKILL_GO = ("^(язык программирования |программирование на |(отличное )?знание (языка программирования )?)?"
            "(g[oо]([- ]?l[aа]ng)?|г[oо](л[aаэе]нг)?)$")
OTHER_LANGUAGE = ("(^|[^a-zа-яё0-9_])(python|питон|php|java|javascript|typescript|js|ts|node|nodejs|react|vue|"
                  "angular|frontend|фронтенд|kotlin|scala|ruby|rust|swift|ios|android|flutter|dart|delphi|perl|"
                  "elixir|erlang|haskell|clojure|lua|unity|bitrix|битрикс|abap|sap|1с|1c)([^a-zа-яё0-9_]|$)"
                  "|c#|c\\+\\+|\\.net|с#|с\\+\\+")


def string_skills() -> pl.Expr:
    return (pl.col("key_skills").fill_null("").str.to_lowercase().str.split(";")
            .list.eval(pl.element().str.strip_chars(" "))
            .list.eval(pl.when(pl.element().str.contains(SKILL_GO)).then(pl.lit("go")).otherwise(pl.element()))
            .list.eval(pl.element().filter(pl.element() != ""))
            .list.unique())


def collect_with_bar(frame: pl.LazyFrame, description: str) -> pl.DataFrame:
    parts = []
    with tqdm(desc=description, unit=" строк") as bar:
        for batch in frame.collect_batches(engine="streaming"):
            parts.append(batch)
            bar.update(batch.height)
    return pl.concat(parts)


def run(source: str = "csv", out_prefix: str | None = None) -> tuple[Summary, dict[int, Rows]]:
    names: dict[int, str] | None = None
    if source == "csv":
        developers = collect_with_bar(pl.scan_csv(CSV, storage_options=STORAGE, infer_schema=False)
                                      .filter(pl.col("role") == ROLE)
                                      .select("vacancy_id", "name", "key_skills"), "polars csv, строк разработчиков")
        developers = developers.with_columns(skills=string_skills())
        go_skill = pl.col("skills").list.contains("go")
    elif source == "parquet":
        developers = collect_with_bar(pl.scan_parquet(PARQUET, storage_options=STORAGE)
                                      .filter(pl.col("role") == ROLE)
                                      .select("vacancy_id", "name", "key_skills"), "polars parquet, строк разработчиков")
        developers = developers.with_columns(skills=string_skills())
        go_skill = pl.col("skills").list.contains("go")
    else:
        dictionary = pl.read_parquet(DICTIONARY, storage_options=STORAGE, columns=["skill_id", "skill"])
        names = dict(zip(dictionary["skill_id"].to_list(), dictionary["skill"].to_list()))
        go_id = dictionary.filter(pl.col("skill") == "go")["skill_id"][0]
        synonyms = dictionary.filter(pl.col("skill").str.contains(SKILL_GO))["skill_id"].to_list()
        developers = collect_with_bar(pl.scan_parquet(PARQUET, storage_options=STORAGE)
                                      .filter(pl.col("role") == ROLE)
                                      .select("vacancy_id", "name", "skill_ids"), "polars parquet_ids, строк разработчиков")
        skills = pl.col("skill_ids").list.eval(
            pl.when(pl.element().is_in(synonyms)).then(pl.lit(go_id)).otherwise(pl.element())).list.unique()
        developers = developers.with_columns(skills=skills)
        go_skill = pl.col("skills").list.contains(go_id)
    name = pl.col("name").fill_null("").str.to_lowercase()
    go = developers.filter(name.str.contains(NAME_GO) | (go_skill & ~name.str.contains(OTHER_LANGUAGE)))
    items = go.select("vacancy_id", pl.col("skills").alias("skill_1")).explode("skill_1").drop_nulls("skill_1")
    second = items.rename({"skill_1": "skill_2"})
    third = items.rename({"skill_1": "skill_3"})
    pairs = items.join(second, on="vacancy_id").filter(pl.col("skill_1") < pl.col("skill_2"))
    triples = pairs.join(third, on="vacancy_id").filter(pl.col("skill_2") < pl.col("skill_3"))
    tables = {1: items.group_by("skill_1").len(),
              2: pairs.group_by("skill_1", "skill_2").len(),
              3: triples.group_by("skill_1", "skill_2", "skill_3").len()}
    rows: dict[int, Rows] = {}
    for size, table in tables.items():
        if names is None:
            rows[size] = [(tuple(row[:-1]), row[-1]) for row in table.iter_rows()]
        else:
            rows[size] = [(tuple(sorted(names[skill_id] for skill_id in row[:-1])), row[-1]) for row in table.iter_rows()]
        rows[size].sort(key=lambda item: (-item[1], item[0]))
    summary = {"source": source, "variant_vacancies": go.height,
               "with_skills": go.filter(pl.col("skills").list.len() > 0).height,
               "unique": {size: len(rows[size]) for size in rows},
               "total_count": {size: sum(count for _, count in rows[size]) for size in rows},
               "sha256": {size: canonical_hash(rows[size]) for size in rows}}
    if out_prefix is not None:
        save(rows, out_prefix)
    return summary, rows


def save(rows: dict[int, Rows], out_prefix: str) -> None:
    for size in rows:
        columns = ["skill_" + str(position) for position in range(1, size + 1)] + ["vacancies"]
        frame = pl.DataFrame([list(combo) + [count] for combo, count in rows[size]], schema=columns, orient="row")
        frame.write_csv("s3://" + BUCKET + "/" + out_prefix + "/skills_k" + str(size) + ".csv", storage_options=STORAGE)
