import duckdb
import pandas as pd

from common import (ACCESS_KEY, BUCKET, CSV_KEY, DICTIONARY_KEY, ENDPOINT, PARQUET_KEY, REGION, SECRET_KEY, Rows,
                    Summary, canonical_hash)

SETUP = ("INSTALL httpfs; LOAD httpfs; "
         "CREATE OR REPLACE SECRET rustfs (TYPE s3, KEY_ID '" + ACCESS_KEY + "', SECRET '" + SECRET_KEY + "', "
         "ENDPOINT '" + ENDPOINT + "', URL_STYLE 'path', USE_SSL false, REGION '" + REGION + "')")

CSV = "read_csv('s3://" + BUCKET + "/" + CSV_KEY + "', header = true, all_varchar = true)"
PARQUET = "read_parquet('s3://" + BUCKET + "/" + PARQUET_KEY + "/*.parquet')"
DICTIONARY = "read_parquet('s3://" + BUCKET + "/" + DICTIONARY_KEY + "')"

NAME_GO = ("'(^|[^a-zа-яё0-9_])(g[oо]([- ]?l[aа]n[gd])?|г[oо]л[aаэе]нг)([^a-zа-яё0-9_]|$)"
           "|(^|[^a-zа-яё0-9_])г[oо][- ](разработчик|программист|developer)"
           "|(разработчик|программист|developer)( на)? г[oо]([^a-zа-яё0-9_]|$)'")
SKILL_GO = ("'^(язык программирования |программирование на |(отличное )?знание (языка программирования )?)?"
            "(g[oо]([- ]?l[aа]ng)?|г[oо](л[aаэе]нг)?)$'")
OTHER_LANGUAGE = ("'(^|[^a-zа-яё0-9_])(python|питон|php|java|javascript|typescript|js|ts|node|nodejs|react|vue|"
                  "angular|frontend|фронтенд|kotlin|scala|ruby|rust|swift|ios|android|flutter|dart|delphi|perl|"
                  "elixir|erlang|haskell|clojure|lua|unity|bitrix|битрикс|abap|sap|1с|1c)([^a-zа-яё0-9_]|$)"
                  "|c#|c\\+\\+|\\.net|с#|с\\+\\+'")

CSV_DEVELOPERS = """
CREATE OR REPLACE TEMP TABLE scan AS
SELECT count(*) AS total_rows,
       list({'vacancy_id': vacancy_id, 'name': coalesce(name, ''), 'key_skills': coalesce(key_skills, '')})
           FILTER (WHERE role = 'Программист, разработчик') AS developers
FROM CSV;
CREATE OR REPLACE TEMP TABLE developers AS SELECT unnest(developers, recursive := true) FROM scan;
"""

PARQUET_DEVELOPERS = """
CREATE OR REPLACE TEMP TABLE developers AS
SELECT vacancy_id, name, SKILLS_COLUMN
FROM PARQUET
WHERE role = 'Программист, разработчик';
"""

STRING_SKILLS = """
CREATE OR REPLACE TEMP TABLE normalized AS
SELECT vacancy_id, name,
       list_distinct(list_filter(
           list_transform(string_split(lower(key_skills), ';'), lambda x:
               CASE WHEN regexp_matches(trim(x, ' '), SKILL_GO) THEN 'go' ELSE trim(x, ' ') END),
           lambda x: x <> '')) AS skills
FROM developers;
"""

DICTIONARY_TABLE = "CREATE OR REPLACE TEMP TABLE dictionary AS SELECT skill_id, skill FROM DICTIONARY"

ID_SKILLS = """
CREATE OR REPLACE TEMP TABLE normalized AS
SELECT vacancy_id, name,
       list_distinct(list_transform(skill_ids, lambda x:
           CASE WHEN list_contains(GO_SYNONYMS, x) THEN GO_ID ELSE x END)) AS skills
FROM developers;
"""

VARIANT = """
CREATE OR REPLACE TEMP TABLE go AS
SELECT * FROM normalized
WHERE regexp_matches(lower(name), NAME_GO)
   OR (list_contains(skills, GO_SKILL) AND NOT regexp_matches(lower(name), OTHER_LANGUAGE));
CREATE OR REPLACE TEMP TABLE s AS SELECT vacancy_id, unnest(skills) AS skill FROM go;
"""

COMBOS = {
    1: "SELECT skill AS skill_1, count(*) AS vacancies FROM s GROUP BY ALL",
    2: """SELECT a.skill AS skill_1, b.skill AS skill_2, count(*) AS vacancies
          FROM s a JOIN s b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
          GROUP BY ALL""",
    3: """SELECT a.skill AS skill_1, b.skill AS skill_2, c.skill AS skill_3, count(*) AS vacancies
          FROM s a JOIN s b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
                   JOIN s c ON b.vacancy_id = c.vacancy_id AND b.skill < c.skill
          GROUP BY ALL""",
}

ETL = """
CREATE OR REPLACE TEMP TABLE vacancies AS
SELECT vacancy_id, source, status, TRY_CAST(published_at AS DATE) AS published_at, area, employer_id,
       coalesce(name, '') AS name, role,
       TRY_CAST(salary_from AS DOUBLE) AS salary_from, TRY_CAST(salary_to AS DOUBLE) AS salary_to,
       currency, gross, experience, schedule, employment, coalesce(key_skills, '') AS key_skills,
       list_distinct(list_filter(
           list_transform(string_split(lower(coalesce(key_skills, '')), ';'), lambda x: trim(x, ' ')),
           lambda x: x <> '')) AS skills
FROM CSV
WHERE source = 'hh';

CREATE OR REPLACE TEMP TABLE dictionary AS
SELECT skill, count(*) AS vacancies, (row_number() OVER (ORDER BY count(*) DESC, skill) - 1)::INTEGER AS skill_id
FROM (SELECT unnest(skills) AS skill FROM vacancies)
GROUP BY skill;

COPY (SELECT skill_id, skill, vacancies FROM dictionary ORDER BY skill_id)
TO 'SKILLS_TARGET' (FORMAT parquet);

COPY (
    SELECT v.* EXCLUDE (skills), coalesce(ids.skill_ids, []::INTEGER[]) AS skill_ids
    FROM vacancies v
    LEFT JOIN (
        SELECT vacancy_id, list(skill_id ORDER BY skill_id) AS skill_ids
        FROM (SELECT vacancy_id, unnest(skills) AS skill FROM vacancies) JOIN dictionary USING (skill)
        GROUP BY vacancy_id) ids USING (vacancy_id)
    ORDER BY role, vacancy_id
) TO 'VACANCIES_TARGET' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 122880,
                               FILE_SIZE_BYTES '256MB', OVERWRITE_OR_IGNORE true);
"""


def connect() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute(SETUP)
    return connection


def fill(sql: str) -> str:
    sql = sql.replace("NAME_GO", NAME_GO).replace("SKILL_GO", SKILL_GO).replace("OTHER_LANGUAGE", OTHER_LANGUAGE)
    sql = sql.replace("SKILLS_TARGET", "s3://" + BUCKET + "/" + DICTIONARY_KEY)
    sql = sql.replace("VACANCIES_TARGET", "s3://" + BUCKET + "/" + PARQUET_KEY)
    return sql.replace("CSV", CSV).replace("PARQUET", PARQUET).replace("DICTIONARY", DICTIONARY)


def convert() -> int:
    connection = connect()
    connection.execute(fill(ETL))
    skills = connection.execute("SELECT count(*) FROM dictionary").fetchall()[0][0]
    connection.close()
    return skills


def run(source: str = "csv", out_prefix: str | None = None) -> tuple[Summary, dict[int, Rows]]:
    connection = connect()
    summary: Summary = {"source": source}
    names: dict[int, str] | None = None
    if source == "csv":
        connection.execute(fill(CSV_DEVELOPERS))
        summary["rows"] = connection.execute("SELECT total_rows FROM scan").fetchall()[0][0]
        connection.execute(fill(STRING_SKILLS))
        connection.execute(fill(VARIANT.replace("GO_SKILL", "'go'")))
    elif source == "parquet":
        connection.execute(fill(PARQUET_DEVELOPERS.replace("SKILLS_COLUMN", "key_skills")))
        connection.execute(fill(STRING_SKILLS))
        connection.execute(fill(VARIANT.replace("GO_SKILL", "'go'")))
    else:
        connection.execute(fill(PARQUET_DEVELOPERS.replace("SKILLS_COLUMN", "skill_ids")))
        connection.execute(fill(DICTIONARY_TABLE))
        names = dict(connection.execute("SELECT skill_id, skill FROM dictionary").fetchall())
        go_id = [skill_id for skill_id, skill in names.items() if skill == "go"][0]
        synonyms = connection.execute(fill("SELECT list(skill_id) FROM dictionary WHERE regexp_matches(skill, SKILL_GO)")).fetchall()[0][0]
        connection.execute(fill(ID_SKILLS.replace("GO_SYNONYMS", str(synonyms)).replace("GO_ID", str(go_id))))
        connection.execute(fill(VARIANT.replace("GO_SKILL", str(go_id))))
    rows: dict[int, Rows] = {}
    for size in (1, 2, 3):
        result = connection.execute(COMBOS[size]).fetchall()
        if names is None:
            rows[size] = [(tuple(row[:-1]), row[-1]) for row in result]
        else:
            rows[size] = [(tuple(sorted(names[skill_id] for skill_id in row[:-1])), row[-1]) for row in result]
        rows[size].sort(key=lambda item: (-item[1], item[0]))
    if out_prefix is not None:
        for size in rows:
            frame = pd.DataFrame([list(combo) + [count] for combo, count in rows[size]],
                                 columns=["skill_" + str(position) for position in range(1, size + 1)] + ["vacancies"])
            connection.register("frame", frame)
            connection.execute("COPY frame TO 's3://" + BUCKET + "/" + out_prefix + "/skills_k" + str(size) + ".csv' (HEADER)")
            connection.unregister("frame")
    summary["variant_vacancies"], summary["with_skills"] = connection.execute(
        "SELECT count(*), count(*) FILTER (WHERE len(skills) > 0) FROM go").fetchall()[0]
    summary["unique"] = {size: len(rows[size]) for size in rows}
    summary["total_count"] = {size: sum(count for _, count in rows[size]) for size in rows}
    summary["sha256"] = {size: canonical_hash(rows[size]) for size in rows}
    connection.close()
    return summary, rows
