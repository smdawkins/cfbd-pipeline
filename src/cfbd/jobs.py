from http_connector import build_session, fetch_json
from bronze import write_bronze_raw
from silver import infer_schema_from_sample, parse_raw_to_cols, flatten_structs_until_done, flatten_and_explode_all, upsert_merge
from endpoints import ENDPOINTS
from pyspark.sql import functions as F

BASE_URL = "https://api.collegefootballdata.com"

def run_ingest_to_bronze(spark, api_key: str, bronze_schema: str, endpoint: str, season: int, week: int):
    conf = ENDPOINTS[endpoint]
    url_path = conf["url"]({"season": season, "week": week})
    print(url_path)
    session = build_session(api_key)
    records = fetch_json(session, BASE_URL, url_path)
    if not records:
        print(f"[{endpoint}] no rows for season={season} week={week}")
        return f"{bronze_schema}.{endpoint}_raw"
    return write_bronze_raw(spark, records, conf["target"], bronze_schema, season, week)

def run_build_silver_flat(spark, silver_schema: str, bronze_schema: str, endpoint: str, season: int, week: int, mode: str = "explode_all"):
    conf = ENDPOINTS[endpoint]
    raw_tbl = f"{bronze_schema}.{conf['target']}_raw"
    silver_tbl = f"{silver_schema}.{conf['target']}_flat_rows"
    schema = infer_schema_from_sample(spark, raw_tbl, season, week)
    if not schema:
        print(f"[{endpoint}] no sample to infer schema; skipping")
        return None

    df = parse_raw_to_cols(spark, raw_tbl, season, week, schema)

    if mode == "explode_all":
        flat = flatten_and_explode_all(df)
    else:
        flat = flatten_structs_until_done(df)

    # OPTIONAL: clean up column prefixes for common arrays/structs
    '''for old in list(flat.columns):
        for p in ("teams_", "stats_", "linescores_", "plays_", "events_"):
            if old.startswith(p):
                flat = flat.withColumnRenamed(old, old[len(p):])

    target = f"{silver_schema}.{conf['target']}_{'flat' if mode=='flatten_only' else 'flat_rows'}"
    (flat.write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", True)
         .saveAsTable(target))
    return target'''

    upsert_merge(flat, silver_tbl, keys=["id","teamId","stat_category"], partition_by=["_season","_week"])
