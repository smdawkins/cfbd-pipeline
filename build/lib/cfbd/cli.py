import argparse
from pyspark.sql import SparkSession

def ingest():
    p = argparse.ArgumentParser()
    p.add_argument("--endpoint", required=True)
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--week", type=int, required=True)
    p.add_argument("--division", default="fbs")
    p.add_argument("--catalog", required=True)
    a = p.parse_args()

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {a.catalog}")
    spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {a.catalog}.bronze")

    from cfbd.jobs import run_ingest_to_bronze
    api_key = dbutils.secrets.get("cfbd", "api-key")
    run_ingest_to_bronze(spark, api_key, f"{a.catalog}.bronze", a.endpoint, a.season, a.week, a.division)

def silver():
    p = argparse.ArgumentParser()
    p.add_argument("--endpoint", required=True)
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--week", type=int, required=True)
    p.add_argument("--catalog", required=True)
    p.add_argument("--mode", choices=["flatten_only","explode_all"], default="explode_all")
    a = p.parse_args()

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {a.catalog}")
    spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {a.catalog}.silver")

    from cfbd.jobs import run_build_silver_flat
    run_build_silver_flat(spark, f"{a.catalog}.silver", f"{a.catalog}.bronze",
                          a.endpoint, a.season, a.week, a.mode)
