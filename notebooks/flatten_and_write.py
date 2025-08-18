# Databricks notebook
dbutils.widgets.text("endpoint", "teams_box")
dbutils.widgets.text("season", "2024")
dbutils.widgets.text("week", "1")
dbutils.widgets.text("catalog", "cfbd")
dbutils.widgets.text("mode", "explode_all")  # "flatten_only" or "explode_all"

endpoint = dbutils.widgets.get("endpoint")
season   = int(dbutils.widgets.get("season"))
week     = int(dbutils.widgets.get("week"))
catalog  = dbutils.widgets.get("catalog")
mode     = dbutils.widgets.get("mode")

bronze_schema = f"{catalog}.bronze"
silver_schema = f"{catalog}.silver"
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {bronze_schema}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {silver_schema}")

from cfbd.jobs import run_build_silver_flat
target = run_build_silver_flat(spark, silver_schema, bronze_schema, endpoint, season, week, mode)
print(f"Silver table: {target}")
