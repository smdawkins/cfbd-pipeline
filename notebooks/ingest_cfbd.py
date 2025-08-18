# Databricks notebook
dbutils.widgets.text("endpoint", "teams")
dbutils.widgets.text("season", "2024")
dbutils.widgets.text("week", "1")
dbutils.widgets.text("division", "fbs")
dbutils.widgets.text("catalog", "cfbd")

endpoint = dbutils.widgets.get("endpoint")
season   = int(dbutils.widgets.get("season"))
week     = int(dbutils.widgets.get("week"))
division = dbutils.widgets.get("division")
catalog  = dbutils.widgets.get("catalog")

bronze_schema = f"{catalog}.bronze"
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {bronze_schema}")

from cfbd.jobs import run_ingest_to_bronze
api_key = dbutils.secrets.get("cfbd", "api_key")  # scope 'cfbd', key 'api_key'

tbl = run_ingest_to_bronze(spark, api_key, bronze_schema, endpoint, season, week, division)
print(f"Bronze table: {tbl}")
