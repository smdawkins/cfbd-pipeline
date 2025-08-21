import json, datetime
from pyspark.sql import functions as F

AUDIT_SCHEMA = "raw string,  _season int, _week int, _source string"
#AUDIT_SCHEMA = "raw string, _ingest_ts string, _season int, _week int, _source string"

def write_bronze_raw(spark, records, endpoint_name: str, bronze_schema: str, season: int, week: int):
    #ingest_ts = datetime.datetime.utcnow().isoformat()
    rows = [(json.dumps(rec),  int(season), int(week), endpoint_name) for rec in records]
    df = spark.createDataFrame(rows, AUDIT_SCHEMA)
    target = f"{bronze_schema}.{endpoint_name}_raw"
    if not spark.catalog.tableExists(target):
        (df.write
           .format("delta")
           .mode("overwrite")
           .partitionBy("_season","_week")
           .saveAsTable(target))
        return
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("replaceWhere", f"_season = {season} AND _week = {week}")
       .saveAsTable(target))
    return 
