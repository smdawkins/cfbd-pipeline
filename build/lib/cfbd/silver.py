from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType

def infer_schema_from_sample(spark, raw_tbl: str, season: int, week: int):
    sample = (spark.table(raw_tbl)
                  .where((F.col("_season")==season) & (F.col("_week")==week))
                  .select("raw").limit(1).collect())
    if not sample: return None
    sample_json = sample[0]["raw"]
    return (spark.range(1)
                 .select(F.schema_of_json(F.lit(sample_json)).alias("s"))
                 .collect()[0]["s"])

def parse_raw_to_cols(spark, raw_tbl: str, season: int, week: int, schema: StructType):
    return (spark.table(raw_tbl)
            .where((F.col("_season")==season) & (F.col("_week")==week))
            .withColumn("obj", F.from_json(F.col("raw"), schema))
            .select("obj.*", "_ingest_ts","_season","_week","_source"))

def flatten_structs_until_done(df, sep="_"):
    changed = True
    while changed:
        changed = False
        new_cols = []
        for f in df.schema.fields:
            if isinstance(f.dataType, StructType):
                for sub in f.dataType.fields:
                    new_cols.append(F.col(f"{f.name}.{sub.name}").alias(f"{f.name}{sep}{sub.name}"))
                changed = True
            else:
                new_cols.append(F.col(f.name))
        df = df.select(*new_cols)
    return df

def flatten_and_explode_all(df, sep="_"):
    """
    Recursively flatten structs, then explode one array at a time,
    until only primitive columns remain.
    """
    while True:
        # flatten one struct
        flattened = False
        for f in df.schema.fields:
            if isinstance(f.dataType, StructType):
                keep = [c for c in df.columns if c != f.name]
                exp  = [F.col(f"{f.name}.{s.name}").alias(f"{f.name}{sep}{s.name}") for s in f.dataType.fields]
                df = df.select(*keep, *exp)
                flattened = True
                break
        if flattened: continue

        # explode one array
        exploded = False
        for f in df.schema.fields:
            if isinstance(f.dataType, ArrayType):
                df = df.withColumn(f.name, F.explode_outer(F.col(f.name)))
                exploded = True
                break
        if exploded: continue

        return df

from delta.tables import DeltaTable

def upsert_merge(df, target_table: str, keys: list[str], partition_by: list[str] | None = None):
    """
    Upsert df into target_table on 'keys'. Creates the table if it doesn't exist.
    - Keeps all columns from df (update all / insert all)
    - Preserves partitioning you choose on the first create
    """
    spark = df.sparkSession
    table_exists = spark.catalog.tableExists(target_table)

    if not table_exists:
        w = df.write.format("delta").mode("overwrite")
        if partition_by:
            w = w.partitionBy(*partition_by)
        w.saveAsTable(target_table)
        return

    cond = " AND ".join([f"t.{k} = s.{k}" for k in keys])

    non_keys = [c for c in df.columns if c not in keys]
    set_map = {c: F.col(f"s.{c}") for c in non_keys}

    print(cond)
    print (set_map)

    target = DeltaTable.forName(spark, target_table)
    (target.alias("t")
           .merge(df.alias("s"), cond)
           .whenMatchedUpdate(condition=F.col("s._ingest_ts") > F.col("t._ingest_ts"),
         set=set_map)
           .whenNotMatchedInsertAll()
           .execute())

