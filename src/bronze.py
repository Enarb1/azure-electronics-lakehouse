from pyspark.sql import functions as F

from src.transform import rename_columns

def ingest_bronze(spark, catalog: str, run_date: str, entity: str, container: str = "bronze"):
    if not run_date:
        raise ValueError("run_date is required")

    path = f"abfss://{container}@learnenarb.dfs.core.windows.net/landing/{entity}/"

    df = (
        spark.read
        .option("header", True)
        .csv(path)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_file_path"),
            F.col("_metadata.file_size").alias("_file_size"),
            F.col("_metadata.file_modification_time").alias("_last_modified_at"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_date", F.lit(run_date).cast("date"))
    )

    df = rename_columns(df)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
    df.write.mode("overwrite").saveAsTable(f"{catalog}.bronze.{entity}")

    return spark.table(f"{catalog}.bronze.{entity}").count()