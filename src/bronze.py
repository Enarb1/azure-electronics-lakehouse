"""Bronze layer: incremental ingestion of the raw CSVs with Auto Loader.

Bronze is an append-only record of what arrived. Nothing is reshaped here
beyond snake_casing the column names; the only additions are lineage
columns describing where each row came from and when it landed.
"""

from pyspark.sql import functions as F

from src.transform import rename_columns
from src.quality import assert_no_mojibake


def ingest_bronze_autoloader(
        spark,
        catalog: str,
        run_date: str,
        entity: str,
        container: str = "bronze",
        options: dict | None = None
):
    """Ingest one entity's landing files into its bronze Delta table.

    Auto Loader (``format("cloudFiles")``) records which files it has already
    processed in a checkpoint, so each run picks up only what is new. Running
    it again with no new files appends nothing, which is what makes the job
    safe to retry unattended.

    ``trigger(availableNow=True)`` borrows streaming's bookkeeping -
    checkpoints, exactly-once, recovery from a crash mid-write - while keeping
    a job that starts, finishes and can be scheduled: it processes everything
    currently available, then stops. ``awaitTermination()`` is what makes this
    function synchronous, since ``writeStream`` returns as soon as the stream
    starts and without it the function would return before any data landed.

    Order matters here. ``rename_columns`` runs on the source columns alone,
    before the lineage columns are added, so it can never strip their leading
    underscore.

    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog catalog name. The table lands as
            ``<catalog>.bronze.<entity>``.
        run_date: Logical date of the run as "YYYY-MM-DD", written to every row
            as ``_run_date``. Handed down from the ADF pipeline parameter, and
            kept mandatory so every row is attributable to a run.
        entity: Entity name. Drives the source folder, the checkpoint path and
            the table name.
        container: ADLS container holding both the landing zone and the
            checkpoints.
        options: Extra reader options, merged over the defaults - e.g.
            ``{"encoding": "ISO-8859-1"}`` for the customers extract.

    Returns:
        Row count of the bronze table after the write. Counted by querying the
        table, because a streaming DataFrame has no ``count()``.

    Raises:
        ValueError: If ``run_date`` is empty, or if the written table contains
            mojibake.

    Note:
        The checkpoint and the table are one piece of state in two places.
        Clearing either alone goes wrong in both directions: an empty
        checkpoint against a full table reprocesses every file into the
        existing rows, and a full checkpoint against an empty table skips the
        files and writes nothing at all. A reset is always both.

        Auto Loader identifies files by *path*, so a file re-delivered under a
        name it has already seen is never picked up. The landing zone's
        contract is immutable files with unique names; a correction has to
        arrive under a new name and be resolved in silver.

        The mojibake check runs against the written table, so bad rows have
        already landed by the time it fails. Acceptable for a layer whose job
        is to record what arrived; catching it before the write would need
        ``foreachBatch``.
    """
    if not run_date:
        raise ValueError("run_date is required")

    source = f"abfss://{container}@learnenarb.dfs.core.windows.net/landing/{entity}/"
    checkpoint = f"abfss://{container}@learnenarb.dfs.core.windows.net/_checkpoints/{entity}/"

    options = {"header": "true", **(options or {})}


    df =  (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{checkpoint}schema/")
        .options(**options)
        .load(source)
    )

    df = rename_columns(df)
    df = df.select(
        "*",
        F.col("_metadata.file_path").alias("_file_path"),
        F.col("_metadata.file_size").alias("_file_size"),
        F.col("_metadata.file_modification_time").alias("_last_modified_at"),
    ).withColumn("_ingested_at", F.current_timestamp()).withColumn("_run_date", F.lit(run_date).cast("date"))

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")

    (
        df.writeStream
        .option("checkpointLocation", f"{checkpoint}write/")
        .trigger(availableNow=True)
        .toTable(f"{catalog}.bronze.{entity}")
        .awaitTermination()
     )

    assert_no_mojibake(spark.table(f"{catalog}.bronze.{entity}"), entity)

    return spark.table(f"{catalog}.bronze.{entity}").count()
