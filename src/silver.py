"""Silver layer: typed and cleaned, one table per entity.

Silver reads from the bronze *tables*, never from the source files. Each
layer's only input is the layer below, and that is what makes the whole
chain replayable from the raw landing zone.
"""

from pyspark.sql import functions as F

from src.clean import type_sales, type_customers, type_products, type_stores, type_exchange_rates
from src.quality import assert_no_rescued_data

CLEAN_MAPPER = {
    "sales": type_sales,
    "customers": type_customers,
    "products": type_products,
    "stores": type_stores,
    "exchange_rates": type_exchange_rates,
}
"""Entity name to the function that types and cleans it, from :mod:`src.clean`.

An unmapped entity raises ``KeyError`` in :func:`build_silver`, which is
the wanted behaviour: a newly added entity has to be given an explicit
cleaning function rather than drifting through untyped.
"""


def build_silver(spark, catalog: str, run_date: str, entity: str):
    """Build one entity's silver table from its bronze table.

    ``mode("overwrite")`` is correct here. Silver is derived entirely from
    bronze, so a full rebuild is deterministic and lands the same table every
    time. Merge is only needed where a layer holds state its upstream does not
    have, which in this pipeline is ``gold.dim_customer`` and its SCD2 history.

    Lineage columns travel unchanged rather than being rewritten per layer.
    ``_file_path`` and ``_ingested_at`` describe a row's *origin*, so
    overwriting them with this layer's details would leave only one hop of
    history visible. ``_file_size`` and ``_last_modified_at`` are dropped -
    they describe the file, not the row - and ``_silver_run_date`` is added as
    a new column, not as a replacement for ``_run_date``.

    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog catalog name. Reads
            ``<catalog>.bronze.<entity>`` and writes
            ``<catalog>.silver.<entity>``.
        run_date: Logical date of the run as "YYYY-MM-DD", written to every row
            as ``_silver_run_date``.
        entity: Entity name. Must be a key of :data:`CLEAN_MAPPER`.

    Returns:
        Row count of the silver table after the write.

    Raises:
        ValueError: If ``run_date`` is empty, or if bronze rescued any data,
            which means the source changed shape.
        KeyError: If ``entity`` has no cleaning function.
    """
    if not run_date:
        raise ValueError("run_date is required")

    df = spark.table(f"{catalog}.bronze.{entity}")
    df = df.drop("_file_size", "_last_modified_at")
    df = CLEAN_MAPPER[entity](df)
    df = df.withColumn("_silver_run_date", F.lit(run_date).cast("date"))
    df = assert_no_rescued_data(df, entity)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
    df.write.mode("overwrite").saveAsTable(f"{catalog}.silver.{entity}")

    return spark.table(f"{catalog}.silver.{entity}").count()