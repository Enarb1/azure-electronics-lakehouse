"""Gold layer: a star schema over the silver tables.

One fact table (``fct_sales``, one row per order line) surrounded by four
dimensions. Deliberately a star and not a snowflake: dimensions are written
only by this pipeline, so there are no update anomalies for normalisation
to prevent, and every attribute an analyst filters on should be one join
away. Normalise for writes, denormalise for reads.

Dimensions are joined on surrogate keys rather than the source's business
keys. The reason is SCD2: once ``dim_customer`` holds two versions of a
customer, ``customerkey`` is no longer unique in it, and joining a fact on
it would match both rows and double the revenue.

Every build function asserts its invariants before writing, so a table that
fails a check never lands.
"""

from pyspark.sql import functions as F

from src.quality import assert_unique_key, assert_no_null_keys, assert_row_count_matches, assert_one_current_version

LINEAGE_DROP = [
    "_rescued_data", "_file_path", "_file_size", "_last_modified_at", "_ingested_at", "_run_date", "_silver_run_date"
]
"""Bronze and silver lineage columns, dropped on the way into gold.

They answer "where did this row come from", which belongs in the layers
that record ingestion. Gold is the layer analysts query, and a
``_file_path`` column in a dimension is noise there.
"""

SCD2_COLS = ["city", "state_code", "state", "zip_code", "country", "continent"]
"""Customer attributes whose changes open a new version instead of updating in place.

These are the ones where the old value was *correct at the time*: someone
really did live in Auberry before moving to Berlin, and a 2024 order should
still report under Auberry.

Everything else about a customer - name, gender, birthday - is treated as
Type 1, because a change there corrects something that was always wrong.
Versioning those too would fill the dimension with a spurious version for
every typo fixed upstream.
"""


def scd2_hash(cols):
    """Build a deterministic 64-bit hash over the given columns.

    Surrogate keys have to be stable across rebuilds. ``row_number()`` is not:
    insert a product with a low business key and every subsequent key shifts,
    while the facts still point at the old ones. A hash depends only on the
    source values, so a full rebuild reproduces exactly the same keys.

    Two details carry the correctness:

    * ``concat_ws`` with a separator - without one, ``("ab", "c")`` and
      ``("a", "bc")`` hash identically.
    * ``coalesce`` to an empty string - ``concat_ws`` skips nulls rather than
      leaving a gap, which would shift every following value into the wrong
      position.

    Args:
        cols: Column names to hash, in a fixed order. For an SCD2 key this must
            include both the business key and the version-defining attributes.

    Returns:
        A Column expression producing a bigint.
    """
    return F.xxhash64(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]))

def build_dim_date(spark, catalog):
    """Build ``gold.dim_date``, one row per calendar day.

    Every fact already carries its dates, so this table earns its place for
    three other reasons: attributes SQL cannot derive (public holidays, a
    fiscal year that does not start in January), consistency (otherwise every
    analyst writes their own quarter and week logic, one of them uses ISO
    weeks, and two reports disagree), and completeness - grouping the fact by
    month only returns months that had sales, so a month with none vanishes
    entirely, and a missing row is far harder to notice than a zero.

    The range is a fixed constant rather than derived from the data: reference
    data should not shift because someone loaded a typo'd date in 1902. It has
    to cover the dates the facts *reference* - order and delivery dates - not
    birthdays, which live inside a dimension and are never joined on.

    ``date_sk`` is the date itself as ``yyyyMMdd``, so it stays readable in the
    fact table and still sorts chronologically.

    Args:
        spark: Active SparkSession.
        catalog: Catalog to write ``<catalog>.gold.dim_date`` into.

    Returns:
        Row count of the written table.

    Raises:
        ValueError: If ``date_sk`` is not unique.
    """
    df = (
        spark.sql("""
        SELECT explode(
                sequence(
                to_date('2015-01-01'),
                to_date('2026-12-31'),
                interval 1 day
                )
        ) AS full_date
        """)
        .select(
            F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_sk"),
            "full_date",
            F.dayofmonth("full_date").alias("day_of_month"),
            F.date_format("full_date", "EEEE").alias("day_name"),
            F.weekofyear("full_date").alias("week_of_year"),
            F.month("full_date").alias("month_number"),
            F.date_format("full_date", "MMMM").alias("month_name"),
            F.quarter("full_date").alias("quarter"),
            F.year("full_date").alias("year"),
            F.when(
                F.dayofweek("full_date").isin([1, 7]),
                True
            ).otherwise(False).alias("is_weekend"),
        )
    )

    df = assert_unique_key(df, "date_sk", "dim_date")

    df.write.mode("overwrite").saveAsTable(f"{catalog}.gold.dim_date")

    return spark.table(f"{catalog}.gold.dim_date").count()


def build_dim_product(spark, catalog):
    """Build ``gold.dim_product``, one row per product (SCD Type 1).

    Type 1 means each run overwrites the dimension and no history is kept: a
    changed product attribute is treated as a correction, not as a fact about
    the past.

    ``product_sk`` hashes the business key alone, so it is stable across
    rebuilds even as the row's other values change. The natural key stays in
    the table - it is how a product is looked up and how a row traces back to
    silver - it just is not what the fact joins on.

    Args:
        spark: Active SparkSession.
        catalog: Catalog to read ``silver.products`` from and write
            ``gold.dim_product`` into.

    Returns:
        Row count of the written table.

    Raises:
        ValueError: If ``product_sk`` is not unique.
    """
    df = (
        spark.table(f"{catalog}.silver.products")
        .withColumn("product_sk", F.xxhash64(F.col("productkey").cast("string")))
        .drop(*LINEAGE_DROP)
    )

    df = assert_unique_key(df, "product_sk", "dim_product")

    df.write.mode("overwrite").saveAsTable(f"{catalog}.gold.dim_product")

    return spark.table(f"{catalog}.gold.dim_product").count()


def build_dim_store(spark, catalog):
    """Build ``gold.dim_store``, one row per store (SCD Type 1).

    ``store_type`` encodes a fact from the data dictionary: ``storekey = 0`` is
    the online store. The tempting alternative - inferring it from
    ``square_meters`` being null - reads a correlation that happens to hold
    today, and missing source data is far more likely than a second online
    store appearing.

    Args:
        spark: Active SparkSession.
        catalog: Catalog to read ``silver.stores`` from and write
            ``gold.dim_store`` into.

    Returns:
        Row count of the written table.

    Raises:
        ValueError: If ``store_sk`` is not unique.
    """
    df = (
        spark.table(f"{catalog}.silver.stores")
        .withColumn("store_sk", F.xxhash64(F.col("storekey").cast("string")))
        .withColumn("store_type", F.when(F.col("storekey") == 0, "Online").otherwise("Physical"))
        .drop(*LINEAGE_DROP)
    )

    df = assert_unique_key(df, "store_sk", "dim_store")

    df.write.mode("overwrite").saveAsTable(f"{catalog}.gold.dim_store")

    return spark.table(f"{catalog}.gold.dim_store").count()


def init_dim_customer(spark, catalog):
    """Create ``gold.dim_customer`` from scratch as an SCD2 dimension.

    The first-load branch of :func:`upsert_dim_customer`. Every customer starts
    as a single current version.

    ``valid_from`` is a sentinel (1900-01-01) rather than a derived date.
    Deriving it from the customer's first order would assert they lived at that
    address then, which the source never says, and using the load date would
    put every historical order before any version's validity window, so every
    fact would fall out of the point-in-time join. An obviously artificial date
    is the honest option: it does not manufacture precision the data does not
    have.

    ``customer_sk`` hashes the business key *and* the versioned attributes.
    Hash only the location columns and two customers in the same city collide;
    hash only ``customerkey`` and both versions of a customer get the same key,
    which defeats the entire design.

    Args:
        spark: Active SparkSession.
        catalog: Catalog to read ``silver.customers`` from and write
            ``gold.dim_customer`` into.

    Returns:
        Row count of the written table.

    Raises:
        ValueError: If any customer has more than one current row, or if
            ``customer_sk`` is not unique.
    """
    df = (
        spark.table(f"{catalog}.silver.customers")
        .drop(*LINEAGE_DROP)
        .withColumn("customer_sk", scd2_hash(["customerkey"] + SCD2_COLS))
        .withColumn("change_hash", scd2_hash(SCD2_COLS))
        .withColumn("valid_from", F.lit("1900-01-01").cast("date"))
        .withColumn("valid_to", F.lit("9999-12-31").cast("date"))
        .withColumn("is_current", F.lit(True))
    )

    df = assert_one_current_version(df, "customerkey", "dim_customer")
    df = assert_unique_key(df, "customer_sk", "dim_customer")

    df.write.mode("overwrite").saveAsTable(f"{catalog}.gold.dim_customer")

    return spark.table(f"{catalog}.gold.dim_customer").count()


def merge_dim_customer(spark, catalog, run_date):
    """Merge today's customers into the existing SCD2 dimension.

    A changed customer needs *two* operations - close the old row and insert
    the new version - but MERGE performs at most one action per matched source
    row. The way around it is to stage every changed customer twice:

    * one copy with ``merge_key = customerkey``, which matches the current row
      and takes the UPDATE branch, closing it;
    * one copy with ``merge_key = NULL``, which can never match, because NULL
      is not equal to any value - not even to another NULL - so it falls
      through to the INSERT branch and lands as the new current version.

    That is a mechanical use of SQL's NULL semantics, not a flag being read.
    New customers are staged once with a NULL merge key and insert the same
    way. Unchanged customers never enter the staged set at all, so MERGE never
    touches them; comparing ``change_hash`` over :data:`SCD2_COLS` is what
    decides who is in it.

    ``valid_to`` is set to the day *before* ``run_date``, so a closed row ends
    exactly where its replacement begins. A day either way leaves a
    point-in-time join matching two rows or none.

    The insert column list is built from the staged DataFrame's own schema
    rather than written out by hand, so it cannot drift when a column is added.

    Args:
        spark: Active SparkSession.
        catalog: Catalog holding ``silver.customers`` and
            ``gold.dim_customer``.
        run_date: Logical date of the run as "YYYY-MM-DD". New versions open on
            this date; the versions they supersede close the day before.

    Returns:
        Row count of the dimension after the merge. Read back from the table,
        because a MERGE returns a DataFrame of metrics rather than the table.

    Note:
        Because this writes through SQL MERGE, the quality assertions cannot
        run before the write the way they do everywhere else, so a bad merge
        leaves a bad table in place. The production shape is to merge into a
        staging table, validate it, and then swap.
    """
    target_name = f"{catalog}.gold.dim_customer"

    source = (
        spark.table(f"{catalog}.silver.customers")
        .drop(*LINEAGE_DROP)
        .withColumn("change_hash", scd2_hash(SCD2_COLS))
    )

    current = spark.table(target_name).filter("is_current")

    changed = (
        source.alias("s")
        .join(current.alias("t"), "customerkey")
        .filter(F.col("s.change_hash") != F.col("t.change_hash"))
        .select("s.*")
    )

    new = source.join(current, "customerkey", "left_anti")

    staged = (
        changed.withColumn("merge_key", F.col("customerkey"))
        .unionByName(changed.withColumn("merge_key", F.lit(None).cast("int")))
        .unionByName(new.withColumn("merge_key", F.lit(None).cast("int")))
        .withColumn("customer_sk", scd2_hash(["customerkey"] + SCD2_COLS))
        .withColumn("valid_from", F.lit(run_date).cast("date"))
        .withColumn("valid_to", F.lit("9999-12-31").cast("date"))
        .withColumn("is_current", F.lit(True))
    )

    staged.createOrReplaceTempView("staged_customers")

    insert_cols = [c for c in staged.columns if c != "merge_key"]

    col_list = ", ".join(insert_cols)
    val_list = ", ".join(f"s.{c}" for c in insert_cols)

    spark.sql(f"""
    MERGE INTO {target_name} AS t
    USING staged_customers AS s
        ON t.customerkey = s.merge_key AND t.is_current
    WHEN MATCHED THEN UPDATE SET
        t.valid_to = date_sub(to_date('{run_date}'), 1),
        t.is_current = false
    WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list})
""")

    return spark.table(target_name).count()


def upsert_dim_customer(spark, catalog, run_date):
    """Build or update ``gold.dim_customer``, whichever this run needs.

    Branches on whether the table already exists, so one job handles both the
    first run and steady state without a separate bootstrap step.

    ``dim_customer`` is the only gold table that cannot be rebuilt from
    upstream: silver holds just the customer's *current* address, so the SCD2
    history exists nowhere else. Every other gold table can be dropped and
    regenerated; drop this one and the record that someone ever lived in
    Auberry is gone for good.

    Args:
        spark: Active SparkSession.
        catalog: Catalog holding ``silver.customers`` and ``gold``.
        run_date: Logical date of the run as "YYYY-MM-DD". Only used by the
            merge path; the initial load uses a sentinel ``valid_from``.

    Returns:
        Row count of the dimension after the build.
    """
    if spark.catalog.tableExists(f"{catalog}.gold.dim_customer"):
       return merge_dim_customer(spark, catalog, run_date)

    return init_dim_customer(spark, catalog)


def build_fct_sales(spark, catalog):
    """Build ``gold.fct_sales``, one row per order line.

    The grain is the lowest the source provides. Rolling up to one row per
    order would destroy information no later query can recover - revenue by
    product category becomes impossible - and aggregating upwards is always
    available to whoever needs it.

    The table holds keys and measures and nothing else. Names, cities and
    categories are dimension attributes: put a product name in the fact and it
    can only be filtered by string matching.

    ``dim_customer`` is joined *point in time*: on the business key, and on the
    order date falling inside the version's validity window. Those three
    conditions are what ``valid_from``/``valid_to`` exist for, and they are
    what keeps a 2024 order reporting under the address the customer had in
    2024.

    ``dim_date`` is joined twice under different aliases, once for the order
    date and once for the delivery date - one physical table playing two
    logical roles. Collapsing that into a single join would only match same-day
    deliveries.

    The measures are stored rather than computed at query time because a fact
    records what happened, and the price applied to the transaction is part of
    what happened. Recomputing 2016 revenue at today's price means last
    quarter's report stops matching what it said last quarter.

    The final ``select`` names the columns to keep instead of dropping the ones
    to lose: a drop list enumerating forty columns from four dimensions breaks
    silently the moment any dimension gains one.

    Args:
        spark: Active SparkSession.
        catalog: Catalog holding ``silver.sales`` and the gold dimensions.

    Returns:
        Row count of the written table.

    Raises:
        ValueError: If any foreign key is null - an orphan fact, from a join
            that matched nothing - or if the row count differs from
            ``silver.sales``, which means a join matched more than one
            dimension row and quietly duplicated facts.

    Note:
        The source only carries current product prices, so ``revenue_usd``
        freezes the price as at load time rather than at sale time. That is an
        approximation, and worth stating rather than presenting the number as
        more authoritative than it is.
    """
    dim_customer = spark.table(f"{catalog}.gold.dim_customer")
    dim_product = spark.table(f"{catalog}.gold.dim_product")
    dim_store = spark.table(f"{catalog}.gold.dim_store")
    dim_date = spark.table(f"{catalog}.gold.dim_date")

    df = (
        spark.table(f"{catalog}.silver.sales").alias("s")
        .join(dim_customer.alias("c"),
              (F.col("s.customerkey") == F.col("c.customerkey")) &
              (F.col("s.order_date") >= F.col("c.valid_from")) &
              (F.col("s.order_date") <= F.col("c.valid_to")),
              "left"
              )
        .join(dim_product.alias("p"),
              F.col("s.productkey") == F.col("p.productkey"),
              "left"
              )
        .join(dim_store.alias("st"),
              (F.col("s.storekey") == F.col("st.storekey")),
              "left"
              )
        .join(dim_date.alias("od"),
              (F.col("s.order_date") == F.col("od.full_date")),
              "left"
              )
        .join(dim_date.alias("dd"),
              (F.col("s.delivery_date") == F.col("dd.full_date")),
              "left"
              )
        .withColumn("revenue_usd", (F.col("s.quantity") * F.col("p.unit_price_usd")))
        .withColumn("cost_usd", (F.col("s.quantity") * F.col("p.unit_cost_usd")))
        .withColumn("margin_usd", (F.col("revenue_usd") - F.col("cost_usd")))
        .select(
            F.col("s.order_number").alias("order_number"),
            F.col("s.line_item").alias("line_item"),
            F.col("c.customer_sk").alias("customer_sk"),
            F.col("p.product_sk").alias("product_sk"),
            F.col("st.store_sk").alias("store_sk"),
            F.col("od.date_sk").alias("order_date_sk"),
            F.col("dd.date_sk").alias("delivery_date_sk"),
            F.col("s.quantity").alias("quantity"),
            F.col("p.unit_price_usd").alias("unit_price_usd"),
            F.col("p.unit_cost_usd").alias("unit_cost_usd"),
            F.col("revenue_usd"),
            F.col("cost_usd"),
            F.col("margin_usd"),
        )
    )

    df = assert_no_null_keys(df, ["customer_sk", "product_sk", "store_sk", "order_date_sk"], "fct_sales")
    df = assert_row_count_matches(df, spark.table(f"{catalog}.silver.sales").count(), "fct_sales")

    df.write.mode("overwrite").saveAsTable(f"{catalog}.gold.fct_sales")

    return spark.table(f"{catalog}.gold.fct_sales").count()

def build_gold(spark, catalog, run_date):
    """Build the whole gold layer, in dependency order.

    ``fct_sales`` joins all four dimensions, so they have to exist and be
    current before it runs. The order of the returned dict is the order the
    tables are built in, and it is not arbitrary.

    Args:
        spark: Active SparkSession.
        catalog: Catalog to build ``<catalog>.gold`` in.
        run_date: Logical date of the run as "YYYY-MM-DD", used to open and
            close SCD2 versions in ``dim_customer``.

    Returns:
        Table name to row count, for the job notebook to log.

    Raises:
        ValueError: If ``run_date`` is empty, or if any build function's
            quality assertions fail.
    """
    # Return order of the table is important
    if not run_date:
        raise ValueError("run_date is required")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")

    return {
        "dim_date": build_dim_date(spark, catalog),
        "dim_product": build_dim_product(spark, catalog),
        "dim_store": build_dim_store(spark, catalog),
        "dim_customer": upsert_dim_customer(spark, catalog, run_date),
        "fct_sales": build_fct_sales(spark, catalog)
    }
