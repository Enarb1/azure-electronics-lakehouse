from pyspark.sql import functions as F

from src.quality import assert_unique_key, assert_no_null_keys, assert_row_count_matches, assert_one_current_version

LINEAGE_DROP = [
    "_rescued_data", "_file_path", "_file_size", "_last_modified_at", "_ingested_at", "_run_date", "_silver_run_date"
]

SCD2_COLS = ["city", "state_code", "state", "zip_code", "country", "continent"]


def scd2_hash(cols):
    return F.xxhash64(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]))

def build_dim_date(spark, catalog):
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
    df = (
        spark.table(f"{catalog}.silver.products")
        .withColumn("product_sk", F.xxhash64(F.col("productkey").cast("string")))
        .drop(*LINEAGE_DROP)
    )

    df = assert_unique_key(df, "product_sk", "dim_product")

    df.write.mode("overwrite").saveAsTable(f"{catalog}.gold.dim_product")

    return spark.table(f"{catalog}.gold.dim_product").count()


def build_dim_store(spark, catalog):
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
    if spark.catalog.tableExists(f"{catalog}.gold.dim_customer"):
       return merge_dim_customer(spark, catalog, run_date)

    return init_dim_customer(spark, catalog)


def build_fct_sales(spark, catalog):

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
