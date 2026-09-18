from pyspark.sql import functions as F

from src.clean import type_sales, type_customers, type_products, type_stores, type_exchange_rates, assert_no_rescued_data


CLEAN_MAPPER = {
    "sales": type_sales,
    "customers": type_customers,
    "products": type_products,
    "stores": type_stores,
    "exchange_rates": type_exchange_rates,
}

def build_silver(spark, catalog: str, run_date: str, entity: str):
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