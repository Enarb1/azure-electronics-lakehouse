# Databricks notebook source
dbutils.widgets.text("catalog", "electronics")
dbutils.widgets.text("run_date", "")

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../..")))
from src.bronze import ingest_bronze_autoloader

# COMMAND ----------

ENTITIES = {
    "sales":          {},
    "customers":      {"encoding": "ISO-8859-1"},
    "products":       {},
    "stores":         {},
    "exchange_rates": {},
}

catalog = dbutils.widgets.get("catalog")
run_date = dbutils.widgets.get("run_date")

for entity, opts in ENTITIES.items():
    n = ingest_bronze_autoloader(spark, catalog, run_date, entity, options=opts)
    print(f"{entity}: {n} rows")