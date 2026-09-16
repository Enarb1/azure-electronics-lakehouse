# Databricks notebook source
dbutils.widgets.text("catalog", "electronics")
dbutils.widgets.text("run_date", "")

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../..")))
from src.bronze import ingest_bronze

# COMMAND ----------

ENTITIES = ["sales", "customers", "products", "stores", "exchange_rates"]

catalog = dbutils.widgets.get("catalog")
run_date = dbutils.widgets.get("run_date")

for entity in ENTITIES:
    n = ingest_bronze(spark, catalog, run_date, entity)
    print(f"{entity}: {n} rows")