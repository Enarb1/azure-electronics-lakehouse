# Databricks notebook source
dbutils.widgets.text("catalog", "electronics")
dbutils.widgets.text("run_date", "")

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../..")))
from src.gold import build_gold

# COMMAND ----------

counts = build_gold(spark, dbutils.widgets.get("catalog"), dbutils.widgets.get("run_date"))
for table, n in counts.items():
    print(f"{table}: {n} rows")