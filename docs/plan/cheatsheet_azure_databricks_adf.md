# Cheat Sheet — Azure + Databricks + ADF + PySpark

Written for the *why*, not just the *how*. Skim it before each project day; re-read the "Best practice" boxes after.

---

## 1. The mental map

Three separate systems that meet at two points:

```
┌──────────────── AZURE (the cloud account) ─────────────────┐
│  Subscription                                              │
│   └─ Resource group  rg-learn-de                           │
│       ├─ Storage account (ADLS Gen2)  ← where data lives   │
│       ├─ Key Vault                    ← where secrets live │
│       ├─ Access Connector / Managed Identity ← who you are │
│       ├─ Data Factory                 ← what starts things │
│       └─ Azure Databricks workspace   ← where compute runs │
└────────────────────────────────────────────────────────────┘
        │                                     │
   meeting point 1:                     meeting point 2:
   ADF triggers a Databricks Job        Databricks reads ADLS
   (identity + job id)                  via a storage credential
```

**The one sentence version:** ADF says *when*, Databricks says *how*, ADLS holds *what*, and managed identities decide *who is allowed*.

---

## 2. Azure vocabulary

| Term | What it actually is | Why you care |
|---|---|---|
| **Tenant** | Your Entra ID (Azure AD) directory — the identity boundary | Users, groups, service principals live here |
| **Subscription** | The billing boundary | Your $100 student credit sits on one |
| **Resource group** | A folder for resources, with a lifecycle | Deleting it deletes everything inside — your cleanup button |
| **Region** | Physical datacenter location | Keep everything in one region; cross-region egress costs money |
| **Resource** | Any deployed thing (storage account, ADF, workspace) | Each has a unique *resource ID* path |
| **ARM / Bicep / Terraform** | Infrastructure as code | How the real environment was built — ask which one your team uses |

### Storage: Blob vs ADLS Gen2
A **storage account** is the top-level resource. Turning on **hierarchical namespace** makes it **ADLS Gen2**: real directories, atomic renames, POSIX-like ACLs. Without it you get flat blob storage where "folders" are just name prefixes, and Spark operations like rename-on-commit become slow and unsafe.

Path form you will type constantly:
```
abfss://<container>@<storageaccount>.dfs.core.windows.net/<path>
   │        │                  │
   │        │                  └── always .dfs for ADLS Gen2 (.blob = the old API)
   │        └── the container, e.g. raw / bronze / silver / gold
   └── Azure Blob File System, Secure (TLS)
```

### Identity — the part that confuses everyone
| Thing | Use it when |
|---|---|
| **Account key / SAS token** | Almost never. It's a password in a config file |
| **Service principal** | An app identity with a client secret — works, but the secret must be rotated and stored in Key Vault |
| **Managed identity** | An identity Azure manages for you, with **no secret at all**. Preferred |
| **Access Connector for Azure Databricks** | A managed identity specifically for Databricks → storage. This is the modern, recommended path |
| **RBAC role** | *What* an identity can do. For data access you want **Storage Blob Data Contributor**, not "Contributor" (which is control-plane only and a classic gotcha) |

> **Best practice — no secrets in code, ever.** The chain should be: managed identity → RBAC role → storage credential. If you must use a secret, it goes in **Key Vault** and is referenced by name. Anything pasted into a notebook ends up in Git forever.

---

## 3. Databricks vocabulary

| Term | What it is |
|---|---|
| **Workspace** | Your Databricks environment — notebooks, jobs, users |
| **Unity Catalog (UC)** | The governance layer: one metastore, three-level naming, permissions, lineage |
| **Three-level namespace** | `catalog.schema.table` — e.g. `dev.silver.orders`. Learn to always fully qualify |
| **Managed table** | Databricks owns the data files; `DROP TABLE` deletes the data |
| **External table** | You own the files at an `abfss://` path; `DROP TABLE` removes only metadata |
| **Volume** | UC-governed storage for *non-tabular* files (CSVs, models, images) |
| **Storage credential** | The identity UC uses to reach cloud storage |
| **External location** | A path + a storage credential = a grantable object |
| **Delta Lake** | Parquet + a transaction log. Gives ACID, time travel, MERGE, schema enforcement |

### Compute types — know which one you're on
| Type | Use | Cost behaviour |
|---|---|---|
| **Serverless** | Notebooks and jobs, starts in seconds | Pay per second of use; nothing to leave running |
| **Job compute (classic)** | Scheduled jobs; cluster created then destroyed | Cheaper DBU rate, ~5 min startup |
| **All-purpose (interactive)** | Development | Most expensive; **always set auto-termination** |
| **SQL warehouse** | BI / SQL queries, Power BI | Separate lifecycle; also set auto-stop |

> **Best practice — don't develop on job compute or schedule on all-purpose compute.** Interactive clusters left running overnight are the #1 way juniors burn cloud budget.

### Delta commands worth memorising
```sql
DESCRIBE DETAIL   my.table;   -- size, file count, location, format
DESCRIBE HISTORY  my.table;   -- every version, who, what operation, metrics
SELECT * FROM my.table VERSION AS OF 3;      -- time travel
SELECT * FROM my.table TIMESTAMP AS OF '2026-09-01';
OPTIMIZE my.table;            -- compact small files
VACUUM  my.table RETAIN 168 HOURS;  -- delete old files (breaks time travel past that)
```

```python
# The workhorse: MERGE / upsert
(DeltaTable.forName(spark, "silver.customers").alias("t")
   .merge(updates.alias("s"), "t.customer_id = s.customer_id")
   .whenMatchedUpdateAll()
   .whenNotMatchedInsertAll()
   .execute())
```

> **Best practice — MERGE is what makes a pipeline re-runnable.** If your job can be run twice with the same input and produce the same table, it's *idempotent*. Aim for that on every table you write.

### Auto Loader (incremental file ingestion)
```python
(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{ckpt}/schema")
    .schema(explicit_schema)
    .load(src)
  .writeStream
    .option("checkpointLocation", ckpt)
    .trigger(availableNow=True)      # batch-style: process what's there, then stop
    .toTable("bronze.orders"))
```
The **checkpoint** remembers which files were already processed. It is state — treat it as part of the pipeline, know its path, and don't delete it casually.

### Jobs / Workflows
- A **job** = tasks + dependencies + schedule + compute + parameters.
- **Job parameters** reach your notebook through `dbutils.widgets.get("catalog")`.
- Set **retries**, a **timeout**, and **failure notifications** on every scheduled job. A silent job is a broken job.

### Things to recognise by name (don't need to master yet)
- **Databricks Asset Bundles** — code + job definitions as YAML (`databricks.yml`), deployed per environment. How teams do CI/CD.
- **Git folders (Repos)** — the workspace pointed directly at a Git branch.
- **Lakeflow Declarative Pipelines / DLT** — you declare tables and expectations; Databricks manages the DAG and incremental logic.
- **Databricks Connect** — you already use this: run local code against remote compute.

---

## 4. Azure Data Factory vocabulary

The whole object model, which is smaller than it looks:

| Object | Meaning | Analogy |
|---|---|---|
| **Linked service** | A connection to a system (storage, Databricks, SQL) | A connection string |
| **Dataset** | The shape/location of data within a linked service | A table or file definition |
| **Activity** | One unit of work (Copy, Lookup, ForEach, Databricks Job…) | A step |
| **Pipeline** | An ordered set of activities | A DAG |
| **Trigger** | What starts a pipeline: schedule, tumbling window, event, manual | Cron |
| **Integration Runtime (IR)** | The compute that executes activities. *AutoResolve* for cloud-to-cloud; *Self-hosted* for on-prem sources | The worker |

### Activities you'll see constantly
- **Copy** — move bytes A→B. Does not transform. Don't make it transform.
- **Lookup** — read a small config/control table into the pipeline.
- **ForEach** — loop over an array (set `batchCount` for parallelism).
- **Get Metadata** + **If Condition** — existence checks and branching.
- **Databricks Job** — trigger an existing Databricks Workflow, including serverless ones. **This is the current recommended way to call Databricks from ADF**, better than the older Notebook activity, because the compute and task definition stay in Databricks where they're version-controlled.
- **Web** — call a REST API (alerts, Teams notification).
- **Set Variable / Append Variable** — pipeline-scoped state.

### Expression language (you will need this)
```
@pipeline().parameters.run_date
@pipeline().RunId
@utcNow('yyyy-MM-dd')
@activity('LookupFiles').output.value
@item()                                 -- current element inside ForEach
@concat('raw/', pipeline().parameters.entity, '/')
@formatDateTime(utcNow(), 'yyyy/MM/dd')
```

### Parameters vs variables
- **Parameters** are set *when the pipeline starts* and are immutable. Use them for everything configurable.
- **Variables** change *during* the run (`Set Variable`). Use sparingly — they're a common source of race conditions inside parallel ForEach loops.

> **Best practice — one parameterised pipeline beats twenty copies.** If you find yourself cloning a pipeline and changing a file name, stop and parameterise instead. Same instinct as not copy-pasting a function.

> **Best practice — Debug ≠ Trigger.** Debug runs your unpublished draft; triggers run the last *published* version. Always publish before you test a schedule.

---

## 5. PySpark quick reference

```python
from pyspark.sql import functions as F, Window as W
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType

df = spark.read.schema(schema).option("header", True).csv(path)   # explicit schema, always

df.select("a", "b").filter(F.col("qty") > 0)
  .withColumn("total", F.col("price") + F.col("freight"))
  .withColumn("ingested_at", F.current_timestamp())

df.groupBy("category").agg(F.sum("total").alias("revenue"),
                           F.countDistinct("order_id").alias("orders"))

w = W.partitionBy("customer_id").orderBy(F.col("order_ts").desc())
df.withColumn("rn", F.row_number().over(w)).filter("rn = 1")      # latest per customer

df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.orders")
```

### Join types and what they're *for*
| Join | Use |
|---|---|
| `inner` | Keep only matches |
| `left` | Enrich without losing rows — check for nulls afterwards |
| `left_semi` | Filter A by existence in B, **no columns added, no row multiplication** |
| `left_anti` | Find orphans / unmatched records — your referential-integrity check |

### Performance rules of thumb
1. **Narrow** transformations (filter, select, withColumn) are cheap. **Wide** ones (join, groupBy, distinct, orderBy) cause a **shuffle** — that's where time goes.
2. Filter and select **before** the shuffle, not after.
3. Broadcast the small side of a join (Spark usually does automatically; `F.broadcast(df)` forces it).
4. Prefer built-in functions over Python UDFs — UDFs break Spark's optimiser and serialise row by row.
5. `collect()` pulls everything to the driver. On real data that's an OOM.
6. `repartition()` = full shuffle, can increase partitions. `coalesce()` = no shuffle, only reduces.
7. `explain("formatted")` before you guess. Look for `Exchange` (shuffle) and `BroadcastHashJoin`.
8. **AQE** fixes many skew and partition-size problems at runtime — check `isFinalPlan=true` in the plan.

> **Best practice — explicit schemas, always.** `inferSchema` reads the file twice, guesses types, and silently changes behaviour when the source changes. Explicit schemas turn a data problem into a loud failure at ingestion, which is where you want it.

---

## 6. The best-practice concepts that matter most

**Medallion layers — why they exist**
- **Bronze**: raw, append-only, as-delivered, plus lineage columns (`_source_file`, `_ingested_at`). Never clean here. It's your ability to reprocess.
- **Silver**: cleaned, typed, deduplicated, conformed, quality-flagged. One row per business entity.
- **Gold**: modelled for consumption — facts, dimensions, aggregates. This is what BI touches.

The value is *replayability*: if Silver logic is wrong, you fix the code and rebuild from Bronze. If you'd cleaned on ingest, the original is gone.

**Idempotency**
Re-running a job must not change the result. Achieved with MERGE on a business key, overwrite of a bounded partition, or Auto Loader checkpoints. Ask of every job you write: *what happens if this runs twice?*

**Incremental over full refresh**
Full reloads are fine at 100 MB and fatal at 1 TB. Know your watermark (a modified timestamp, a file arrival, a high-water-mark ID) and store it somewhere durable.

**Schema enforcement and evolution**
Delta rejects mismatched writes by default — that's a feature. Evolve deliberately (`ALTER TABLE ADD COLUMNS` or an explicit evolution option), never by accident.

**Data quality as code**
Don't just observe quality — *act* on it. Split valid vs rejected rows with a reason column (you already do this — it's genuinely good practice), and fail the job on assertions that should never break: no duplicate keys, no orphan foreign keys, row counts within tolerance.

**Separation of concerns**
Orchestrator orchestrates; transformation transforms. Business logic in ADF expressions is unreadable and untestable. Keep it in PySpark, in Git.

**Parameterise everything environment-specific**
Catalog names, paths, dates. `dev` / `test` / `prod` should be the same code with different parameters. Hard-coded `workspace.silver.orders` is a thing you'll have to fix later.

**Naming conventions**
Pick one and hold it: `rg-`, `st`, `adf-`, `dbw-`, `kv-` for Azure resources; `bronze_`/`dim_`/`fct_` for tables; `PL_`, `LS_`, `DS_` for ADF objects. Consistency is what makes 200 objects navigable.

**Cost hygiene**
Auto-terminate everything. Prefer serverless for bursty work. Set budget alerts. Look at the cost blade weekly. A junior who thinks about cost gets noticed for the right reasons.

**Observability**
Every scheduled job needs retries, a timeout, and a failure notification. Log row counts in and out. "It ran" isn't the same as "it worked."

---

## 7. Errors you will meet, and what they mean

| Symptom | Likely cause |
|---|---|
| `403` / `AuthorizationPermissionMismatch` on `abfss://` | Identity lacks **Storage Blob Data Contributor** on the storage account (the `Contributor` role is not enough) |
| `Path does not exist` but it's clearly there | Wrong container name, `.blob` instead of `.dfs`, or hierarchical namespace was never enabled |
| Databricks can't create the external location | No storage credential yet, or the access connector has no role assignment |
| ADF pipeline works in Debug, fails on trigger | You never published |
| ADF Databricks activity fails instantly | Expired PAT, or the managed identity lacks `CAN MANAGE RUN` on the job |
| Job reprocesses everything unexpectedly | Checkpoint directory deleted or moved |
| Row count explodes after a join | One-to-many join on a non-unique key — check grain before and after |
| `AnalysisException: schema mismatch` on write | Delta schema enforcement doing its job. Decide: fix the data, or evolve the schema deliberately |
| Job is slow with no obvious cause | Skew or tiny files. Check `DESCRIBE DETAIL` for file count, run `OPTIMIZE`, look for `Exchange` in the plan |
| Databricks Free Edition can't reach an external service | Free Edition restricts outbound network access to a limited set of trusted domains |

---

## 8. Command quick reference

```bash
# Azure CLI
az login
az account show
az group create -n rg-learn-de -l westeurope
az storage blob upload-batch -d raw -s ./data/raw --account-name <acct> --auth-mode login
az group delete -n rg-learn-de --yes          # the cleanup button

# Databricks CLI
databricks auth login --host https://adb-xxxx.azuredatabricks.net
databricks fs ls dbfs:/
databricks jobs list
databricks jobs run-now --job-id 123
databricks bundle init
```

---

## 9. A day-one navigation checklist

When you sit down at the new job, find these five things in the first week:

1. **Where the data physically is** — which storage accounts, which containers, managed or external tables.
2. **What starts the pipelines** — ADF, Databricks Workflows, Airflow, or something custom.
3. **How code ships** — repo, branch strategy, whether asset bundles or CI/CD exist.
4. **How to tell dev from prod** — catalog names, workspace URLs, subscription names.
5. **Where the logs are** — ADF monitoring, Databricks job runs, and whoever gets paged.

Everything else is detail you'll pick up as you go.
