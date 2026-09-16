# Day 3 — Git workflow, bronze ingestion, data quality

## Where code lives and why

```
src/                    → logic. Testable, importable, no dbutils.
  bronze.py
  transform.py
  __init__.py           → required for `from src.bronze import ...`
notebooks/
  exploration/*.ipynb   → scratch, run once, a record of how I figured things out
  jobs/*.py             → thin entry points the Job runs
docs/
```

**Rule: `dbutils` stays at the edge.** It's a Databricks-only object injected into notebooks — it doesn't exist in plain Python, so any file importing it can't run or be tested outside Databricks.

```python
# Notebook — the edge. Reads input.
dbutils.widgets.text("catalog", "electronics")
catalog = dbutils.widgets.get("catalog")

# src/ — plain PySpark. Receives values as arguments.
def ingest_bronze(spark, catalog, run_date, entity, options=None):
```

Same function runs in three places unchanged: local notebook (hard-coded constants), Databricks notebook (widgets), Job (job parameters). Only the call site differs.

**Widgets** are input boxes at the top of a notebook. They only appear *after* the `dbutils.widgets.text(...)` line runs. Job parameters fill them.

**Extract to `src/` when there's a second caller**, not before.

## Git workflow

Local → GitHub → Databricks Git folder. One direction of travel.

Editing the workspace copy directly creates a divergent version — commit and push from the Databricks Git dialog before going back to PyCharm, or resolve conflicts later.

Setup:
- Fine-grained PAT, scoped to the one repo, Contents: read+write
- Databricks → Settings → Linked accounts → GitHub + token
- Workspace → user folder → Create → Git folder
- Pull via the branch indicator next to the folder name

`.gitignore` **before the first `git add`** — Git only ignores files it isn't already tracking. Read `git status` before committing: a secret pushed to GitHub stays in the history even after deletion, and GitHub indexes it.

**nbstripout** (`pip install nbstripout && nbstripout --install`) strips notebook outputs on commit. Local file keeps them; Git gets clean diffs. The best practice is: **strip outputs, and write findings as text in `docs/`.** A markdown line is greppable and readable on GitHub; a cell output showing mojibake is neither. Committed outputs are also the most common accidental leak.


## Caching — the most confusing failure mode

Edited `bronze.py`, behaviour didn't change. Python had already imported the module and cached it in `sys.modules`; a later `import` returns the cached object regardless of what's on disk.

Fix now: detach/reattach compute, or Run → Clear state.
Fix for a dev session:
```python
%load_ext autoreload
%autoreload 2
```

**Whenever a change "doesn't take," ask what's cached:** the Python module, the notebook kernel, the Spark session, or a Git folder that hasn't been pulled.

## Lossless vs lossy — where the layer boundary actually is

**Bronze is lossless.** The original must be recoverable.
- Renaming columns ✓
- Lineage columns ✓
- Casts that provably can't fail ✓

**Silver is where you're allowed to lose things**, because bronze still has the original if you were wrong.
- Casts that can null out values
- Trimming, deduplication, filtering
- Any business rule

Each layer's only input is the layer below it. Silver reads `bronze.sales` (the table), not the CSV. That's what makes the chain replayable: fix silver logic, rerun from bronze, no re-ingestion.

### Lineage columns

```python
F.col("_metadata.file_path")
F.col("_metadata.file_size")
F.col("_metadata.file_modification_time")
F.current_timestamp()          → _ingested_at
F.lit(run_date).cast("date")   → _run_date
```

`_ingested_at` = when the row was physically written. `_run_date` = which logical batch it belongs to. **They differ when reprocessing** — re-run the 15th on the 16th and that's exactly what you want to be able to see.

Prefix pipeline columns with `_` so source data and plumbing are distinguishable at a glance.

Raise on an empty `run_date` rather than defaulting to today — silently substituting hides a real bug that only appears when reprocessing history.

## Data quality

### Count before and after every lossy operation

Row counts before/after a join. Non-null counts before/after a cast. Distinct keys before/after a dedup.

```python
def to_date(df, cols, fmt="M/d/yyyy"):
    for col in cols:
        before = df.filter(F.col(col).isNotNull()).count()
        df = df.withColumn(col, F.try_to_date(F.col(col), fmt))
        after = df.filter(F.col(col).isNotNull()).count()
        if after < before:
            raise ValueError(f"{col}: {before - after} values failed to parse as {fmt}")
    return df
```

Mistakes I made writing this:
- **Comparison backwards** — a check that can never fire is worse than no check, because it creates false confidence. **Make every new check fail on purpose once.**
- **Name contradicted the operation** — `before_null_values = df.filter(isNotNull())`. You then reason about the name instead of the code and get the logic inverted.
- **Called `.count()` too late** — Spark is lazy, so holding DataFrames instead of ints means re-reading the source. Call `.count()` immediately.
- Iterate `cols` directly, not `df.columns` filtered by membership — then a typo raises instead of silently matching nothing.
- Put the *count* in the error message. "12 values failed" tells you systemic vs a few bad rows.

### ANSI mode

On this runtime `to_date` **throws** on unparseable input rather than returning null — fail-fast, which is safer. But ANSI isn't universal and can be flipped per session, so don't rely on it. **Ask what the team's clusters are configured with.**

The count check still earns its place with `try_to_date`, which you want in real pipelines: three bad rows shouldn't kill a load of ten million. Then nulls come back silently and counting is the only way to know.

## The encoding bug — silent corruption

`Customers.csv` is Latin-1. Spark read it as UTF-8. Invalid bytes became `�` (U+FFFD). `München` → `M�nchen`.

**Row counts were correct. Schema was correct. The job was green. The data was wrong.**

**Not recoverable downstream** — the original byte is gone. The only fix is re-reading the source with `.option("encoding", "ISO-8859-1")`. This is what "lossy" means in practice, and why bronze must stay a faithful copy: it's the only route back.

The check that catches it:

```python
def assert_no_mojibake(df, entity=""):
    string_cols = [f.name for f in df.schema.fields
                   if isinstance(f.dataType, T.StringType)]
    condition = None
    for col in string_cols:
        c = F.col(col).contains("\uFFFD")
        condition = c if condition is None else (condition | c)
    bad = df.filter(condition).count()
    if bad:
        raise ValueError(f"{entity}: {bad} rows contain U+FFFD — wrong encoding on read")
    return df
```

One combined filter, single pass, not N passes.

**The general habit: for each way the data can be silently wrong, write the assertion that catches it.**

Don't just read everything as ISO-8859-1 "because it never fails" — that silently mangles genuinely UTF-8 files the other way.

## Config over code, again

Only `customers` needs a different encoding. Rather than `if entity == "customers"`:

```python
ENTITIES = {
    "sales":          {},
    "customers":      {"encoding": "ISO-8859-1"},
    "products":       {},
    "stores":         {},
    "exchange_rates": {},
}

for entity, opts in ENTITIES.items():
    ingest_bronze(spark, catalog, run_date, entity, options=opts)
```

```python
def ingest_bronze(spark, catalog, run_date, entity, options: dict | None = None):
    options = {"header": "true", **(options or {})}
    df = spark.read.options(**options).csv(path)
```

Per-entity knowledge lives in one declarative structure; the function never needs to know what the options are. **Configuration is data, not code** — same conclusion as the ADF entity array, reached independently.

`options=None` with `or {}`, never `options={}` as a default — mutable default arguments are shared across every call in Python.

`options: dict = None` is a lie the type checker correctly flags. Use `dict | None = None`.

## Databricks Jobs from Git

Task source: **Git provider**, branch `main`, path `notebooks/jobs/bronze_ingest` — **omit the `.py`**, because Databricks imports it as a notebook and workspace notebooks have no extension.

The run records the **exact commit** (`3ee0ad10`). That's the reproducibility: you can point at the code that produced any output. A workspace copy someone edited gives you none of that.

Compute: serverless. Job parameters fill the notebook widgets.

Job ID is on the run page — ADF needs it.

## Notebook format

`.ipynb` is JSON — good for exploration, bad diffs.

Databricks source format is a plain `.py`:
```python
# Databricks notebook source     ← first line, makes it a notebook
...
# COMMAND ----------             ← cell break
```
Diffs properly in Git, renders as a notebook in the workspace. The marker does nothing inside an `.ipynb`.

Use `# COMMAND ----------` generously — one giant cell means you can't run the import separately from the loop while debugging.

## Still open

- Bronze uses `mode("overwrite")` — throws away history. Bronze should be append-only, accumulating. Day 4's Auto Loader resolves this.
- `sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../..")))` is fragile — depends on the working directory. Verify with `print(os.getcwd())` rather than guessing. Databricks Asset Bundles solve this properly.
- ADF → Databricks Job activity: not done, Studio wouldn't load.
