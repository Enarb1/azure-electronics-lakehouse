# Day 4 — ADF → Databricks, and Auto Loader

## Two permission systems, stacked

Azure RBAC governs Azure resources. **Databricks has its own user directory and access control.** Being an Azure admin grants nothing inside a Databricks workspace.

The linked-service test failed with:

> Processed HTTP request failed. **User** not authorized.

Note the wording — Databricks saying "I have no user for you," not Azure saying no.

### Object ID vs Application ID

Every service principal has **two** GUIDs:

| | What it identifies |
|---|---|
| **Object ID** | The record inside this tenant's directory |
| **Application ID** (client ID) | The application globally — **this is what appears in tokens** |

ADF's linked-service screen displays the **object ID**. Databricks asks for the **application ID**. Paste the wrong one and Databricks creates a principal keyed to a GUID that never matches an incoming token → "User not authorized," with everything else looking correct.

```bash
az ad sp show --id <objectId> --query "{appId:appId, objectId:id}" -o table
```

Tools ask for one or the other, usually without saying which.

### Setup

1. Databricks → Settings → Identity and access → **Service principals** → Microsoft Entra ID managed, with the **application ID**
2. Entitlements: **Workspace access On**. Databricks SQL, Consumer, Admin all **Off** — ADF only needs to trigger jobs
3. Job → Permissions → add the principal with **Can Manage Run**

`Can Manage Run` = start, cancel, view output. **Cannot** edit the job definition, change compute, or repoint it at different code. Least privilege: the orchestrator says "run this," not "change what this does."

Permissions are tied to the principal — delete and recreate it and the grants don't carry over.

**Test connection tests the workspace, not the job.** It needs the principal to exist with workspace access; a per-job grant won't fix a failing connection test.

## The Job activity

ADF → Activities → **Databricks → Job** (not Notebook — that's the older pattern where ADF owns the compute config).

- Linked service authenticates with ADF's system-assigned managed identity
- Cluster type: **Serverless** — no cluster version, node type, or Python version to configure, and no coupling of compute config into ADF's JSON
- Settings → select the job, then add job parameters
- Connect `ForEach → Job` with the **green success arrow**: transform only runs if the copy succeeded

### Parameter chain

```
pipeline parameter run_date
  → activity job parameter  @pipeline().parameters.run_date
    → job parameter
      → dbutils.widgets.get("run_date")
        → F.lit(run_date)
          → _run_date column
```

Verified by querying the table, not by trusting a green tick.

### Retry and timeout

General tab: timeout `0.00:10:00`, retry `1`, interval `30s`.

A transient failure shouldn't wake anyone up; a hung job shouldn't run forever. **The automatic retry is only safe because the job is idempotent** — without that, an unattended retry could duplicate data.

## Errors met today

**Stripped underscores.** `rename_columns` ran *after* the lineage columns were added, and `.strip("_")` removed the prefix — `_run_date` became `run_date`. Fix: rename first, then add pipeline columns, so the function only ever sees source headers.

A transformation correct in isolation became wrong because of what ran before it. **Order matters, and it produced no error** — just a silently different name.

**`DELTA_METADATA_MISMATCH`.** Delta enforces schema on write. `mode("overwrite")` overwrites *data*, not schema — renaming a column is a schema change, so it refuses.

```python
.option("overwriteSchema", "true")   # deliberate replacement
.option("mergeSchema", "true")       # additive — would have kept BOTH name sets
```

**Remove `overwriteSchema` again afterwards.** Leaving it on permanently disables the protection that caught this: a source could add, drop, or retype a column and the table would silently reshape itself. Schema changes should be **deployment events**, not things that happen during a normal run. The cleaner production form is an explicit `ALTER TABLE` migration, separate from the pipeline code.

**Merge conflict.** Editing `bronze_ingest.py` in the Databricks workspace *and* pushing a different version from PyCharm. `<<<<<<< Updated upstream` is what's on main; `>>>>>>> Stashed changes` is the local workspace edit. Keep one side, delete the markers, Mark as resolved, Continue Merge.

**Edit in one place.** PyCharm is the editing surface; the Git folder is a copy to pull into.

## Error-tracing path

**ADF red → activity error details → link to the Databricks run → Spark stack trace.**

Walk it once while things work, because it's the path you take every time something fails in production. Read the first few lines and the `Caused by:` entries; skip the JVM frames between.

## Auto Loader

### The problem it solves

`mode("overwrite")` re-reads everything and replaces the table each run. Bronze becomes a snapshot instead of an accumulating record, and yesterday's data disappears. Fine at 62k rows, fatal at scale.

### The shape

```python
df = (spark.readStream                      # was spark.read
      .format("cloudFiles")                 # this is what makes it Auto Loader
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", f"{checkpoint}schema/")
      .options(**options)
      .load(source))                        # was .csv(source)

(df.writeStream                             # was df.write
   .option("checkpointLocation", f"{checkpoint}write/")
   .trigger(availableNow=True)
   .toTable(f"{catalog}.bronze.{entity}")
   .awaitTermination())
```

Everything between read and write is unchanged — a streaming DataFrame takes the same transformations.

- `.load()` not `.csv()`, because the format is already `cloudFiles`
- `schemaLocation` holds the inferred schema (so changes can be detected); `checkpointLocation` holds processed-file state and write progress
- **`awaitTermination()`** — `writeStream` returns immediately; without this the function returns before the data lands
- **`.count()` doesn't work on a streaming DataFrame.** Query the table after the write instead. Same for `assert_no_mojibake` — it runs against the written table, so bad data has already landed. Acceptable for bronze; catching it pre-write needs `foreachBatch`.

### `trigger(availableNow=True)`

Process everything currently available, **then stop**. Without it the stream runs forever waiting for new files — a job that never finishes.

The point is to borrow streaming's **bookkeeping** — checkpoints, exactly-once, recovery from a mid-write crash — while keeping a job that starts, finishes, and can be scheduled. Very common production pattern.

### Checkpoints

```
abfss://bronze@learnenarb.dfs.core.windows.net/_checkpoints/{entity}/
```

**They live in cloud storage because the job is ephemeral** — it starts, runs, exits, and anything in its memory or local disk is gone. State that must survive between runs needs somewhere durable.

Under `bronze` alongside the data it serves, not `raw` (which stays untouched). `_` prefix marks it as infrastructure, same instinct as `_` columns.

Inside: `offsets` (what each batch consumed), `commits` (what finished), `sources` (a RocksDB store of processed file paths), `metadata`. The gap between offsets and commits is how it recovers from a crash mid-write.

### How it detects new files

**File paths, not content, not timestamps.** It lists the directory and processes paths it hasn't recorded.

**Consequence: overwriting a file with the same name is invisible.** A vendor re-delivering `Sales.csv` with corrections gets silently skipped — the path has been seen.

Auto Loader's contract is **immutable files with unique names**. That's also the normal landing-zone contract: `sales_20260917.csv`, or date-partitioned folders.

Awkward cases:
- **Same name overwritten each time** (an SFTP always writing `current.csv`) → wrong tool. Rename on arrival with a timestamp, or use a different change-detection strategy.
- **Corrections to processed data** → not an ingestion trick. The correction arrives under a new name, lands in bronze as *additional* rows, and **silver resolves which version wins** (latest `_ingested_at` per business key). This is why bronze being append-only matters — overwrite would destroy the record that a correction happened.

*My setup is the awkward case:* ADF re-copies `Sales.csv` with the same name each run. Works for the exercise; a realistic version would land `sales/2026/09/17/Sales.csv` so each run gets a distinct path.

### Checkpoint and table must agree

| State | Result |
|---|---|
| Checkpoint empty, table full | **Duplicates** — reprocesses everything into existing rows |
| Checkpoint full, table empty | **Silent data loss** — skips files, writes nothing |

**Reset is a pair operation: delete the checkpoint AND drop/truncate the table.**

Deleting the source file does nothing on its own — rows are already in bronze, and the path is still recorded. **You cannot undo an ingestion by tidying up storage.** Reprocessing is a deliberate operation on both state and table.

### Tests that prove it works

1. Run → 62,884
2. Run again, same files → **62,884**, nothing appended
3. Add `Sales_2.csv` → 125,768, only the new file processed
4. Reset (delete file + checkpoint + table), re-run → 62,884

Then the same at pipeline level: Debug twice, counts don't move.

## Idempotency

**Re-running produces the same result.**

At 3am with a failed run:

*Not idempotent:* Did it fail before or after the write? Did it write partially? Will re-running duplicate? You must investigate the state before acting.

*Idempotent:* Re-run it. Go back to bed.

It's also what makes automated retries and overlapping backfills safe.

## Current architecture

```
ADF (schedule, orchestration, file movement)
  → Databricks Job (Git-pinned commit, serverless)
      → Auto Loader (incremental, checkpointed)
          → Bronze Delta tables (append-only, lineage columns, validated)
```
