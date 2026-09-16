# Day 2 — Azure Data Factory

## What ADF is for

ADF pulls data from source systems and lands it in the lake, then hands off to Databricks for transformation.

**ADF moves bytes and coordinates. Spark does the thinking.** Business logic in ADF expressions can't be unit tested, isn't reviewable in a meaningful diff, can't be debugged against real data, can't be reused, and processes row by row instead of as a distributed operation. Writing a currency conversion in an ADF expression means writing business logic in a config file.

Pick **Data Factory (V2)** in the marketplace. Plain "Data Factory" is the legacy V1 product — no one starts new work on it.

## Object model

| Object | Responsibility |
|---|---|
| **Linked service** | A connection to a system. **Credentials live here** — the only object type that holds secrets, so the only one that points at Key Vault. |
| **Dataset** | The shape and location of data *within* a linked service — a folder, a file, a table. |
| **Activity** | One unit of work: copy, look up, loop, call Databricks. |
| **Pipeline** | An ordered set of activities. |
| **Trigger** | What starts a pipeline: schedule, tumbling window, event, manual. |

**Integration Runtime** — the compute that executes an activity. `AutoResolveIntegrationRuntime` for cloud-to-cloud. Self-hosted IR is what you install on-prem to reach a database behind a corporate firewall.

Nesting: a linked service says *how to connect*, a dataset says *what to connect to*, an activity says *what to do with it*.

## Debug vs Trigger

**Debug** runs the unpublished draft. **Trigger now** runs the last *published* version.

Fix a bug, hit Trigger now, watch the old broken behaviour run again → you forgot to publish. Hours get lost to this.

## Parameterised datasets

One dataset with `container` / `folder` / `file_name` parameters instead of one dataset per file.

**Maintenance:** change the storage account or a format option and you edit one object, not five — and don't miss one.

**Extension:** a sixth entity is a line in an array. Nothing new gets created, the flow doesn't change. The pipeline's structure is independent of how many entities it handles.

Set **Import schema: None**. Importing a schema binds the dataset to one file's columns and defeats the point.

Expressions on the dataset's own parameters: `@dataset().container`, etc.

## Parameters vs variables

| | Parameters | Variables |
|---|---|---|
| Set | When the pipeline starts | During the run, via Set Variable |
| Mutable | **No** | **Yes** |
| Use for | Everything configurable | Accumulating or changing state |

Variables are for counters, building lists with Append Variable, storing a Lookup result for reuse.

**Trap:** variables are **pipeline-scoped, not iteration-scoped**. Setting one inside a parallel ForEach means every iteration writes to the same variable and they overwrite each other. A race condition that produces correct-looking results most of the time.

## Metadata-driven ingestion

Array of objects rather than a `*.csv` wildcard:

```json
[
  {"name":"sales","file":"Sales.csv"},
  {"name":"customers","file":"Customers.csv"},
  {"name":"products","file":"Products.csv"},
  {"name":"stores","file":"Stores.csv"},
  {"name":"exchange_rates","file":"Exchange_Rates.csv"}
]
```

**Why objects:** the contract is explicit — the array declares exactly what's expected. A missing file fails loudly on that iteration. A wildcard means "whatever happens to be there": two files and you silently process both; zero files and the activity often succeeds having copied nothing. **Silent success is worse than failure.**

**Why wildcards win sometimes:** unpredictable filenames you can't enumerate — a vendor dropping `export_20260915_113422.csv`. Then use a wildcard plus Get Metadata to see what turned up. (Wildcards don't lowercase anything — `*.csv` matches `Sales.csv` fine. My problem was needing to *derive* the filename from a lowercase folder name.)

**Where this goes:** the array is a **control table**. Today it's a parameter default; next it's a JSON file or a database table read by a **Lookup** activity at pipeline start. Then adding an entity is a config change, not a code change. That's the standard metadata-driven ingestion pattern.

## Expressions

```
@pipeline().parameters.entities
@pipeline().RunId
@activity('Check_Exists').output.exists
@item()                                -- current element inside ForEach
@item().name  /  @item().file          -- fields of an object element
@concat('electronics/', item().name)
@utcNow('yyyy-MM-dd')
@formatDateTime(utcNow(), 'yyyy/MM/dd')
```

## What I built

```
Trigger  →  Pipeline (entities array)
              └─ ForEach (batch count 2)
                   └─ Get Metadata  →  If Condition
                                          └─ True: Copy
```

Container activities (ForEach, If Condition) have their **own inner canvas**. Navigate in via the pencil icon, out via the breadcrumb. Activities must be **connected with the green success arrow** — unconnected activities run in parallel, so the If would evaluate before Get Metadata produced output.

## Concurrency ≠ error handling

**Batch count** = how many ForEach iterations run concurrently.

Reasons to force **Sequential**: order matters (dimensions before facts), writing to a shared resource where parallel writes conflict, or a rate-limited source API.

Sequential has **nothing to do with failure behaviour**. Error handling is retries, timeouts, and failure paths — separate concern.

## Identities

ADF has its own system-assigned managed identity, separate from yesterday's access connector. It needed its own `Storage Blob Data Contributor` assignment.

**Each service brings its own identity, and each needs its own grant.** There is no shared "the pipeline" identity. A production storage account's IAM page is a long list — every service, every environment, every team. Which is why real setups assign roles to **Entra groups** rather than individual identities.

## Cost

ADF has no idle charge — roughly per activity run and per data-movement hour, fractions of a cent at this volume.

The thing that costs money is **a trigger you forgot to disable**. Build it, see how it's configured, then Manage → Triggers → Stop. And publish after stopping, or the published version stays active.

## Layers — getting this right

- **raw** — as delivered
- **bronze** — *also* as-delivered: appended, typed, lineage columns (`_source_file`, `_ingested_at`). **Not cleaned.**
- **silver** — cleaned, typed, deduplicated, conformed, quality-flagged
- **gold** — modelled for consumption: facts, dimensions, aggregates

Cleaning happens in **silver**, not bronze. The value of keeping bronze raw is replayability: fix the silver logic and rebuild from bronze. Clean on ingest and the original is gone.
