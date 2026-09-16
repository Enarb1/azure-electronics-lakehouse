# Day 1 — Azure Storage + Identity + Unity Catalog

## Resource hierarchy

Everything lives inside a **resource group**. Deleting the group deletes everything in it — that's the cleanup button.

Keep every resource in the **same region**. Cross-region traffic costs egress and adds latency.

**Azure Policy** can restrict which regions you're allowed to use. My student subscription only permits `norwayeast`, `germanywestcentral`, `italynorth`, `uksouth`, `polandcentral`. Companies use the same mechanism to enforce data residency, allowed SKUs, and tagging.

My setup: `rg-learn-de` / `learnenarb` / `dbw-learn-de` / `ac-learn-de`, all in Germany West Central.

## Storage account — hierarchical namespace

Ticking **hierarchical namespace** at creation turns plain blob storage into **ADLS Gen2**. It cannot be enabled afterwards.

Without it, "folders" are only name prefixes in a flat key space. Renaming a directory means copying and deleting every file underneath — slow, and **not atomic**: a failure halfway leaves a partial state. Spark's write commit depends on atomic directory renames, so this is a correctness issue, not just performance.

It also enables directory-level ACLs.

### Path format

```
abfss://<container>@<account>.dfs.core.windows.net/<path>
```

- `abfss` — Azure Blob File System, Secure (TLS). Not `abfs`.
- `.dfs.` — the Data Lake API. `.blob.` is the legacy flat-blob endpoint; it often still *works*, which makes it dangerous — you get degraded semantics rather than a clean error. Always `.dfs.`

## Control plane vs data plane — the big one

Azure separates **managing a resource** from **accessing what's inside it**.

| Role | Grants |
|---|---|
| `Contributor` | Rename, configure, delete the storage account. **Zero** ability to read a file. |
| `Storage Blob Data Reader` / `Storage Blob Data Contributor` | Read/write the actual bytes. |

The word **Data** in a role name signals data plane. Being subscription owner and still getting a 403 on a file read is normal and expected.

Roles are **narrow and additive**. `Storage Blob Data Contributor` covers blobs only — not queues, not EventGrid. Each resource type needs its own role.

## The identity chain

```
Access Connector for Azure Databricks   (a managed identity)
        ↓  RBAC: Storage Blob Data Contributor on the storage account
Storage account
        ↑
Storage credential   (Unity Catalog object wrapping the connector)
        ↓
External location    (path + credential = a grantable securable)
        ↓
Query
```

No keys, no passwords, no secrets in a notebook. **Managed identity > service principal with a secret > account key.**

An external location is what people actually get granted access to — you can grant use of it without handing anyone the identity behind it.

Role assignments take **1–2 minutes to propagate**, in both directions. A propagation delay produces a failure identical to a genuine misconfiguration. Wait before re-doing correct work.

## Reading errors

**403 `AuthorizationPermissionMismatch`** — authenticated but not authorised. Identity recognised, role missing. → check RBAC.

**401** — the credential itself was rejected. → check the credential.

### Credential vending

The 403 error URL contained `sig=`, `se=`, `sp=rl`, `skoid=`. Unity Catalog had exchanged the managed identity for a **short-lived SAS token**, scoped to that one path, read+list only (`sp=rl`), expiring in about an hour.

The compute never holds the identity. A leaked token grants one path, one permission set, for one hour — instead of everything the connector can do, forever.

## Managed vs external tables

Both are metadata in the metastore. The difference is where the **files** live and who owns their lifecycle.

| | Managed | External |
|---|---|---|
| Location | Databricks picks it (shows blank in `DESCRIBE`) | I specify `LOCATION 'abfss://...'` |
| Owns the bytes | Databricks | Me |
| `DROP TABLE` | **Deletes the data** | Removes metadata only |

**Deciding question:** does anything outside Databricks need to read these files?
**Yes → external. No → managed.**

`DESCRIBE EXTENDED` shows `Type: MANAGED` and `Is_managed_location: true`. The blank Location is deliberate — Databricks treats the physical path as an implementation detail so nothing depends on it.

## Gotchas hit today

**Delta rejects column names with spaces.** Parquet underneath doesn't allow ` ,;{}()\n\t=`. Either enable column mapping (logical name ≠ physical name — works, but means backticks in SQL forever) or rename to `snake_case` at ingestion.

Renaming is the right call. Cleaning column *names* isn't cleaning column *values* — the bronze rule stays intact, and downstream code shouldn't have to know someone typed a space in a header five years ago.

```python
import re
def clean_columns(df):
    for old in df.columns:
        new = re.sub(r"[^0-9a-zA-Z]+", "_", old).strip("_").lower()
        df = df.withColumnRenamed(old, new)
    return df
```

**File events failed on the external location test** — 6 green, 2 red. Provisioning a storage queue and EventGrid subscription needs `Storage Queue Data Contributor`, `EventGrid EventSubscription Contributor`, `Storage Account Contributor`. Blob data permissions don't extend to queues.

Force-create is fine at my scale: Auto Loader falls back to directory listing. With millions of files in a landing zone, listing gets slow and the storage API calls cost real money — that's when file notifications stop being optional.

**Revoking the role broke `LIST` but not `SELECT`.** The table's files are in the metastore's managed storage, which Databricks reaches with its own credentials — nothing to do with my storage account.

Consequence for production: a broken ingestion permission is **silent**. Dashboards look fine, yesterday's numbers are all there, nothing fails loudly — new data just stops arriving. Monitor **freshness**, not only job success.

## Cost

Azure Databricks has no free tier. The Trial SKU is blocked on student subscriptions, so Premium bills against the $100 credit. Premium is required for Unity Catalog — Standard won't do.

Real risks: an **all-purpose cluster** left running (VMs + DBUs, including overnight) and a **SQL warehouse** left on (separate page, easy to forget). Serverless bills per second and releases on its own.

Habit: before closing the browser, open **Compute** and confirm nothing is running. Budget alert set at $10 with alerts at 50/80/100% — notifications only, not a cap. Cost data lags several hours.

Emergency stop: `az group delete -n rg-learn-de --yes --no-wait`

## Environment note

This workspace's catalog is `dbw_learn_de`, not `workspace` as in Free Edition. Any notebook with a hard-coded catalog breaks when moved. Concrete reason to parameterise the catalog name: same code, different environment, one parameter.
