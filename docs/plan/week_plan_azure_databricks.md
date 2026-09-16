# 7-Day Prep Plan — Azure + Databricks + PySpark

**Goal:** not to build something impressive, but to be able to *navigate* the tools on day one and understand why each piece exists. You already have the PySpark and Delta side covered from your lakehouse repo. The gap is **Azure itself** (storage, identity, secrets) and **orchestration** (Data Factory driving Databricks).

---

## Read this before Day 1 — two important constraints

**1. Your Databricks Free Edition account cannot talk to Azure.**
Free Edition is serverless-only, hosted outside Azure, and it has hard limits: one workspace and one metastore, no account console or account-level APIs, no custom workspace storage locations, and outbound network access restricted to a small set of trusted domains. That means you **cannot** register an ADLS Gen2 container as an external location in it, and Azure Data Factory **cannot** use it as a linked service (ADF expects an `*.azuredatabricks.net` workspace).

So:
- **Free Edition** → use it for pure PySpark / Delta / Gold-modelling practice (Days 4–5). It's great for that and costs nothing.
- **Azure Databricks workspace** (deployed in your Azure for Students subscription) → use it for anything that touches ADLS, Key Vault, managed identity, or ADF (Days 1–3). When you create it, pick the pricing tier **Trial (Premium — 14 days free DBUs)**; you still want Premium because Unity Catalog needs it.

**2. Azure for Students is $100 of credit for 12 months.** The workload in this plan is tiny (a few GB of storage, a handful of serverless job runs, some ADF activity runs) and should cost single-digit dollars — *if* you don't leave compute running. Day 0 sets up the guardrails.

**A naming note:** I've assumed you mean **Azure Data Factory (ADF)**, not Microsoft Fabric — ADF is what's normally paired with Databricks in an Azure data-engineering shop. If your new team actually uses Microsoft Fabric, tell me and I'll swap Days 2–3.

---

## Day 0 — Pre-flight (45 minutes, do it tonight)

Not a project. Just removing the friction so Day 1 isn't spent on account problems.

| Step | Why |
|---|---|
| Sign in to [portal.azure.com](https://portal.azure.com), confirm your Students subscription is active and shows credit | Nothing works without this |
| Create a resource group `rg-learn-de` in **West Europe** (closest to you, and everything must live in the same region) | Cross-region traffic costs money and adds latency |
| **Cost Management → Budgets → Add**: budget of $10, alerts at 50% / 80% / 100% | The single most important habit. Do this before you create anything |
| Install the Azure CLI (`az login`) and confirm `databricks --version` still works | You already use the Databricks CLI; the Azure CLI is its counterpart |
| Register resource providers if prompted: `Microsoft.Databricks`, `Microsoft.DataFactory`, `Microsoft.KeyVault` | Student subscriptions sometimes have these unregistered, which produces a confusing deployment error |
| Download the Olist CSVs again into a local `data/raw/` folder | You'll re-land them into Azure, not reuse the Databricks volume |

**Done when:** you can run `az group show -n rg-learn-de` and get JSON back, and you have a budget alert email confirmation.

---

## Project A — Day 1: "Where does the data actually live?" (3–4 hours)

**Theme: Azure storage + identity. This is the single biggest gap in your current repo.**
Right now your data lives in a Databricks-managed volume. In a real job it lives in *your company's* storage account and Databricks is granted access to it. That grant is the thing you need to understand.

### Build
1. Create a **Storage account** in `rg-learn-de`:
   - Performance: Standard, Redundancy: **LRS** (cheapest — you're learning, not running prod)
   - Advanced tab: **enable Hierarchical namespace**. This is what makes it ADLS Gen2 rather than plain Blob. Miss this checkbox and nothing later works.
2. Create containers: `raw`, `bronze`, `silver`, `gold`.
3. Upload two or three Olist CSVs into `raw/olist/orders/` etc. (portal upload is fine, or `az storage blob upload-batch`).
4. Deploy an **Azure Databricks** workspace (`dbw-learn-de`), same resource group and region, pricing tier **Trial (Premium)**.
5. Wire the two together — this is the actual exercise:
   - Create an **Access Connector for Azure Databricks** (a managed identity resource).
   - In the storage account → **Access Control (IAM)** → assign the role **Storage Blob Data Contributor** to that access connector.
   - In Databricks **Catalog → External Data → Credentials** → create a **storage credential** pointing at the access connector's resource ID.
   - Then **External Locations** → create one over `abfss://raw@<youraccount>.dfs.core.windows.net/`.
   - Run `LIST 'abfss://raw@<youraccount>.dfs.core.windows.net/olist/'` in a notebook.
6. Read one CSV with an explicit schema straight from `abfss://` and write it as a **managed** Delta table in a `bronze` schema.

### What to notice (this is the learning, not the code)
- Nowhere did you type a key or a password. That's the point. The identity chain is: *Azure resource (access connector) → RBAC role on storage → storage credential in Unity Catalog → external location → your query.*
- Deliberately break it: remove the role assignment, re-run the `LIST`, read the error, put it back. You will see that exact error in your job within a month.
- Compare `abfss://` to the `dbfs:/` and volume paths you used before. Understand which is *your* storage and which is *Databricks'* storage.

### Done when
You can explain out loud, in four sentences, how Databricks got permission to read that container — and you've seen the failure mode.

---

## Project B — Day 2: "Move bytes, don't transform them" (3–4 hours)

**Theme: Azure Data Factory as a copy and orchestration tool. No Databricks today.**
ADF's job in most shops is *land data and coordinate things*. The transformation lives in Databricks. Keeping that boundary clear is itself a best practice.

### Build
1. Create a **Data Factory** (`adf-learn-de`) in the same resource group. Open **Data Factory Studio**.
2. Build a **Copy activity** pipeline: source = one CSV in `raw`, sink = the same file in `bronze/landing/`. Debug-run it. Look at the monitoring output: rows read, rows written, duration, data volume.
3. Now make it **generic** — this is the real exercise:
   - Add pipeline **parameters**: `source_folder`, `file_name`.
   - Make the datasets use dataset parameters fed by those pipeline parameters, so one dataset object serves every file.
   - Add a **ForEach** activity over an array of the five Olist entities, calling the copy inside it. Set `batchCount` to 2 and watch them run in parallel.
4. Add a **schedule trigger** (daily). Publish it. Then **disable it** so it doesn't quietly burn credit.
5. Optional but valuable: add a **Get Metadata** activity before the copy that checks the file exists, and an **If Condition** that skips the copy if it doesn't.

### What to notice
- ADF's object model is small and worth memorising: **Linked Service** (connection) → **Dataset** (shape/location of data) → **Activity** (unit of work) → **Pipeline** (container of activities) → **Trigger** (what starts it). Integration Runtime = the compute that runs the copy (AutoResolve for cloud-to-cloud).
- The difference between **Debug** (runs your unpublished draft) and **Trigger now** (runs the published version). People lose hours to this.
- Hard-coding a file name in a dataset works and is wrong. Parameterised datasets are the difference between 1 pipeline and 40.

### Done when
One pipeline, driven by parameters, copies all five files, and you didn't create five datasets to do it.

---

## Project C — Day 3: "ADF drives, Databricks works" (3–4 hours)

**Theme: the integration point. This is the pattern your new team almost certainly runs.**

### Build
1. In your **Azure Databricks** workspace, take a parameterised notebook (reuse your Silver one — it already has `dbutils.widgets`) and wrap it in a **Databricks Job** with a `catalog` (or `run_date`) job parameter. Confirm it runs on **serverless** job compute.
2. In ADF, create an **Azure Databricks linked service**. For authentication, use the **ADF system-assigned managed identity** rather than a personal access token — then grant that identity access to the workspace and `CAN MANAGE RUN` on the job. (PAT works too; if you use one, put it in **Key Vault** and reference it from the linked service, never paste it in plain text.)
3. Add the **Databricks Job activity** to your Day 2 pipeline, chained after the ForEach. This activity triggers your existing Databricks Workflow and runs it on serverless — it's the current recommended pattern, better than the older Notebook activity, because the job definition stays in Databricks where it's version-controlled.
4. Pass a parameter from ADF into the job (e.g. `@pipeline().parameters.run_date` or `@utcNow()`), and have the notebook print it.
5. Deliberately fail it: make the notebook raise an exception. Watch the ADF activity go red, click through to the Databricks run page from the activity output, read the error. Then set **retry = 1** and **timeout = 10 minutes** on the activity.
6. Add one **failure path**: an activity connected by the red arrow (e.g. a Web activity or just a Set Variable) so you see how failure branching works.

### What to notice
- Two systems, two sets of logs. Learning to jump from "ADF says failed" to the actual Spark stack trace is a genuine day-job skill.
- Parameters flow: *trigger → pipeline parameter → activity base parameter → job parameter → `dbutils.widgets.get()` → your code.* Draw that chain on paper.
- Why the Job activity beats the Notebook activity: the compute config, libraries, and task graph live in Databricks, so ADF stays a thin orchestrator.

### Done when
One ADF pipeline run lands files **and** produces updated Delta tables, with a date parameter that reached your PySpark code.

---

## Project D — Day 4: "Only process what's new" (3 hours) — *Free Edition is fine here*

**Theme: incremental ingestion. Your current Bronze layer re-reads everything; production never does.**

### Build
1. Rewrite Bronze ingestion using **Auto Loader**:
   ```python
   (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
        .schema(orders_schema)
        .load(source_path)
     .writeStream
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable("bronze.orders"))
   ```
2. Run it. Note the row count. Run it **again without adding files** — zero new rows. That's the checkpoint doing its job.
3. Drop one new CSV into the source folder, run again, and see only those rows land.
4. Break it on purpose: add a column to an incoming file and watch the schema-evolution behaviour; try `cloudFiles.schemaEvolutionMode` settings.
5. Write down, in your repo docs, the difference between: re-run everything (full refresh), append-only incremental, and MERGE-based upsert — and when each is correct.

### What to notice
- `trigger(availableNow=True)` gives you streaming's bookkeeping with a batch job's lifecycle. Very common in real pipelines.
- The checkpoint directory *is* your state. Delete it and you reprocess everything. Know where it lives.
- **Idempotency**: running the same job twice should not double your data. Auto Loader gives you that for ingestion; MERGE gives it to you for transformation.

---

## Project E — Day 5: "Gold layer and slowly changing dimensions" (4 hours) — *Free Edition is fine here*

**Theme: dimensional modelling. This was already the next step in your repo, and it's the part juniors most often can't do.**

### Build
1. From Silver, build `gold.dim_customer`, `gold.dim_product`, `gold.dim_date`, and `gold.fct_order_items`.
2. Give dimensions **surrogate keys** (not the natural business key) and the fact table foreign keys to them.
3. Implement **SCD Type 2** on `dim_customer` using Delta `MERGE`: `valid_from`, `valid_to`, `is_current`. Change a customer's city in the source, re-run, and confirm you now have two rows — one closed, one current.
4. Implement `dim_product` as **SCD Type 1** (overwrite) so you can feel the difference.
5. Make the fact load **idempotent**: re-running the same day's load must not duplicate rows. Use `MERGE` on the business key, or delete-then-insert scoped to the partition.
6. Add three data-quality assertions that *fail the job loudly*: fact row count vs source, no orphan foreign keys, no duplicate current rows per dimension key.

### What to notice
- Why surrogate keys exist at all: because the business key isn't unique once history is tracked.
- `MERGE` is the workhorse of the job. Get comfortable with `WHEN MATCHED AND ... THEN UPDATE` / `WHEN NOT MATCHED THEN INSERT`.
- A pipeline that silently produces wrong numbers is worse than one that fails. Assertions over hope.

---

## Day 6 — Consolidate (3 hours)

Not a new project — turning the week into something you can actually talk about.

- Put Days 1–5 into your repo as a second project (or a `week-3/` branch of the lakehouse one). Add a short `README` with an architecture diagram: `Source → ADF Copy → ADLS raw → Auto Loader → Bronze → Silver → Gold → (Power BI)`.
- Move any secret into **Key Vault** and reference it, so nothing sensitive is in Git. Check your notebooks for a pasted token — there usually is one.
- Try **Databricks Git folders**: connect the workspace to your GitHub repo and run a notebook from the repo instead of an imported copy. This is how teams actually work.
- Skim **Databricks Asset Bundles** (`databricks bundle init`) for 30 minutes. You won't master it; you just want to recognise `databricks.yml` when someone mentions it.
- **Tear down**: delete the Data Factory and the Databricks workspace, or at minimum confirm nothing is running. Check the cost analysis blade and see what the week actually cost. Keep the storage account — it's pennies.

---

## Day 7 — Rest, and prepare questions (1 hour)

Genuinely take most of the day off; you start a new job in a few days and arriving fresh matters more than one more notebook.

Spend one hour writing down what you'll ask in week 1. Good ones:

- Which layers exist, and what are they called here? (Not everyone says bronze/silver/gold.)
- What orchestrates the pipelines — ADF, Databricks Workflows, Airflow, or something in-house?
- Are tables managed or external, and which storage accounts back them?
- How does code get from my laptop to production? Git branching, asset bundles, CI/CD?
- Which environments exist (dev/test/prod) and how do I tell which one I'm in?
- How do we handle late-arriving and re-delivered data?
- What's on call / what breaks most often?
- Who owns the data quality definitions?

---

## Time budget summary

| Day | Project | Where | Hours | Core? |
|---|---|---|---|---|
| 0 | Pre-flight + budget alert | Azure portal | 0.75 | **Yes** |
| 1 | **A** — ADLS Gen2 + Unity Catalog external location | Azure Databricks | 3–4 | **Yes** |
| 2 | **B** — Parameterised ADF copy pipeline | ADF | 3–4 | **Yes** |
| 3 | **C** — ADF triggers a Databricks Job | Both | 3–4 | **Yes** |
| 4 | **D** — Auto Loader incremental Bronze | Free Edition | 3 | Recommended |
| 5 | **E** — Gold star schema + SCD2 MERGE | Free Edition | 4 | Recommended |
| 6 | Consolidate, secrets, Git folders, teardown | Both | 3 | Recommended |
| 7 | Rest + questions list | — | 1 | Yes |

If you only get three days: **A, B, C**. They're the ones your repo doesn't already cover.

---

## Rules for the week

1. **Type the code.** You were guided the whole way last time; that's how you end up with a repo you can't defend in an interview. Read docs, then type, then fix your own errors.
2. **Break one thing per day on purpose.** Remove a permission, pass a bad parameter, delete a checkpoint. Recognising errors is 60% of the job.
3. **Timebox to 4 hours.** If something isn't working at hour 4, write down where you got stuck and move on. The stuck point is a great question for your new team.
4. **Write one paragraph per day** in `docs/` explaining what you built and why. Your existing repo already does this well — keep it up.
5. **Check the cost blade every evening.** Ten seconds, builds the right instinct.
