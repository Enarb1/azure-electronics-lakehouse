# Day 5 — Silver and Gold

## Silver

Read from bronze **tables**, not the source files. Each layer's only input is the layer below — that's what makes the chain replayable.

`mode("overwrite")` is correct here: silver is derived entirely from bronze, so a full rebuild is deterministic and safe. Merge is for when a layer holds state its upstream doesn't have.

### Config vs code — where the line is

```python
SILVER_TYPES = {
    "sales": {"int": [...], "date": [...], "string": [...]},
}

def type_products(df):
    df = apply_types(df, SILVER_TYPES["products"])
    return df.withColumn("unit_price_usd", clean_money("unit_price_usd"))
```

**Config is for variation within a fixed shape** — column A is an int, column B is a date. Same operation, different inputs.

**Code is for variation in the operation itself.** `clean_money` is strip-then-cast, not just cast. Forcing that into config means inventing a mini-language and maintaining an interpreter.

Precision (`decimal(10,2)` vs `decimal(10,4)`) is a *parameter* of the same operation — config could express it if the dict were shaped column→type instead of type→columns. "The config can't express this" was really "the config's shape can't express this." Different problem.

One thin function per entity: config-driven for the boring part, plain Python for the special cases.

### Money is never a float

Floating point can't represent most decimal fractions exactly. Sum a million transactions and the error becomes visible in a financial report.

```python
F.regexp_replace(F.col(c), r"[$,\s]", "").cast("decimal(10,2)")
```

Explicit about what's stripped (`$`, thousands separators, whitespace) rather than a blanket `.strip()` — so an unexpected character fails loudly instead of quietly mangling.

Check the value range before fixing precision: `decimal(10,2)` silently truncates a fourth decimal place.

### Type to the domain, not to the sample

`square_meters` is whole numbers in this data, but a measurement *can* be fractional — an int cast would truncate `1250.5` to `1250` silently. `quantity` is genuinely int by the domain: you can't sell 2.5 items.

Ask what the column *means*, not what the first five rows look like. Measurements → decimal. Counts and identifiers → int.

### No wall-clock time in a transformation

`age = months_between(current_date(), birthday)` means the same input produces different output tomorrow. Re-run January's batch in September and every age is wrong for January.

**A transformation that depends on wall-clock time isn't idempotent.** Store `birthday`; compute age where "as of when" is an explicit question — gold, or the BI tool.

`_ingested_at` has the same property but is fine: it *documents* when the write happened rather than deriving a business value.

### Lineage columns travel unchanged

Don't "update" them at each layer. They describe **origin**, not the current layer.

- `_file_path` — keep, unchanged. Answers "which file did this row come from," at every layer. Overwrite it with the silver table's location and you destroy the only thing that made it useful.
- `_ingested_at` — keep, unchanged. When it entered the platform. This is the field that resolves "latest wins" for vendor corrections.
- `_file_size`, `_last_modified_at` — drop. Properties of the file, not the row.
- `_silver_run_date` — a *new* column, not an overwrite of `_run_date`.

Rewriting lineage at each layer means you can only ever see one hop back.

### `_rescued_data`

Auto Loader adds this: anything that didn't fit the expected schema, stashed as JSON rather than dropped or failed on.

Don't silently carry it forward, and don't silently drop it. **Assert it's empty, then drop it** — so a schema surprise fails the silver build loudly.

That's the third assertion of the same shape (mojibake, date-parse counts, rescued data): *check the thing that would otherwise be silent, fail with a count.*

---

## Gold — dimensional modelling

### Star, not snowflake

I wanted to split `state`/`country` into their own dimensions with keys pointing at them. That's normalisation instinct, and it's wrong here.

**Why the rules invert:**
- Normalisation prevents update anomalies. A dimension is written only by the pipeline — there are no anomalies to prevent.
- "Revenue by continent" = one join in a star, three in a snowflake. Every analyst has to know the chain exists.
- 15,266 repeated country strings is a few hundred KB. Real query complexity traded for imaginary storage savings.

**Normalise for writes, denormalise for reads.** Gold is read-optimised.

Keeping `subcategorykey` alongside `subcategory` is *not* snowflaking — that's just carrying the source identifiers as plain columns. Snowflaking is creating a separate table.

### The model

| Table | SCD | Grain |
|---|---|---|
| `dim_customer` | 2 | one row per customer **version** |
| `dim_product` | 1 | one row per product |
| `dim_store` | 1 | one row per store |
| `dim_date` | — | one row per calendar date |
| `fct_sales` | — | one row per **order line** |

### Grain: the lowest the source provides

I first described `fct_sales` as one row per order with a total. Wrong — the source is one row per line item.

Aggregating destroys information you can't recover: revenue by product category becomes impossible. **You can always aggregate up in a query; you can never disaggregate back down.**

And no text in the fact. Names, cities, categories are dimension attributes. A comma-joined product list in one field can't be joined, filtered or grouped without string parsing — looks convenient, makes the table unusable.

**Fact = keys, degenerate dimensions, and measures. Nothing else.**

`unit_price_usd` stays as a measure even though it came from products — it's the price applied to *this transaction*, a measurement. `product_name` is context and lives in the dimension.

### Why `dim_date` exists at all

Every fact already has an order date, so:

1. **Attributes you can't derive in SQL** — public holidays, fiscal quarters (your fiscal year might start in April), promotional periods.
2. **Consistency** — otherwise every analyst writes their own `MONTH()` and quarter logic, one uses ISO weeks, and two reports disagree.
3. **Completeness** — grouping the fact by month only returns months that had sales. A month with zero sales *vanishes*, and a missing row is far harder to spot than a zero.

Range covers the dates **facts reference** — not birthdays, which are attributes inside a dimension that nothing joins to. Fixed constants (2015–2026), not derived from the data: reference data shouldn't shift because someone loaded a typo'd date in 1902.

**Role-playing dimension:** `fct_sales` joins `dim_date` twice, once per date column, each with its own alias. One physical table, several logical roles.

### Surrogate keys

**The reason they exist is SCD2.** Once a customer has two versions, `customerkey` is no longer unique in the dimension — join on it and one order line matches both rows, so revenue doubles.

`customer_sk` is unique per *version*. A 2024 order points at the Auberry version and still reports as Auberry.

Keep the natural key in the dimension — it's how you look someone up and how the merge matches rows. It just isn't the join key.

For SCD1 the natural key *would* work. Surrogate keys there buy consistency (every dimension joins the same way) and future-proofing (converting to SCD2 later becomes a dimension-only change). Plus the **unknown member**: a `-1` row for unmatched facts, so foreign keys are never null and inner joins never silently drop rows.

**Stability is the requirement.** `row_number()` breaks on rebuild — insert a product with a low key and every subsequent `product_sk` shifts while the fact still holds the old ones. Options: Delta identity columns (incremental only), a hash of the business key (deterministic, rebuild-safe), or a key mapping table.

```python
F.xxhash64(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]))
```

`concat_ws` separator matters — without it `("ab","c")` and `("a","bc")` hash identically. `coalesce` matters — `concat_ws` skips nulls, which shifts positions.

For SCD2 the key hash must include the **version-defining attributes**, or both versions get the same key and the whole thing collapses.

---

## SCD2

**Slowly Changing Dimension.** Type 1 = overwrite, history lost — for when the old value was *wrong*. Type 2 = new row, old one closed — for when the old value was *correct then*.

### Which attributes are which

Not all columns are Type 2. Name, gender, birthday → Type 1 (corrections). City, state, zip, country, continent → Type 2 (real history).

Mixing them means every typo correction spawns a spurious version.

(Open question: a Type 1 correction should arguably update *all* historical versions, not just the current one — "we had it wrong all along.")

### `valid_from` on the initial load

Don't derive it from the first order date — that asserts they lived there then, which you don't know. **Don't manufacture precision you don't have.**

Use a sentinel: `1900-01-01` to `9999-12-31`. Obviously artificial, which is the point.

Not the load date: then no fact predates `valid_from`, and every historical order falls to the unknown member.

### The merge

One source row must produce **two** dimension operations (close the old, insert the new), but MERGE does at most one action per matched row.

**The trick:** stage changed customers twice — one copy with `merge_key = customerkey`, one with `merge_key = NULL`.

```sql
MERGE INTO dim_customer AS t
USING staged AS s
  ON t.customerkey = s.merge_key AND t.is_current
WHEN MATCHED THEN UPDATE SET t.valid_to = date_sub(run_date, 1), t.is_current = false
WHEN NOT MATCHED THEN INSERT (...) VALUES (...)
```

**NULL is never equal to any value — not even to another NULL.** So the NULL copy's join condition can never be true and it's guaranteed to fall through to the INSERT branch. A mechanical exploitation of SQL's NULL semantics, not a flag being read.

Unchanged customers never enter the staged set, so MERGE never touches them.

`valid_to = run_date - 1` closes the old row the day *before* the new one opens. No gap, no overlap. Off by one day and a point-in-time join matches either two rows or none.

`.select("s.*")` after the join — otherwise both sides' columns come through and the insert breaks.

`insert_cols` built dynamically from the staged schema, so the column list can't drift.

### Point-in-time join in the fact

```python
.join(dim_customer.alias("c"),
      (F.col("s.customerkey") == F.col("c.customerkey")) &
      (F.col("s.order_date")  >= F.col("c.valid_from")) &
      (F.col("s.order_date")  <= F.col("c.valid_to")),
      "left")
```

Three conditions, not one. **This is what `valid_from`/`valid_to` are for** — everything else was building toward it.

Proof it works: the test customer's historical orders still report under Auberry even though they now live in Berlin.

### The invariant

```
COUNT(*) WHERE is_current  ==  COUNT(DISTINCT customerkey)
```

Always. Total rows grow as history accumulates; current rows stay flat at the customer count. **If total rows ever equalled distinct customers again, history is being lost.**

---

## Measures: store or compute?

Store `revenue_usd` in the fact rather than joining to `dim_product` at query time.

**Why:** product prices change. Computing revenue at query time means 2016 revenue is recalculated with today's price — reports stop reconciling, and last quarter's numbers no longer match what was reported last quarter. A fact records **what happened**; price at sale is part of what happened.

**The honest caveat for this dataset:** the source has no historical prices. Products holds one current price. So storing the measure freezes the price at *load* time, not sale time — an approximation. Say so rather than shipping a number that looks more authoritative than it is.

### The exchange rate that isn't needed

Sales carry `currency_code` but **no local-currency amount** — the only prices are USD, from products. So converting gives `revenue_usd → revenue_local`, a derived estimate in the less useful direction.

Dropped it, and noted in `docs/` why the table exists but isn't used. Better than a number nobody can explain.

---

## Data quality as code

Four assertions in `src/quality.py`, called inside the build functions **before** the write, so a bad table never lands:

- `assert_unique_key` — every dimension's surrogate key is unique
- `assert_one_current_version` — exactly one current row per business key
- `assert_no_null_keys` — no null foreign keys in the fact (referential integrity)
- `assert_row_count_matches` — fact grain preserved through the joins

**Limitation of merge-based loads:** `dim_customer` writes via SQL MERGE, so it can't be validated before the write. A failure leaves a bad table in place. The production answer is merge into a staging table, validate, then swap.

**After every join, check the row count.** Above the expected number means a join matched more than one row and facts were silently duplicated.

---

## What's safe to drop

**The question: does anything live in this table that can't be reconstructed from upstream?**

| Table | Droppable | Why |
|---|---|---|
| `dim_date` | Yes | Generated |
| `dim_product` | Yes | Rebuilt from silver |
| `dim_store` | Yes | Rebuilt from silver |
| `fct_sales` | Yes | Rebuilt from silver + dimensions |
| `dim_customer` | **No** | Holds SCD2 history that exists nowhere else |

Silver only knows Berlin. Drop `dim_customer` and the Auberry row — and the fact the customer ever lived there — is gone permanently.

`upsert_dim_customer` checks `spark.catalog.tableExists` and branches to init or merge, so the job handles both first run and steady state.

---

## Encoding business knowledge, not inferring it

`store_type = CASE WHEN storekey = 0 THEN 'Online'` hardcodes a fact from the data dictionary. The alternative — `square_meters IS NULL` — infers it from a correlation that happens to hold today.

Explicit knowledge beats inferred correlation, and missing source data is far more likely than a second online store.

What makes it safe isn't a better rule, it's the assertion:

```python
if df.filter(F.col("square_meters").isNull()).count() != 1:
    raise ValueError(...)
```

**You can't write a rule that's right in all futures, but you can write one that fails loudly when its assumptions break.**

---

## Errors met

**Missing `customerkey` in the SK hash** — hashing only location columns meant two different customers in the same city got the same surrogate key.

**Dates as strings** — `F.lit("1900-01-01")` without `.cast("date")`. String comparison happens to work for ISO dates and breaks the moment something isn't zero-padded.

**Role-playing join collapsed into one** — `order_date == full_date AND delivery_date == full_date` only matches same-day deliveries. Needs two separate joins with separate aliases.

**Wrong alias in the second date join** — `s.delivery_date == od.full_date` instead of `dd.full_date`. Parsed fine because `od` was in scope; produced all nulls.

**`DELTA_METADATA_MISMATCH` again** — adding `change_hash` to an existing table. `overwriteSchema` for the deliberate change.

**MERGE returns metrics, not the table** — `df.count()` on a MERGE result gives 1. Query the table instead.

**A drop list that enumerates 40 columns from four dimensions** is fragile and breaks silently when any dimension gains a column. Use `select` to name the dozen you want.
