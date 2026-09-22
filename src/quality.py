"""Data-quality assertions that fail the job loudly.

Every function here takes a DataFrame, checks one invariant, and either
raises ``ValueError`` with a count of the offending rows or returns the
DataFrame unchanged. Returning the DataFrame lets a check sit inline in a
build chain, and calling it *before* the write means a table that violates
an invariant never lands.

They all follow the same rule: check the thing that would otherwise be
silent. A pipeline that quietly produces wrong numbers is worse than one
that fails.
"""

from pyspark.sql import functions as F, types as T

from src.transform import REPLACEMENT_CHAR


def assert_unique_key(df, key_col, table_name=""):
    """Assert that ``key_col`` holds no duplicate values.

    Used on a dimension's surrogate key. A duplicate there means a fact row
    joins to more than one dimension row, and the measures are silently
    doubled.

    Args:
        df: DataFrame to check.
        key_col: Name of the column that must be unique.
        table_name: Table name, used only in the error message.

    Returns:
        ``df`` unchanged, so the call can sit inside a build chain.

    Raises:
        ValueError: If any value in ``key_col`` appears more than once.
    """
    total = df.count()
    distinct = df.select(key_col).distinct().count()
    if total != distinct:
        raise ValueError(
            f"{table_name}: {total - distinct} duplicate values in {key_col}"
        )
    return df


def assert_one_current_version(df, business_key, table_name=""):
    """Assert an SCD2 dimension has exactly one current row per business key.

    The invariant of a Type 2 dimension::

        COUNT(*) WHERE is_current == COUNT(DISTINCT <business_key>)

    Total rows grow as history accumulates, but the number of current rows
    stays flat at the number of business keys. Two current rows for one key
    means a merge inserted a new version without closing the old one; none
    means it closed a version without inserting its replacement.

    Args:
        df: DataFrame with an ``is_current`` boolean column.
        business_key: Natural key of the dimension, e.g. "customerkey".
        table_name: Table name, used only in the error message.

    Returns:
        ``df`` unchanged.

    Raises:
        ValueError: If the number of current rows differs from the number of
            distinct business keys among them.
    """
    current = df.filter("is_current")
    n_current = current.count()
    n_keys = current.select(business_key).distinct().count()
    if n_current != n_keys:
        raise ValueError(
            f"{table_name}: {n_current} current rows for {n_keys} distinct {business_key}"
        )
    return df


def assert_no_null_keys(df, key_cols, table_name=""):
    """Assert that none of the given key columns contain nulls.

    A null surrogate key in a fact table means a join found no matching
    dimension row. The joins are left joins, so the fact row survives rather
    than disappearing, and the orphan stays invisible until an inner join in
    some report silently drops it.

    Args:
        df: DataFrame to check.
        key_cols: Column names that must be fully populated.
        table_name: Table name, used only in the error message.

    Returns:
        ``df`` unchanged.

    Raises:
        ValueError: On the first column found to contain a null.
    """
    for c in key_cols:
        nulls = df.filter(F.col(c).isNull()).count()
        if nulls:
            raise ValueError(f"{table_name}: {nulls} null values in {c}")
    return df


def assert_row_count_matches(df, expected, table_name=""):
    """Assert the DataFrame has exactly ``expected`` rows.

    Run after the fact table's joins to prove the grain survived them. More
    rows than expected means a join matched several dimension rows and
    duplicated facts; fewer means rows were dropped. Both are silent without
    this check, which is why the count is taken from the source table rather
    than eyeballed.

    Args:
        df: DataFrame to check.
        expected: Row count the DataFrame must have, normally the row count of
            the source table the fact was built from.
        table_name: Table name, used only in the error message.

    Returns:
        ``df`` unchanged.

    Raises:
        ValueError: If the actual row count differs from ``expected``.
    """
    actual = df.count()
    if actual != expected:
        raise ValueError(
            f"{table_name}: expected {expected} rows, got {actual}"
        )
    return df


def assert_no_mojibake(df, entity: str = ""):
    """Assert that no string column contains the Unicode replacement character.

    U+FFFD is what a decoder emits when it meets a byte it cannot decode, so
    finding one means the file was read with the wrong encoding - the
    ``customers`` extract, for instance, is ISO-8859-1 rather than UTF-8. The
    rows still land and still look plausible; only the accented characters are
    destroyed, which is exactly the kind of damage that goes unnoticed.

    Args:
        df: DataFrame to check. A table with no string columns passes trivially.
        entity: Entity name, used only in the error message.

    Returns:
        ``df`` unchanged.

    Raises:
        ValueError: If any string column contains U+FFFD.
    """
    string_cols = [f.name for f in df.schema.fields
                   if isinstance(f.dataType, T.StringType)]
    if not string_cols:
        return df

    condition = None
    for col in string_cols:
        c = F.col(col).contains(REPLACEMENT_CHAR)
        condition = c if condition is None else (condition | c)

    bad = df.filter(condition).count()
    if bad:
        raise ValueError(
            f"{entity}: {bad} rows contain U+FFFD — wrong encoding on read"
        )
    return df


def assert_no_rescued_data(df, entity=""):
    """Assert Auto Loader rescued nothing, then drop the rescue column.

    Auto Loader stashes any value that did not fit the inferred schema into
    ``_rescued_data`` as JSON, instead of failing or discarding it. That is the
    right behaviour for bronze, where nothing should be lost, but the wrong
    thing to carry into silver: a populated ``_rescued_data`` means the source
    changed shape and should stop the build.

    Dropping the column only after the check is the point. Carrying it forward
    hides the problem; dropping it without looking destroys the evidence.

    Args:
        df: DataFrame that may carry a ``_rescued_data`` column.
        entity: Entity name, used only in the error message.

    Returns:
        ``df`` without ``_rescued_data``, or unchanged if the column is absent.

    Raises:
        ValueError: If any row has a non-null ``_rescued_data``.
    """
    if "_rescued_data" not in df.columns:
        return df

    bad = df.filter(F.col("_rescued_data").isNotNull()).count()

    if bad:
        raise ValueError(f"{entity}: {bad} rows have rescued data — schema mismatch at ingestion")

    return df.drop("_rescued_data")
