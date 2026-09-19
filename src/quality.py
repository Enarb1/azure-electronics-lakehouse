from pyspark.sql import functions as F

def assert_unique_key(df, key_col, table_name=""):
    total = df.count()
    distinct = df.select(key_col).distinct().count()
    if total != distinct:
        raise ValueError(
            f"{table_name}: {total - distinct} duplicate values in {key_col}"
        )
    return df


def assert_one_current_version(df, business_key, table_name=""):
    current = df.filter("is_current")
    n_current = current.count()
    n_keys = current.select(business_key).distinct().count()
    if n_current != n_keys:
        raise ValueError(
            f"{table_name}: {n_current} current rows for {n_keys} distinct {business_key}"
        )
    return df


def assert_no_null_keys(df, key_cols, table_name=""):
    for c in key_cols:
        nulls = df.filter(F.col(c).isNull()).count()
        if nulls:
            raise ValueError(f"{table_name}: {nulls} null values in {c}")
    return df


def assert_row_count_matches(df, expected, table_name=""):
    actual = df.count()
    if actual != expected:
        raise ValueError(
            f"{table_name}: expected {expected} rows, got {actual}"
        )
    return df