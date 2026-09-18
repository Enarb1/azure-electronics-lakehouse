import re
from pyspark.sql import functions as F, types as T

REPLACEMENT_CHAR = "\uFFFD"

def rename_columns(df):
    for old in df.columns:
        new = re.sub(r"[^0-9a-zA-Z]+", "_", old).rstrip("_").lower()
        df = df.withColumnRenamed(old, new)
    return df


def assert_no_mojibake(df, entity: str = ""):
    """Fail if any string column contains the Unicode replacement character,
    which means the file was read with the wrong encoding."""
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
