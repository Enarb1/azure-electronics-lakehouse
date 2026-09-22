"""Column-name normalisation shared by every layer.

The source CSVs arrive with human-friendly headers ("Unit Price USD",
"Order Number") and everything downstream expects snake_case. Keeping the
rule in one place means bronze, silver and gold always agree on how a
column is spelled.
"""

import re

REPLACEMENT_CHAR = "\uFFFD"
"""U+FFFD, the character a decoder emits when it cannot decode a byte.

Finding one in a string column means the file was read with the wrong
encoding. See :func:`src.quality.assert_no_mojibake`.
"""


def rename_columns(df):
    """Normalise every column name to snake_case.

    Runs of non-alphanumeric characters collapse to a single underscore, any
    trailing underscores are dropped and the result is lowercased, so
    "Unit Price USD" becomes "unit_price_usd".

    Only *trailing* underscores are stripped, never leading ones: the
    pipeline's own lineage columns are marked with a leading underscore
    (``_ingested_at``, ``_run_date``), and stripping both ends would quietly
    rename them. For the same reason, call this on the raw source columns
    before any lineage columns are added, so the function only ever sees
    source headers.

    Args:
        df: DataFrame whose columns come straight from the source files.

    Returns:
        The same DataFrame with renamed columns. No rows or values change.
    """
    for old in df.columns:
        new = re.sub(r"[^0-9a-zA-Z]+", "_", old).rstrip("_").lower()
        df = df.withColumnRenamed(old, new)
    return df


