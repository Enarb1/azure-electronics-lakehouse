"""Type-casting and cleaning rules for the silver layer.

Bronze holds every column as a string, exactly as the CSVs delivered it.
Silver is where columns get their real types, and this module holds the
rules for doing that.

The split between config and code is deliberate:

* ``SILVER_TYPES`` is config - variation *within* a fixed operation. Column
  A is an int, column B is a date: same cast, different inputs.
* The ``type_*`` functions are code - variation in the operation itself.
  ``clean_money`` strips currency symbols before casting, and expressing
  that in config would mean inventing a mini-language and maintaining an
  interpreter for it.

So each entity gets one thin function: config-driven for the boring
columns, plain PySpark for the special cases.
"""

from pyspark.sql import functions as F

SILVER_TYPES = {
    "customers" : {
        "int": ["customerkey"],
        "date": ["birthday"],
        "string": ["gender", "name", "city", "state_code", "state", "zip_code", "country", "continent"]
    },
    "sales": {
        "int": ["order_number", "line_item", "customerkey", "storekey", "productkey", "quantity"],
        "date": ["order_date", "delivery_date"],
        "string": ["currency_code"]
    },
    "products": {
        "int": ["productkey", "subcategorykey", "categorykey"],
        "string": ["product_name", "brand", "color", "subcategory", "category"]
    },
    "stores": {
        "int": ["storekey", "square_meters"],
        "date": ["open_date"],
        "string": ["country", "state"]
    },
    "exchange_rates": {
        "date": ["date"],
        "string": ["currency"],
    }
}
"""Target type per column, per entity: ``{entity: {type_name: [columns]}}``.

``type_name`` is one of the keys ``apply_types`` knows how to handle
("int", "date", "string"). A column not listed keeps the string type
bronze gave it.

Types follow what a column *means*, not what the first few rows happen to
look like: ``quantity`` is an int because you cannot sell half an item.
Money is not in here at all - it needs stripping before casting, so it is
handled in code by :func:`clean_money`.
"""


def clean_money(col_name):
    """Build a column expression turning a money string into a decimal.

    Money arrives as ``"$1,234.56"``. The currency symbol, thousands
    separators and whitespace are stripped, and the result cast to
    ``decimal(10,2)``.

    Decimal, never float: floating point cannot represent most decimal
    fractions exactly, and summing a million transactions makes the error
    visible in a financial report.

    The strip list is explicit rather than "remove anything that is not a
    digit", so an unexpected character survives into the cast and turns the
    value null, instead of being quietly filed off to leave a plausible-looking
    but wrong number.

    Args:
        col_name: Name of the string column to convert.

    Returns:
        A Column expression, to be passed to ``withColumn``.
    """
    return F.regexp_replace(F.col(col_name), r"[$,\s]", "").cast("decimal(10,2)")


def to_int(df, cols):
    """Cast the named columns to int.

    Args:
        df: DataFrame to convert.
        cols: Column names to cast. Names not present in ``df`` are ignored, so
            one shared config can cover entities that do not all carry every
            column.

    Returns:
        A new DataFrame with those columns typed as int.

    Note:
        Spark's cast is lenient - a value that is not a number becomes null
        rather than raising. Unlike :func:`to_date`, this is not guarded, so
        only use it on columns the source guarantees are numeric.
    """
    for col in df.columns:
        if col in cols:
            df = df.withColumn(col, F.col(col).cast("int"))

    return df


def to_date(df, cols, fmt="M/d/yyyy"):
    """Parse the named columns as dates, failing if any value does not parse.

    ``try_to_date`` returns null for a value it cannot parse rather than
    raising, which on its own would turn a format mismatch into a column of
    silent nulls. So the non-null count is taken before and after the parse and
    any drop is treated as an error - the same "make the silent thing loud"
    rule the quality assertions follow.

    Args:
        df: DataFrame to convert.
        cols: Column names to parse. Names not present in ``df`` are ignored.
        fmt: Date pattern of the source. Defaults to the unpadded US-style
            format the CSV extracts use.

    Returns:
        A new DataFrame with those columns typed as date.

    Raises:
        ValueError: If any non-null value failed to parse, reporting how many.

    Note:
        The before/after counts are two Spark actions per column, which makes
        this the expensive part of the silver build. A deliberate trade of
        speed for correctness at this data size.
    """
    for col in df.columns:
        if col in cols:
            before_count = df.filter(F.col(col).isNotNull()).count()
            df = df.withColumn(col, F.try_to_date(F.col(col), fmt))
            after_count = df.filter(F.col(col).isNotNull()).count()

            if after_count < before_count:
                raise ValueError(f"{col}: {before_count - after_count} values failed to parse as {fmt}")

    return df


def str_clean(df, cols):
    """Trim leading and trailing whitespace from the named columns.

    A trailing space is invisible in a result grid but makes ``"Berlin "`` and
    ``"Berlin"`` two different group-by keys - and, in ``dim_customer``, two
    different SCD2 versions of the same person.

    Args:
        df: DataFrame to clean.
        cols: Column names to trim. Names not present in ``df`` are ignored.

    Returns:
        A new DataFrame with those columns trimmed.
    """
    for col in df.columns:
        if col in cols:
            df = df.withColumn(col, F.trim(F.col(col)))

    return df


def apply_types(df, types_dict):
    """Apply every rule in one entity's type config to a DataFrame.

    Args:
        df: DataFrame to convert, normally read straight from bronze.
        types_dict: One entity's entry from :data:`SILVER_TYPES`, mapping a
            type name to the columns that should have it.

    Returns:
        A new DataFrame with all the configured casts applied.

    Raises:
        KeyError: If the config names a type with no handler. That is the
            wanted behaviour: a typo'd type name should stop the build rather
            than quietly skip the cast.
    """
    d_type_mapper =  {
    "int": to_int,
    "date": to_date,
    "string": str_clean,
}
    for d_type, cols in types_dict.items():
        df = d_type_mapper[d_type](df, cols)

    return df


def type_sales(df):
    """Type the ``sales`` entity for silver.

    Args:
        df: The bronze ``sales`` DataFrame.

    Returns:
        The typed DataFrame.
    """
    df = apply_types(df, SILVER_TYPES["sales"])

    return df


def type_customers(df):
    """Type the ``customers`` entity for silver.

    ``birthday`` is stored as given and no age is derived here: age depends on
    when you ask, so computing it during a transformation would make the same
    input produce different output tomorrow. "As of when" is a question for
    gold or the BI tool.

    Args:
        df: The bronze ``customers`` DataFrame.

    Returns:
        The typed DataFrame.
    """
    df = apply_types(df, SILVER_TYPES["customers"])

    return df


def type_products(df):
    """Type the ``products`` entity for silver.

    On top of the configured casts, the two money columns need their currency
    symbols and separators stripped before they can be cast, so they are
    handled in code - see :func:`clean_money`.

    Args:
        df: The bronze ``products`` DataFrame.

    Returns:
        The typed DataFrame, with price and cost as ``decimal(10,2)``.
    """
    df = apply_types(df, SILVER_TYPES["products"])
    return (
        df
        .withColumn("unit_price_usd", clean_money("unit_price_usd"))
        .withColumn("unit_cost_usd", clean_money("unit_cost_usd"))
    )


def type_stores(df):
    """Type the ``stores`` entity for silver.

    Args:
        df: The bronze ``stores`` DataFrame.

    Returns:
        The typed DataFrame.
    """
    df = apply_types(df, SILVER_TYPES["stores"])

    return df


def type_exchange_rates(df):
    """Type the ``exchange_rates`` entity for silver.

    ``exchange`` gets four decimal places rather than the two used for money:
    a rate is a ratio, not an amount, and rounding it to cents before
    multiplying would push a visible error into every converted value.

    Args:
        df: The bronze ``exchange_rates`` DataFrame.

    Returns:
        The typed DataFrame.

    Note:
        Loaded for completeness, but nothing downstream uses it. Sales carry a
        ``currency_code`` yet no local-currency amount, and the only prices in
        the source are already USD, so a conversion would run in the less
        useful direction. See ``docs/notes/day5_notes.md``.
    """
    df = apply_types(df, SILVER_TYPES["exchange_rates"])

    return (
        df
        .withColumn("exchange", F.col("exchange").cast("decimal(10,4)"))
    )
