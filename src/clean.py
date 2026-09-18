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


def assert_no_rescued_data(df, entity=""):
    if "_rescued_data" not in df.columns:
        return df

    bad = df.filter(F.col("_rescued_data").isNotNull()).count()

    if bad:
        raise ValueError(f"{entity}: {bad} rows have rescued data — schema mismatch at ingestion")

    return df.drop("_rescued_data")

def clean_money(col_name):
    return F.regexp_replace(F.col(col_name), r"[$,\s]", "").cast("decimal(10,2)")


def to_int(df, cols):
    for col in df.columns:
        if col in cols:
            df = df.withColumn(col, F.col(col).cast("int"))

    return df


def to_date(df, cols, fmt="M/d/yyyy"):

    for col in df.columns:
        if col in cols:
            before_count = df.filter(F.col(col).isNotNull()).count()
            df = df.withColumn(col, F.try_to_date(F.col(col), fmt))
            after_count = df.filter(F.col(col).isNotNull()).count()

            if after_count < before_count:
                raise ValueError(f"{col}: {before_count - after_count} values failed to parse as {fmt}")

    return df


def str_clean(df, cols):
    for col in df.columns:
        if col in cols:
            df = df.withColumn(col, F.trim(F.col(col)))

    return df


def apply_types(df, types_dict):
    d_type_mapper =  {
    "int": to_int,
    "date": to_date,
    "string": str_clean,
}
    for d_type, cols in types_dict.items():
        df = d_type_mapper[d_type](df, cols)

    return df


def type_sales(df):
    df = apply_types(df, SILVER_TYPES["sales"])

    return df


def type_customers(df):
    df = apply_types(df, SILVER_TYPES["customers"])

    return df


def type_products(df):
    df = apply_types(df, SILVER_TYPES["products"])
    return (
        df
        .withColumn("unit_price_usd", clean_money("unit_price_usd"))
        .withColumn("unit_cost_usd", clean_money("unit_cost_usd"))
    )


def type_stores(df):
    df = apply_types(df, SILVER_TYPES["stores"])

    return df


def type_exchange_rates(df):
    df = apply_types(df, SILVER_TYPES["exchange_rates"])

    return (
        df
        .withColumn("exchange", F.col("exchange").cast("decimal(10,4)"))
    )
