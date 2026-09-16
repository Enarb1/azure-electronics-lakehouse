import re

def clean_columns(df):
    for old in df.columns:
        new = re.sub(r"[^0-9a-zA-Z]+", "_", old).strip("_").lower()
        df = df.withColumnRenamed(old, new)
    return df

