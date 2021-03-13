import pandas as pd

def deduplicate(df):
    df.index = pd.to_datetime(df.index, utc=True)
    fi = ~df.index.duplicated()
    df = df.loc[fi]
    return df
