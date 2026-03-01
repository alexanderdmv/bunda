def preprocess(df):
    df = df.copy()
    df = df.drop_duplicates()
    df["liquidity_usd"] = pd.to_numeric(df["liquidity_usd"], errors="coerce")
    return df
