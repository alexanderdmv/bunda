import pandas as pd


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Делает базовые фичи из табличного df.
    Важно: файл должен быть синтаксически валиден (раньше тут было '...').
    """
    df = df.copy()
    now = pd.Timestamp.utcnow()

    # created_at -> age_minutes
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
        df["age_minutes"] = (now - df["created_at"]).dt.total_seconds() / 60.0

    # dev_wallet_share, если есть dev_wallet_balance и total_supply
    if "dev_wallet_balance" in df.columns and "total_supply" in df.columns:
        df["dev_wallet_balance"] = pd.to_numeric(df["dev_wallet_balance"], errors="coerce")
        df["total_supply"] = pd.to_numeric(df["total_supply"], errors="coerce")
        df["dev_wallet_share"] = df["dev_wallet_balance"] / df["total_supply"]

    # top10_holders может быть либо долей, либо балансом
    if "top10_holders" in df.columns:
        df["top10_holders"] = pd.to_numeric(df["top10_holders"], errors="coerce")

    numeric_cols = [
        "liquidity_usd",
        "age_minutes",
        "dev_wallet_share",
        "top10_holders",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df
