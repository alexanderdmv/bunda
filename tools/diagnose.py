import pandas as pd

df = pd.read_csv("data/processed/snapshots.csv")

print("rows:", len(df))
print("cols:", df.columns.tolist())

for col in ["token_address","timestamp_utc","timestamp","price_usd"]:
    print(col, "exists:", col in df.columns)

if "token_address" in df.columns:
    vc = df["token_address"].value_counts()
    print("unique tokens:", vc.size)
    print("max repeats for one token:", vc.max())
    print("tokens with repeats>=2:", (vc >= 2).sum())
