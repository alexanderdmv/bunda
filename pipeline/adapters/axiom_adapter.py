import random
import time
from datetime import datetime

def stream_tokens():
    while True:
        token = {
            "symbol": f"MEME{random.randint(1000,9999)}",
            "liquidity_usd": random.randint(5000, 300000),
            "age_minutes": random.randint(1, 120),
            "dev_wallet_share": round(random.uniform(0.05, 0.5), 2),
            "top10_holders": round(random.uniform(0.2, 0.9), 2),
            "timestamp": datetime.utcnow()
        }
        yield token
        time.sleep(1)
