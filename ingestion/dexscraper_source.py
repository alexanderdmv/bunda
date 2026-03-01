def fetch_market_snapshots() -> list[dict]:
    """
    Returns list of normalized token snapshots:
    {
        symbol,
        chain,
        liquidity_usd,
        volume_5m,
        volume_1h,
        price_change_5m,
        pair_created_at
    }
    """
