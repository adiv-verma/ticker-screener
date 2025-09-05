# --- add near the top with other imports ---
import math

RATIOS_URL = "https://financialmodelingprep.com/stable/ratios-ttm"

def fetch_ratios_ttm(symbol: str, session: requests.Session) -> dict:
    """
    Returns a dict with peRatioTTM, priceToBookRatioTTM, enterpriseValueOverEBITDATTM for one symbol.
    """
    params = {"apikey": FMP_API_KEY, "symbol": symbol}
    r = session.get(RATIOS_URL, params=params, timeout=20)
    r.raise_for_status()
    js = r.json()
    if not js:
        return {}
    row = js[0]
    return {
        "symbol": symbol,
        "peRatioTTM": row.get("peRatioTTM"),
        "priceToBookRatioTTM": row.get("priceToBookRatioTTM"),
        "enterpriseValueOverEBITDATTM": row.get("enterpriseValueOverEBITDATTM"),
    }

def enrich_with_ratios(df: pd.DataFrame, max_symbols: int | None = 400) -> pd.DataFrame:
    """
    Fetch ratios for symbols in df (optionally cap to first N to keep it snappy), then compute
    industry medians and the margin-of-safety flags.
    """
    if df.empty or "symbol" not in df.columns:
        return df

    # (Optional) limit how many we fetch during early testing
    target = df.copy()
    if max_symbols is not None and len(target) > max_symbols:
        target = target.head(max_symbols).copy()

    session = requests.Session()
    session.headers.update({"User-Agent": "streamlit-fmp-screener/ratios"})

    out = []
    for sym in target["symbol"]:
        try:
            out.append(fetch_ratios_ttm(sym, session))
        except Exception:
            # skip on error
            out.append({"symbol": sym})

    ratios = pd.DataFrame(out)
    df2 = df.merge(ratios, on="symbol", how="left")

    # Compute industry medians for each ratio
    for col in ["peRatioTTM", "priceToBookRatioTTM", "enterpriseValueOverEBITDATTM"]:
        med_col = f"{col}_industry_median"
        df2[med_col] = df2.groupby("industry")[col].transform("median")

    # Helper: ≥20% below industry median (and median > 0)
    def underval_flag(x, med):
        if pd.notna(x) and pd.notna(med) and med > 0:
            return x <= 0.8 * med
        return False

    df2["underval_pe"] = [underval_flag(x, m) for x, m in zip(df2["peRatioTTM"], df2["peRatioTTM_industry_median"])]
    df2["underval_pb"] = [underval_flag(x, m) for x, m in zip(df2["priceToBookRatioTTM"], df2["priceToBookRatioTTM_industry_median"])]
    df2["underval_evebitda"] = [underval_flag(x, m) for x, m in zip(
        df2["enterpriseValueOverEBITDATTM"], df2["enterpriseValueOverEBITDATTM_industry_median"]
    )]

    df2["underval_count"] = df2[["underval_pe", "underval_pb", "underval_evebitda"]].sum(axis=1)
    df2["margin_of_safety_ok"] = df2["underval_count"] >= 2

    return df2
