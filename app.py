import time
import requests
import pandas as pd
import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------
# Simple password gate
# ---------------------------
APP_PASSWORD = st.secrets.get("APP_PASSWORD", "")
if not APP_PASSWORD:
    st.error("APP_PASSWORD is not set in secrets.")
    st.stop()

def check_password() -> bool:
    if "auth_ok" not in st.session_state:
        st.session_state.auth_ok = False
    if st.session_state.auth_ok:
        return True
    st.markdown("### 🔐 Enter password")
    pw = st.text_input("Password", type="password")
    if pw:
        if pw == APP_PASSWORD:
            st.session_state.auth_ok = True
            return True
        else:
            st.error("Incorrect password.")
    st.stop()

# ---------------------------
# API setup
# ---------------------------
FMP_API_KEY = st.secrets.get("FMP_API_KEY", "")
if not FMP_API_KEY:
    st.error("FMP_API_KEY is not set in secrets.")
    st.stop()

SCREENER_URL              = "https://financialmodelingprep.com/stable/company-screener"
RATIOS_TTM_BULK_URL       = "https://financialmodelingprep.com/stable/ratios-ttm-bulk"
KEY_METRICS_TTM_BULK_URL  = "https://financialmodelingprep.com/stable/key-metrics-ttm-bulk"

DEFAULT_MARKET_CAP = 500_000_000   # 500M
DEFAULT_VOLUME     = 100_000       # 100k
DEFAULT_LIMIT      = 1000          # per exchange

# ---------------------------
# HTTP helpers (retry + session)
# ---------------------------
def get_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "streamlit-fmp-screener/1.1"})
    return s

def get_json_with_retry(session, url: str, params: dict, retries: int = 2, timeout: int = 25):
    last_exc = None
    for i in range(retries + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_exc = e
            time.sleep(0.8 * (2 ** i))
    raise last_exc

# ---------------------------
# Screener fetch (cached)
# ---------------------------
@st.cache_data(ttl=900, show_spinner=False)  # 15 min cache
def fetch_screener_batch(
    exchanges: tuple,
    market_cap_more: int,
    volume_more: int,
    country: str,
    limit: int,
    include_all_share_classes: bool,
):
    """
    Fetch NASDAQ/NYSE in parallel; return merged, deduped DataFrame.
    """
    session = get_session()

    def _params(exchange: str):
        return {
            "apikey": FMP_API_KEY,
            "country": country,
            "exchange": exchange,
            "marketCapMoreThan": int(market_cap_more),
            "volumeMoreThan": int(volume_more),
            "isEtf": "false",
            "isFund": "false",
            "isActivelyTrading": "true",
            "limit": int(limit),
            "includeAllShareClasses": str(include_all_share_classes).lower(),
        }

    dfs = []
    errors = []
    with ThreadPoolExecutor(max_workers=min(4, len(exchanges))) as ex:
        futs = {ex.submit(get_json_with_retry, session, SCREENER_URL, _params(x)): x for x in exchanges}
        for fut in as_completed(futs):
            exch = futs[fut]
            try:
                data = fut.result()
                df = pd.DataFrame(data)
                if not df.empty:
                    df["sourceExchange"] = exch
                dfs.append(df)
            except Exception as e:
                errors.append(f"{exch}: {e}")

    if errors:
        st.warning("Some requests failed: " + "; ".join(errors))

    if not dfs:
        return pd.DataFrame()

    df_all = pd.concat(dfs, ignore_index=True)

    # Deduplicate by symbol
    if "symbol" in df_all.columns:
        df_all = df_all.drop_duplicates(subset=["symbol"]).reset_index(drop=True)

    return df_all

# ---------------------------
# Valuation via BULK endpoints
# ---------------------------
def add_valuation_columns_bulk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Use FMP bulk endpoints to attach P/E, P/B, EV/EBITDA to df.
    - Ratios TTM Bulk -> priceToEarningsRatioTTM, priceToBookRatioTTM, enterpriseValueMultipleTTM (EV/EBITDA)
    - Key Metrics TTM Bulk -> evToEBITDATTM (fallback for EV/EBITDA)
    """
    if df.empty or "symbol" not in df.columns:
        return df

    symbols = set(df["symbol"].dropna().unique().tolist())
    session = requests.Session()
    session.headers.update({"User-Agent": "streamlit-fmp-screener/bulk"})

    # Fetch both bulks (global payloads), then filter to our symbols
    t0 = time.time()
    try:
        r1 = session.get(RATIOS_TTM_BULK_URL, params={"apikey": FMP_API_KEY}, timeout=60)
        r1.raise_for_status()
        ratios_bulk = pd.DataFrame(r1.json())
    except Exception:
        ratios_bulk = pd.DataFrame(columns=[
            "symbol", "priceToEarningsRatioTTM", "priceToBookRatioTTM", "enterpriseValueMultipleTTM"
        ])

    try:
        r2 = session.get(KEY_METRICS_TTM_BULK_URL, params={"apikey": FMP_API_KEY}, timeout=60)
        r2.raise_for_status()
        km_bulk = pd.DataFrame(r2.json())
    except Exception:
        km_bulk = pd.DataFrame(columns=["symbol", "evToEBITDATTM"])
    t_fetch = time.time() - t0

    # Keep only needed columns and our tickers
    ratios_keep = ["symbol", "priceToEarningsRatioTTM", "priceToBookRatioTTM", "enterpriseValueMultipleTTM"]
    if not ratios_bulk.empty:
        ratios_bulk = ratios_bulk[ratios_keep]
        ratios_bulk = ratios_bulk[ratios_bulk["symbol"].isin(symbols)]

    km_keep = ["symbol", "evToEBITDATTM"]
    if not km_bulk.empty:
        km_bulk = km_bulk[km_keep]
        km_bulk = km_bulk[km_bulk["symbol"].isin(symbols)]

    # Merge onto df
    merged = df.merge(ratios_bulk, on="symbol", how="left").merge(km_bulk, on="symbol", how="left")

    # Coalesce EV/EBITDA: prefer enterpriseValueMultipleTTM (same thing), fallback to evToEBITDATTM
    def _coalesce_ev_ebitda(row):
        a = row.get("enterpriseValueMultipleTTM")
        b = row.get("evToEBITDATTM")
        try:
            return float(a) if pd.notna(a) else (float(b) if pd.notna(b) else None)
        except Exception:
            return a if pd.notna(a) else b

    merged["enterpriseValueOverEBITDATTM"] = merged.apply(_coalesce_ev_ebitda, axis=1)

    # Map to final column names you want
    merged["peRatioTTM"] = pd.to_numeric(merged.get("priceToEarningsRatioTTM"), errors="coerce")
    merged["priceToBookRatioTTM"] = pd.to_numeric(merged.get("priceToBookRatioTTM"), errors="coerce")

    # Optionally drop the raw bulk names (uncomment if you prefer a cleaner table)
    # merged = merged.drop(columns=["priceToEarningsRatioTTM","priceToBookRatioTTM","enterpriseValueMultipleTTM","evToEBITDATTM"], errors="ignore")

    return merged, t_fetch

# ---------------------------
# UI
# ---------------------------
st.set_page_config(page_title="US Equity Universe — FMP Screener", layout="wide", page_icon="📈")

if check_password():
    st.title("US Equity Universe — FMP Stock Screener 📈")
    st.caption("Filters: Country=US, Market Cap ≥ $500M, Avg Daily Volume ≥ 100k, exclude ETFs/Funds, actively trading. Merges NASDAQ + NYSE.")

    with st.sidebar:
        st.header("Filters")
        exchanges = st.multiselect("Exchanges", ["NASDAQ", "NYSE"], default=["NASDAQ", "NYSE"])
        country = st.text_input("Country", "US")
        market_cap_more = st.number_input("Market Cap ≥", value=DEFAULT_MARKET_CAP, min_value=0, step=50_000_000)
        volume_more     = st.number_input("Avg Daily Volume ≥", value=DEFAULT_VOLUME, min_value=0, step=10_000)
        limit           = st.number_input("Per-exchange limit", value=DEFAULT_LIMIT, min_value=10, max_value=3000, step=100)
        include_all_share_classes = st.checkbox("Include all share classes", value=False)

        st.divider()
        st.header("Display")
        show_sector_summary   = st.checkbox("Show sector summary", value=True)
        show_industry_summary = st.checkbox("Show industry summary", value=False)
        show_all_columns      = st.checkbox("Show full table", value=True)
        quick_mode            = st.checkbox("Quick mode (hide full table; summaries + download only)", value=False)

        run_btn = st.button("Run Screener", type="primary", use_container_width=True)

    if run_btn:
        if not exchanges:
            st.warning("Pick at least one exchange.")
            st.stop()

        # 1) Screener fetch
        t0 = time.time()
        with st.spinner("Fetching screener data from FMP…"):
            df = fetch_screener_batch(
                exchanges=tuple(exchanges),
                market_cap_more=int(market_cap_more),
                volume_more=int(volume_more),
                country=country,
                limit=int(limit),
                include_all_share_classes=include_all_share_classes,
            )
        screener_secs = time.time() - t0

        if df.empty:
            st.error("No data returned. Try lowering filters or increasing the per-exchange limit.")
            st.stop()

        # 2) Valuation columns via BULK endpoints
        t1 = time.time()
        with st.spinner("Fetching valuation metrics (bulk)…"):
            df, bulk_secs = add_valuation_columns_bulk(df)
        ratio_secs = time.time() - t1  # local processing; fetch time is bulk_secs

        # Metrics
        n = len(df)
        min_mcap = int(market_cap_more)
        min_vol  = int(volume_more)
        col1, col2, col3, col4 = st.columns(4)
        with col1: st.metric("Total tickers", n)
        with col2: st.metric("Min Market Cap", f"${min_mcap:,}")
        with col3: st.metric("Min Volume", f"{min_vol:,} shares")
        with col4: st.metric("Timing", f"Screener {screener_secs:.1f}s • Bulk fetch {bulk_secs:.1f}s")

        st.caption("Deduped by symbol • Columns shown depend on ticker; we keep them all.")
        st.divider()

        # Optional summaries
        def _fmt_money(x):
            try:
                return f"${x:,.0f}"
            except Exception:
                return x

        if show_sector_summary and "sector" in df.columns:
            st.subheader("Sector Summary")
            sec = (
                df.groupby("sector", dropna=False)
                  .agg(tickers=("symbol", "nunique"),
                       total_mktcap=("marketCap", "sum"),
                       avg_mktcap=("marketCap", "mean"),
                       median_mktcap=("marketCap", "median"))
                  .sort_values("tickers", ascending=False)
            )
            sec_display = sec.copy()
            for c in ["total_mktcap", "avg_mktcap", "median_mktcap"]:
                if c in sec_display.columns:
                    sec_display[c] = sec_display[c].apply(_fmt_money)
            st.dataframe(sec_display, use_container_width=True)

        if show_industry_summary and "industry" in df.columns:
            st.subheader("Industry Summary")
            ind = (
                df.groupby(["sector", "industry"], dropna=False)
                  .agg(tickers=("symbol", "nunique"),
                       total_mktcap=("marketCap", "sum"),
                       avg_mktcap=("marketCap", "mean"))
                  .sort_values(["sector", "tickers"], ascending=[True, False])
            )
            ind_display = ind.copy()
            for c in ["total_mktcap", "avg_mktcap"]:
                if c in ind_display.columns:
                    ind_display[c] = ind_display[c].apply(_fmt_money)
            st.dataframe(ind_display, use_container_width=True)

        st.divider()

        # Compact valuation preview
        val_cols = [
            "symbol", "companyName", "sector", "industry",
            "peRatioTTM", "priceToBookRatioTTM", "enterpriseValueOverEBITDATTM"
        ]
        show_cols = [c for c in val_cols if c in df.columns]
        if show_cols:
            st.subheader("Valuation Columns (preview)")
            st.dataframe(df[show_cols].head(30), use_container_width=True, hide_index=True)

        # Full table (optional)
        if show_all_columns and not quick_mode:
            st.subheader("Results (All columns)")
            st.dataframe(df, use_container_width=True, hide_index=True)

        # Download
        csv = df.to_csv(index=False).encode()
        st.download_button("⬇️ Download CSV", data=csv, file_name="us_universe_full.csv", mime="text/csv")

        st.divider()
        with st.expander("Notes"):
            st.markdown(
                """
- Screener: **/stable/company-screener** (filters: `country`, `exchange`, `marketCapMoreThan`, `volumeMoreThan`, `isEtf=false`, `isFund=false`, `isActivelyTrading=true`, `limit`, `includeAllShareClasses`).
- Valuation (bulk): **/stable/ratios-ttm-bulk** → `priceToEarningsRatioTTM`, `priceToBookRatioTTM`, `enterpriseValueMultipleTTM`; **/stable/key-metrics-ttm-bulk** → `evToEBITDATTM` (fallback).
- EV/EBITDA uses `enterpriseValueMultipleTTM` primarily; falls back to `evToEBITDATTM` when needed.
- Financials/REITs or firms with negative earnings/EBITDA may still show blanks for certain ratios.
- Results cached for 15 minutes; reduce **Per-exchange limit** if you just want a quick run.
                """
            )
