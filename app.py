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

SCREENER_URL = "https://financialmodelingprep.com/stable/company-screener"
RATIOS_URL   = "https://financialmodelingprep.com/stable/ratios-ttm"

DEFAULT_MARKET_CAP = 500_000_000   # 500M
DEFAULT_VOLUME     = 100_000       # 100k
DEFAULT_LIMIT      = 1000          # per exchange

# ---------------------------
# HTTP helpers (retry + session)
# ---------------------------
def get_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "streamlit-fmp-screener/1.0"})
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
# Valuation ratios (P/E, P/B, EV/EBITDA)
# ---------------------------
def _fetch_ratios_one(symbol: str, session, timeout: int = 20) -> dict:
    """
    Fetch P/E, P/B, EV/EBITDA (TTM) for a single symbol.
    Returns keys: symbol, peRatioTTM, priceToBookRatioTTM, enterpriseValueOverEBITDATTM
    """
    try:
        r = session.get(RATIOS_URL, params={"apikey": FMP_API_KEY, "symbol": symbol}, timeout=timeout)
        r.raise_for_status()
        js = r.json()
        row = js[0] if js else {}
        return {
            "symbol": symbol,
            "peRatioTTM": row.get("peRatioTTM"),
            "priceToBookRatioTTM": row.get("priceToBookRatioTTM"),
            "enterpriseValueOverEBITDATTM": row.get("enterpriseValueOverEBITDATTM"),
        }
    except Exception:
        return {"symbol": symbol}

def add_valuation_columns(df: pd.DataFrame, max_workers: int = 8) -> pd.DataFrame:
    """
    Fetch ratios for all symbols in df (parallelized) and merge back.
    Adds: peRatioTTM, priceToBookRatioTTM, enterpriseValueOverEBITDATTM
    """
    if df.empty or "symbol" not in df.columns:
        return df

    session = requests.Session()
    session.headers.update({"User-Agent": "streamlit-fmp-screener/ratios"})

    symbols = df["symbol"].dropna().unique().tolist()
    results = []

    progress = st.progress(0, text="Fetching valuation ratios…")

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch_ratios_one, sym, session): sym for sym in symbols}
        total = len(futures)
        done = 0
        for fut in as_completed(futures):
            results.append(fut.result())
            done += 1
            if done % 10 == 0 or done == total:
                progress.progress(done / total, text=f"Fetching valuation ratios… {done}/{total}")

    progress.empty()

    ratios_df = pd.DataFrame(results)
    out = df.merge(ratios_df, on="symbol", how="left")
    return out

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
        quick_mode            = st.checkbox("Quick mode (faster render: hide table, show only summaries + download)", value=False)

        run_btn = st.button("Run Screener", type="primary", use_container_width=True)

    if run_btn:
        if not exchanges:
            st.warning("Pick at least one exchange.")
            st.stop()

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
        fetch_secs = time.time() - t0

        if df.empty:
            st.error("No data returned. Try lowering filters or increasing the per-exchange limit.")
            st.stop()

        # Add valuation columns (P/E, P/B, EV/EBITDA)
        t1 = time.time()
        df = add_valuation_columns(df)
        ratio_secs = time.time() - t1

        # Metrics
        n = len(df)
        min_mcap = int(market_cap_more)
        min_vol  = int(volume_more)
        col1, col2, col3, col4 = st.columns(4)
        with col1: st.metric("Total tickers", n)
        with col2: st.metric("Min Market Cap", f"${min_mcap:,}")
        with col3: st.metric("Min Volume", f"{min_vol:,} shares")
        with col4: st.metric("Timing", f"Screener {fetch_secs:.1f}s • Ratios {ratio_secs:.1f}s")

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
- Screener endpoint: **/stable/company-screener** (filters: `country`, `exchange`, `marketCapMoreThan`, `volumeMoreThan`, `isEtf=false`, `isFund=false`, `isActivelyTrading=true`, `limit`, `includeAllShareClasses`).
- Valuation endpoint: **/stable/ratios-ttm?symbol=...** for `peRatioTTM`, `priceToBookRatioTTM`, `enterpriseValueOverEBITDATTM`.
- Performance tips:
  - Reduce **Per-exchange limit** if you don’t need the full 1000.
  - Toggle **Quick mode** to skip rendering the full table.
  - Ratios are fetched per symbol in parallel; total time scales with the number of tickers and your FMP rate limits.
- Numeric literals like `500_000_000` are the same as `500000000` in Python (underscores for readability).
                """
            )
