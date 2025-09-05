import time
import requests
import pandas as pd
import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed

# --------------------------------
# Basic app setup
# --------------------------------
st.set_page_config(page_title="US Equity Universe — FMP Screener", layout="wide", page_icon="📈")

# Secrets
APP_PASSWORD = st.secrets.get("APP_PASSWORD", "")
FMP_API_KEY = st.secrets.get("FMP_API_KEY", "")
if not APP_PASSWORD:
    st.error("APP_PASSWORD is not set in secrets.")
    st.stop()
if not FMP_API_KEY:
    st.error("FMP_API_KEY is not set in secrets.")
    st.stop()

# --------------------------------
# Password gate
# --------------------------------
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

# --------------------------------
# Endpoints & defaults
# --------------------------------
SCREENER_URL = "https://financialmodelingprep.com/stable/company-screener"
RATIOS_TTM_V3 = "https://financialmodelingprep.com/api/v3/ratios-ttm"

DEFAULT_MARKET_CAP = 500_000_000   # $500M
DEFAULT_VOLUME     = 100_000       # 100k shares
DEFAULT_LIMIT      = 1000          # per exchange

# --------------------------------
# HTTP helpers
# --------------------------------
def get_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "streamlit-fmp-screener/2.0"})
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

# --------------------------------
# Screener fetch (cached)
# --------------------------------
@st.cache_data(ttl=900, show_spinner=False)  # 15 minutes
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

    dfs, errors = [], []
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
    if "symbol" in df_all.columns:
        df_all = df_all.drop_duplicates(subset=["symbol"]).reset_index(drop=True)
    return df_all

# --------------------------------
# Per-symbol ratios (reliable)
# --------------------------------
def _fetch_ratios_one(sym: str, session, timeout: int = 20) -> dict:
    """
    GET /api/v3/ratios-ttm/{symbol} and pull the 3 fields we need.
    Returns at least {"symbol": sym} so merge is safe on errors.
    """
    try:
        url = f"{RATIOS_TTM_V3}/{sym}"
        r = session.get(url, params={"apikey": FMP_API_KEY}, timeout=timeout)
        r.raise_for_status()
        js = r.json()
        row = js[0] if js else {}
        return {
            "symbol": sym,
            "peRatioTTM": row.get("peRatioTTM"),
            "priceToBookRatioTTM": row.get("priceToBookRatioTTM"),
            "enterpriseValueOverEBITDATTM": row.get("enterpriseValueOverEBITDATTM"),
        }
    except Exception:
        return {"symbol": sym}

def add_valuation_columns_from_symbols(
    df: pd.DataFrame,
    max_workers: int = 8,
    throttle_every: int = 60,
    sleep_secs: float = 1.0
) -> pd.DataFrame:
    """
    Fetch P/E, P/B, EV/EBITDA for exactly the symbols in df using /api/v3/ratios-ttm/{symbol}.
    - max_workers: parallel requests (tune down if you see HTTP 429)
    - throttle_every: every N completed requests, sleep briefly to be polite
    """
    if df.empty or "symbol" not in df.columns:
        return df

    session = requests.Session()
    session.headers.update({"User-Agent": "streamlit-fmp-screener/per-symbol"})

    symbols = df["symbol"].dropna().unique().tolist()
    results = []

    progress = st.progress(0, text="Fetching valuation ratios (per symbol)…")

    done, total = 0, len(symbols)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {pool.submit(_fetch_ratios_one, s, session): s for s in symbols}
        for fut in as_completed(futs):
            results.append(fut.result())
            done += 1
            if throttle_every and (done % throttle_every == 0):
                time.sleep(sleep_secs)
            if done % 10 == 0 or done == total:
                progress.progress(done / total, text=f"Fetching valuation ratios… {done}/{total}")

    progress.empty()

    ratios_df = pd.DataFrame(results)
    merged = df.merge(ratios_df, on="symbol", how="left")
    return merged

# --------------------------------
# UI
# --------------------------------
if check_password():
    st.title("US Equity Universe — FMP Stock Screener 📈")
    st.caption("Country=US • Mkt Cap ≥ $500M • Avg Vol ≥ 100k • Exclude ETFs/Funds • Actively trading • NASDAQ + NYSE")

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

        # 1) Screener
        t0 = time.time()
        with st.spinner("Fetching screener data…"):
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

        # 2) Valuation ratios per-symbol
        t1 = time.time()
        with st.spinner("Fetching valuation metrics (per symbol)…"):
            df = add_valuation_columns_from_symbols(
                df,
                max_workers=8,          # tune if you see 429s
                throttle_every=60,      # pause every N requests
                sleep_secs=1.0
            )
        ratio_secs = time.time() - t1

        # Metrics
        n = len(df)
        col1, col2, col3, col4 = st.columns(4)
        with col1: st.metric("Total tickers", n)
        with col2: st.metric("Min Market Cap", f"${int(market_cap_more):,}")
        with col3: st.metric("Min Volume", f"{int(volume_more):,} shares")
        with col4: st.metric("Timing", f"Screener {screener_secs:.1f}s • Ratios {ratio_secs:.1f}s")

        st.caption("Deduped by symbol • Some ratios may be blank for loss-making firms or financials/REITs.")
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
