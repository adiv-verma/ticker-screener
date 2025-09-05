import hashlib
import io
import os
from typing import List, Dict

import pandas as pd
import requests
import streamlit as st

# ---------------------------
# Auth (simple password gate)
# ---------------------------
def check_password() -> bool:
    """
    Compare SHA256(user_input) with APP_PASSWORD_HASH in secrets.
    To create a hash locally:
        python - <<'PY'
        import hashlib
        print(hashlib.sha256("YOUR_PASSWORD".encode()).hexdigest())
        PY
    """
    pw_hash = st.secrets.get("APP_PASSWORD_HASH", "")
    if not pw_hash:
        st.error("APP_PASSWORD_HASH is not set in secrets.")
        st.stop()

    if "auth_ok" not in st.session_state:
        st.session_state.auth_ok = False

    if st.session_state.auth_ok:
        return True

    st.markdown("### 🔐 Enter password")
    password = st.text_input("Password", type="password")
    if password:
        if hashlib.sha256(password.encode()).hexdigest() == pw_hash:
            st.session_state.auth_ok = True
            return True
        else:
            st.error("Incorrect password.")
    st.stop()  # abort the app beyond this point


# ---------------------------
# API
# ---------------------------
FMP_API_KEY = st.secrets.get("FMP_API_KEY", "")
if not FMP_API_KEY:
    st.error("FMP_API_KEY is not set in secrets.")
    st.stop()

SCREENER_URL = "https://financialmodelingprep.com/stable/company-screener"


@st.cache_data(ttl=900)  # cache results for 15 minutes
def fetch_screener(exchange: str,
                   market_cap_more: int,
                   volume_more: int,
                   country: str,
                   limit: int,
                   include_all_share_classes: bool) -> pd.DataFrame:
    params: Dict[str, str | int | bool] = {
        "apikey": FMP_API_KEY,
        "country": country,
        "exchange": exchange,
        "marketCapMoreThan": market_cap_more,
        "volumeMoreThan": volume_more,
        "isEtf": "false",
        "isFund": "false",
        "isActivelyTrading": "true",
        "limit": limit,
        "includeAllShareClasses": str(include_all_share_classes).lower(),
    }
    r = requests.get(SCREENER_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    df = pd.DataFrame(data)
    df["sourceExchange"] = exchange  # tag which call it came from
    return df


def make_download(df: pd.DataFrame, filename: str) -> None:
    csv = df.to_csv(index=False).encode()
    st.download_button("⬇️ Download CSV", data=csv, file_name=filename, mime="text/csv")


# ---------------------------
# UI
# ---------------------------
st.set_page_config(page_title="US Equity Universe (FMP Screener)", layout="wide", page_icon="📈")

if check_password():
    st.title("US Equity Universe — FMP Stock Screener 📈")
    st.caption("Filters: US, Market Cap ≥ $500M, Avg Daily Volume ≥ 100k (adjustable). Combines NASDAQ + NYSE.")

    with st.sidebar:
        st.header("Filters")
        exchanges = st.multiselect("Exchanges", ["NASDAQ", "NYSE"], default=["NASDAQ", "NYSE"])
        country = st.text_input("Country", value="US")
        market_cap_more = st.number_input("Market Cap ≥", value=500_000_000, step=50_000_000, min_value=0)
        volume_more = st.number_input("Avg Daily Volume ≥", value=100_000, step=10_000, min_value=0)
        limit = st.number_input("Per-exchange limit", value=1000, min_value=1, step=100)
        include_all_share_classes = st.checkbox("Include all share classes", value=False)
        show_sector_summary = st.checkbox("Show sector summary", value=True)
        show_industry_summary = st.checkbox("Show industry summary", value=False)
        show_all_columns = st.checkbox("Show all columns", value=True)

        run_btn = st.button("Run Screener", type="primary", use_container_width=True)

    if run_btn:
        if not exchanges:
            st.warning("Pick at least one exchange.")
            st.stop()

        all_frames: List[pd.DataFrame] = []
        errs: List[str] = []

        with st.spinner("Fetching data from FMP..."):
            for ex in exchanges:
                try:
                    df_ex = fetch_screener(
                        exchange=ex,
                        market_cap_more=int(market_cap_more),
                        volume_more=int(volume_more),
                        country=country,
                        limit=int(limit),
                        include_all_share_classes=include_all_share_classes,
                    )
                    all_frames.append(df_ex)
                except requests.HTTPError as e:
                    errs.append(f"{ex}: HTTP {e.response.status_code}")
                except Exception as e:
                    errs.append(f"{ex}: {e}")

        if errs:
            st.error("Some requests failed: " + "; ".join(errs))

        if not all_frames:
            st.warning("No data returned.")
            st.stop()

        df = pd.concat(all_frames, ignore_index=True)

        # Deduplicate by symbol (keep first occurrence)
        if "symbol" in df.columns:
            before = len(df)
            df = df.drop_duplicates(subset=["symbol"]).reset_index(drop=True)
            after = len(df)
        else:
            before = after = len(df)

        # Metrics row
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total tickers", after)
        with col2:
            st.metric("Removed duplicates", before - after)
        with col3:
            st.metric("Min Market Cap (filter)", f"${int(market_cap_more):,}")
        with col4:
            st.metric("Min Volume (filter)", f"{int(volume_more):,} shares")

        st.subheader("Results")
        st.caption("All columns returned by FMP Screener + `sourceExchange` tag.")
        st.dataframe(df, use_container_width=True, hide_index=True)

        make_download(df, "us_universe_full.csv")

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
                  .agg(
                      tickers=("symbol", "nunique"),
                      total_mktcap=("marketCap", "sum"),
                      avg_mktcap=("marketCap", "mean"),
                      median_mktcap=("marketCap", "median"),
                  )
                  .sort_values("tickers", ascending=False)
            )
            sec_display = sec.copy()
            for col in ["total_mktcap", "avg_mktcap", "median_mktcap"]:
                if col in sec_display:
                    sec_display[col] = sec_display[col].apply(_fmt_money)
            st.dataframe(sec_display, use_container_width=True)
            make_download(sec.reset_index(), "sector_summary.csv")

        if show_industry_summary and "industry" in df.columns:
            st.subheader("Industry Summary")
            ind = (
                df.groupby(["sector", "industry"], dropna=False)
                  .agg(
                      tickers=("symbol", "nunique"),
                      total_mktcap=("marketCap", "sum"),
                      avg_mktcap=("marketCap", "mean"),
                  )
                  .sort_values(["sector", "tickers"], ascending=[True, False])
            )
            ind_display = ind.copy()
            for col in ["total_mktcap", "avg_mktcap"]:
                if col in ind_display:
                    ind_display[col] = ind_display[col].apply(_fmt_money)
            st.dataframe(ind_display, use_container_width=True)
            make_download(ind.reset_index(), "industry_summary.csv")

        # Helpful tips
        with st.expander("Notes & Tips"):
            st.markdown(
                """
- This uses **FMP Stock Screener** (`/stable/company-screener`) with your filters.
- If you suspect there are >1000 qualifying symbols in an exchange, raise the **per-exchange limit**.
- `exchange` in the response is the full venue name (e.g., *NASDAQ Global Select*); `exchangeShortName` is *NASDAQ* or *NYSE*.
- We add `sourceExchange` to identify which call (NASDAQ or NYSE) produced the row before deduplication.
- Numbers like `500_000_000` are just Python-friendly separators (same as `500000000`).
                """
            )
