import os
import requests
import pandas as pd
import streamlit as st

# ---------------------------
# Simple password gate
# ---------------------------
APP_PASSWORD = st.secrets.get("APP_PASSWORD", "")

def check_password() -> bool:
    if not APP_PASSWORD:
        st.error("APP_PASSWORD is not set in secrets.")
        st.stop()

    if "auth_ok" not in st.session_state:
        st.session_state.auth_ok = False

    if st.session_state.auth_ok:
        return True

    st.markdown("### 🔐 Enter password")
    password = st.text_input("Password", type="password")
    if password:
        if password == APP_PASSWORD:
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

@st.cache_data(ttl=900)
def fetch_screener(exchange: str,
                   market_cap_more: int,
                   volume_more: int) -> pd.DataFrame:
    params = {
        "apikey": FMP_API_KEY,
        "country": "US",
        "exchange": exchange,
        "marketCapMoreThan": market_cap_more,
        "volumeMoreThan": volume_more,
        "isEtf": "false",
        "isFund": "false",
        "isActivelyTrading": "true",
        "limit": 1000
    }
    r = requests.get(SCREENER_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    df = pd.DataFrame(data)
    df["sourceExchange"] = exchange
    return df


# ---------------------------
# UI
# ---------------------------
st.set_page_config(page_title="US Equity Universe", layout="wide", page_icon="📈")

if check_password():
    st.title("US Equity Universe — FMP Screener 📊")

    with st.sidebar:
        st.header("Filters")
        exchanges = st.multiselect("Exchanges", ["NASDAQ", "NYSE"], default=["NASDAQ", "NYSE"])
        market_cap_more = st.number_input("Market Cap ≥", value=500_000_000, step=50_000_000)
        volume_more = st.number_input("Avg Daily Volume ≥", value=100_000, step=10_000)
        run_btn = st.button("Run Screener", type="primary", use_container_width=True)

    if run_btn:
        if not exchanges:
            st.warning("Pick at least one exchange.")
            st.stop()

        dfs = []
        for ex in exchanges:
            dfs.append(fetch_screener(ex, int(market_cap_more), int(volume_more)))

        df = pd.concat(dfs, ignore_index=True)
        df = df.drop_duplicates(subset=["symbol"]).reset_index(drop=True)

        st.metric("Total tickers", len(df))

        st.subheader("Results")
        st.dataframe(df, use_container_width=True, hide_index=True)

        # Download CSV
        csv = df.to_csv(index=False).encode()
        st.download_button("⬇️ Download CSV", data=csv,
                           file_name="us_universe_full.csv", mime="text/csv")
