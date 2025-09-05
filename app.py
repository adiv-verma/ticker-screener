import os
import requests
import pandas as pd
import streamlit as st

# ---------- Config ----------
st.set_page_config(page_title="FMP Screener (Minimal)", layout="wide")

# Secrets
FMP_API_KEY = st.secrets.get("FMP_API_KEY", "")
APP_PASSWORD = st.secrets.get("APP_PASSWORD", "")

if not FMP_API_KEY:
    st.error("Missing FMP_API_KEY in secrets.")
    st.stop()
if not APP_PASSWORD:
    st.error("Missing APP_PASSWORD in secrets.")
    st.stop()

# ---------- Auth ----------
if "auth_ok" not in st.session_state:
    st.session_state.auth_ok = False

if not st.session_state.auth_ok:
    st.title("🔐 FMP Screener (Minimal)")
    pw = st.text_input("Password", type="password")
    if pw and pw == APP_PASSWORD:
        st.session_state.auth_ok = True
    else:
        st.stop()

# ---------- UI ----------
st.title("US Equity Universe — Minimal Screener")
st.caption("Country=US • Exclude ETFs/Funds • Actively trading • Merge NASDAQ + NYSE")

with st.sidebar:
    st.header("Filters")
    market_cap_more = st.number_input("Market Cap ≥", value=500_000_000, step=50_000_000, min_value=0)
    volume_more = st.number_input("Avg Daily Volume ≥", value=100_000, step=10_000, min_value=0)
    limit = st.number_input("Per-exchange limit", value=400, min_value=10, max_value=1000, step=50)
    run = st.button("Run", type="primary", use_container_width=True)

SCREENER_URL = "https://financialmodelingprep.com/stable/company-screener"

def fetch(exchange: str) -> pd.DataFrame:
    params = {
        "apikey": FMP_API_KEY,
        "country": "US",
        "exchange": exchange,
        "marketCapMoreThan": int(market_cap_more),
        "volumeMoreThan": int(volume_more),
        "isEtf": "false",
        "isFund": "false",
        "isActivelyTrading": "true",
        "limit": int(limit),
    }
    r = requests.get(SCREENER_URL, params=params, timeout=25)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    if not df.empty:
        df["sourceExchange"] = exchange
    return df

if run:
    with st.spinner("Fetching…"):
        try:
            nasdaq = fetch("NASDAQ")
            nyse = fetch("NYSE")
        except requests.HTTPError as e:
            st.error(f"HTTP error: {e.response.status_code} {e.response.text[:200]}")
            st.stop()
        except Exception as e:
            st.error(f"Error: {e}")
            st.stop()

    df = pd.concat([nasdaq, nyse], ignore_index=True)
    if "symbol" in df.columns:
        df = df.drop_duplicates(subset=["symbol"]).reset_index(drop=True)

    st.write(f"**Total tickers:** {len(df)}  •  Min MCAP: ${int(market_cap_more):,}  •  Min Vol: {int(volume_more):,}")

    # Show small preview to keep UI snappy
    st.subheader("Preview (first 50 rows)")
    st.dataframe(df.head(50), use_container_width=True, hide_index=True)

    # Full CSV download
    csv = df.to_csv(index=False).encode()
    st.download_button("⬇️ Download full CSV", data=csv, file_name="us_universe_full.csv", mime="text/csv")

    # (Optional) toggle to view whole table (can be slow with many rows)
    if st.checkbox("Show entire table (may be slow)"):
        st.dataframe(df, use_container_width=True, hide_index=True)
