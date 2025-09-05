import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="FMP Screener (Click-to-Fetch)", layout="wide")

FMP_API_KEY = st.secrets.get("FMP_API_KEY", "")
if not FMP_API_KEY:
    st.error("Please set FMP_API_KEY in .streamlit/secrets.toml")
    st.stop()

SCREENER_URL = "https://financialmodelingprep.com/stable/company-screener"

def fetch_screener(exchange: str, market_cap_more: int, volume_more: int, limit: int) -> pd.DataFrame:
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
    r = requests.get(SCREENER_URL, params=params, timeout=30)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    if not df.empty:
        df["sourceExchange"] = exchange
    return df

# ---------------- UI ----------------
st.title("US Equity Universe — Click to Fetch")
st.caption("No prefetch. Results load only after you press the button.")

with st.sidebar:
    st.header("Filters")
    market_cap_more = st.number_input("Market Cap ≥", value=500_000_000, step=50_000_000, min_value=0)
    volume_more     = st.number_input("Avg Daily Volume ≥", value=100_000, step=10_000, min_value=0)
    limit           = st.number_input("Per-exchange limit", value=200, min_value=10, max_value=1000, step=50)

    colA, colB = st.columns(2)
    run   = colA.button("Fetch", type="primary", use_container_width=True)
    reset = colB.button("Reset", use_container_width=True)

# Use session_state so results persist across reruns
if "results" not in st.session_state:
    st.session_state.results = None

if reset:
    st.session_state.results = None
    st.experimental_rerun()

if run:
    with st.spinner("Fetching from FMP…"):
        try:
            nasdaq = fetch_screener("NASDAQ", market_cap_more, volume_more, limit)
            nyse   = fetch_screener("NYSE",   market_cap_more, volume_more, limit)
            df = pd.concat([nasdaq, nyse], ignore_index=True)
            if "symbol" in df.columns:
                df = df.drop_duplicates(subset=["symbol"]).reset_index(drop=True)
            st.session_state.results = df
        except requests.HTTPError as e:
            st.error(f"HTTP error: {e.response.status_code} {e.response.text[:200]}")
        except Exception as e:
            st.error(f"Error: {e}")

# Render results only if present
df = st.session_state.results
if df is None:
    st.info("Click **Fetch** to retrieve results.")
else:
    st.success(f"✅ Total tickers: {len(df)}")
    st.subheader("Preview (first 20 rows)")
    st.dataframe(df.head(20), use_container_width=True, hide_index=True)

    csv = df.to_csv(index=False).encode()
    st.download_button("⬇️ Download full CSV", data=csv, file_name="us_universe_full.csv", mime="text/csv")

    # Optional: show full table on demand
    if st.checkbox("Show entire table (may be slow)"):
        st.dataframe(df, use_container_width=True, hide_index=True)
