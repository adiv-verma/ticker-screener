import pandas as pd
import requests
import streamlit as st

# Read API key from Streamlit secrets
FMP_API_KEY = st.secrets.get("FMP_API_KEY", "")

if not FMP_API_KEY:
    st.error("⚠️ Please set FMP_API_KEY in your Streamlit secrets.")
    st.stop()

def fetch_screener(exchange: str) -> pd.DataFrame:
    url = "https://financialmodelingprep.com/stable/company-screener"
    params = {
        "apikey": FMP_API_KEY,
        "country": "US",
        "exchange": exchange,
        "marketCapMoreThan": 500_000_000,  # 500 million
        "volumeMoreThan": 100_000,         # 100k shares
        "isEtf": "false",
        "isFund": "false",
        "isActivelyTrading": "true",
        "limit": 1000
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    if not df.empty:
        df["sourceExchange"] = exchange
    return df

st.title("US Equity Universe — FMP Screener 📈")

if st.button("Fetch NASDAQ + NYSE Universe"):
    with st.spinner("Fetching data from FMP…"):
        nasdaq = fetch_screener("NASDAQ")
        nyse = fetch_screener("NYSE")

    df = pd.concat([nasdaq, nyse], ignore_index=True)
    df = df.drop_duplicates(subset=["symbol"]).reset_index(drop=True)

    st.success(f"✅ Total tickers after filter: {len(df)}")

    # Show first 10
    st.subheader("Preview (first 10 rows)")
    st.dataframe(df.head(10), use_container_width=True)

    # Download CSV
    csv = df.to_csv(index=False).encode()
    st.download_button(
        "⬇️ Download full CSV",
        data=csv,
        file_name="us_universe_full.csv",
        mime="text/csv",
    )
