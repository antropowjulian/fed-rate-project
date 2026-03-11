# scripts/collect_stock_data.py
# ─────────────────────────────────────────────────────────────
# PURPOSE: Downloads daily stock prices from Yahoo Finance
#          for major bank stocks and the S&P 500 (SPY).
#
# Uses yfinance — completely free, no API key needed,
# no daily call limits.
# ─────────────────────────────────────────────────────────────

import os
import yfinance as yf
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

# ── Load secrets ───────────────────────────────────────────────
load_dotenv()
MYSQL_PASS = os.getenv("MYSQL_PASSWORD")

# ── Tickers to download ────────────────────────────────────────
# SPY = S&P 500 ETF. This is our benchmark — we compare every
# bank stock's performance against SPY after each Fed meeting.
TICKERS = {
    "SPY" : "S&P 500 ETF (Benchmark)",
    "JPM" : "JPMorgan Chase",
    "GS"  : "Goldman Sachs",
    "BAC" : "Bank of America",
    "WFC" : "Wells Fargo",
    "C"   : "Citigroup",
    "MS"  : "Morgan Stanley",
    "USB" : "U.S. Bancorp",
    "PNC" : "PNC Financial Services",
    "TFC" : "Truist Financial",
    "COF" : "Capital One",
    "BK"  : "Bank of New York Mellon",
    "STT" : "State Street",
    "SCHW": "Charles Schwab",
    "AXP" : "American Express",
}

# ── Connect to database ────────────────────────────────────────
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password=MYSQL_PASS,
        database="fed_rate_project"
    )

# ── Download one stock's full price history ────────────────────
def download_stock(ticker):
    """
    Uses yfinance to download the full daily price history
    for a given ticker. Returns a clean DataFrame or None.
    """
    try:
        # Download all available history from 2000 onwards
        stock = yf.Ticker(ticker)
        df = stock.history(start="2000-01-01", auto_adjust=True)

        if df.empty:
            print(f"   ⚠️  No data returned for {ticker}")
            return None

        # Reset index so date becomes a regular column
        df = df.reset_index()

        # Keep only the columns we need
        df = df[["Date", "Open", "High", "Low", "Close", "Volume"]]

        # Rename columns to match our database
        df.columns = ["date", "open_price", "high_price",
                      "low_price", "close_price", "volume"]

        # Clean up the date format
        df["date"]   = pd.to_datetime(df["date"]).dt.date
        df["ticker"] = ticker

        # Remove any rows with missing closing price
        df = df.dropna(subset=["close_price"])
        df = df.sort_values("date")

        return df

    except Exception as e:
        print(f"   ⚠️  Error downloading {ticker}: {e}")
        return None

# ── Save stock data to MySQL ───────────────────────────────────
def save_stock_data(df):
    conn   = get_connection()
    cursor = conn.cursor()
    saved  = 0

    INSERT_SQL = """
        INSERT INTO stock_prices
            (ticker, date, open_price, high_price,
             low_price, close_price, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open_price  = VALUES(open_price),
            high_price  = VALUES(high_price),
            low_price   = VALUES(low_price),
            close_price = VALUES(close_price),
            volume      = VALUES(volume)
    """

    for _, row in df.iterrows():
        try:
            cursor.execute(INSERT_SQL, (
                row["ticker"],
                row["date"],
                float(row["open_price"]),
                float(row["high_price"]),
                float(row["low_price"]),
                float(row["close_price"]),
                int(row["volume"])
            ))
            saved += 1
        except Exception:
            pass

    conn.commit()
    cursor.close()
    conn.close()
    return saved

# ── Main loop ─────────────────────────────────────────────────
print("🚀 Starting stock data collection (via Yahoo Finance)...")
print(f"   Tickers to download : {len(TICKERS)}")
print(f"   No rate limits — this should take under 2 minutes\n")

summary = []

for i, (ticker, name) in enumerate(TICKERS.items(), start=1):
    print(f"[{i}/{len(TICKERS)}] {name} ({ticker})...")

    df = download_stock(ticker)

    if df is not None:
        saved = save_stock_data(df)
        print(f"   ✅ {len(df)} records downloaded, {saved} saved to DB")
        print(f"   Date range: {df['date'].min()} → {df['date'].max()}")
        summary.append({
            "Ticker"  : ticker,
            "Status"  : "✅ Success",
            "Records" : len(df)
        })
    else:
        print(f"   ❌ Failed to download {ticker}")
        summary.append({
            "Ticker"  : ticker,
            "Status"  : "❌ Failed",
            "Records" : 0
        })

# ── Print summary ──────────────────────────────────────────────
print("\n" + "="*50)
print("📊 DOWNLOAD SUMMARY")
print("="*50)
print(pd.DataFrame(summary).to_string(index=False))
total = sum(r["Records"] for r in summary)
print(f"\nTotal records saved to database: {total:,}")
print("\n🎉 Stock data collection complete!")