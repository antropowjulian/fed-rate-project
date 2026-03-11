# scripts/clean_and_merge.py
# ─────────────────────────────────────────────────────────────
# PURPOSE: Takes raw data from MySQL and creates a clean,
#          analysis-ready dataset for machine learning.
#
# Steps:
#   1. Load FOMC meetings and stock prices from MySQL
#   2. For each FOMC meeting, find prices on the meeting date
#      and 30 days later for every bank stock and SPY
#   3. Calculate 30-day returns
#   4. Calculate relative return vs SPY (target variable)
#   5. Save clean dataset to data/processed/
# ─────────────────────────────────────────────────────────────

import os
import pandas as pd
import mysql.connector
import numpy as np
from dotenv import load_dotenv

load_dotenv()
MYSQL_PASS = os.getenv("MYSQL_PASSWORD")

def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password=MYSQL_PASS,
        database="fed_rate_project"
    )

# ════════════════════════════════════════════════════════════════
# STEP 1 — Load data from MySQL
# ════════════════════════════════════════════════════════════════

print("📂 Loading data from database...")
conn = get_connection()

fomc_df = pd.read_sql("""
    SELECT meeting_date, rate_before, rate_after, rate_change, decision
    FROM fomc_meetings
    ORDER BY meeting_date
""", conn)

stocks_df = pd.read_sql("""
    SELECT ticker, date, close_price
    FROM stock_prices
    ORDER BY ticker, date
""", conn)

macro_df = pd.read_sql("""
    SELECT indicator_code, date, value
    FROM macro_indicators
    ORDER BY indicator_code, date
""", conn)

conn.close()

fomc_df["meeting_date"] = pd.to_datetime(fomc_df["meeting_date"])
stocks_df["date"]       = pd.to_datetime(stocks_df["date"])
macro_df["date"]        = pd.to_datetime(macro_df["date"])

print(f"   FOMC meetings  : {len(fomc_df)}")
print(f"   Stock rows     : {len(stocks_df):,}")
print(f"   Unique tickers : {stocks_df['ticker'].nunique()}")
print(f"   Macro rows     : {len(macro_df):,}")

# ════════════════════════════════════════════════════════════════
# STEP 2 — Separate SPY from bank stocks
# ════════════════════════════════════════════════════════════════

spy_df   = stocks_df[stocks_df["ticker"] == "SPY"].copy()
banks_df = stocks_df[stocks_df["ticker"] != "SPY"].copy()
tickers  = banks_df["ticker"].unique().tolist()

print(f"\n   Bank tickers : {tickers}")
print(f"   Benchmark    : SPY")

# ════════════════════════════════════════════════════════════════
# STEP 3 — Helper: get nearest trading day price
# ════════════════════════════════════════════════════════════════
# Markets are closed on weekends and holidays. This function
# searches forward up to 5 days to find the next available price.

def get_nearest_price(price_df, target_date, max_offset=5):
    for offset in range(max_offset + 1):
        check = target_date + pd.Timedelta(days=offset)
        match = price_df[price_df["date"] == check]
        if not match.empty:
            return float(match.iloc[0]["close_price"]), check
    return None, None

# ════════════════════════════════════════════════════════════════
# STEP 4 — Pivot macro indicators into wide format
# ════════════════════════════════════════════════════════════════
# Converts from one row per (indicator, date)
# to one row per date with each indicator as its own column.

macro_wide = macro_df.pivot_table(
    index="date", columns="indicator_code", values="value"
).reset_index()
macro_wide.columns.name = None
macro_wide = macro_wide.sort_values("date").ffill()

print(f"\n   Macro columns: {[c for c in macro_wide.columns if c != 'date']}")

# ════════════════════════════════════════════════════════════════
# STEP 5 — Calculate 30-day returns for every meeting × ticker
# ════════════════════════════════════════════════════════════════

print("\n⚙️  Calculating 30-day post-FOMC returns...")
print("   This may take a minute...\n")

records = []

for _, fomc_row in fomc_df.iterrows():
    meeting_date = fomc_row["meeting_date"]
    end_date     = meeting_date + pd.Timedelta(days=30)

    spy_start, _ = get_nearest_price(spy_df, meeting_date)
    spy_end,   _ = get_nearest_price(spy_df, end_date)

    if spy_start is None or spy_end is None:
        continue

    spy_return = (spy_end - spy_start) / spy_start * 100

    macro_at_meeting = macro_wide[macro_wide["date"] <= meeting_date]
    macro_vals = {}
    if not macro_at_meeting.empty:
        latest = macro_at_meeting.iloc[-1]
        for col in macro_wide.columns:
            if col != "date":
                macro_vals[col] = latest[col] if col in latest else None

    for ticker in tickers:
        t_df = banks_df[banks_df["ticker"] == ticker][["date", "close_price"]].copy()

        stock_start, _ = get_nearest_price(t_df, meeting_date)
        stock_end,   _ = get_nearest_price(t_df, end_date)

        if stock_start is None or stock_end is None:
            continue

        stock_return    = (stock_end - stock_start) / stock_start * 100
        relative_return = stock_return - spy_return

        # ── TARGET VARIABLE ──────────────────────────────────
        # 1 = bank stock beat the S&P 500 in 30 days after meeting
        # 0 = it did not
        outperformed = 1 if relative_return > 0 else 0

        record = {
            "meeting_date"        : meeting_date,
            "ticker"              : ticker,
            "rate_before"         : fomc_row["rate_before"],
            "rate_after"          : fomc_row["rate_after"],
            "rate_change"         : fomc_row["rate_change"],
            "decision"            : fomc_row["decision"],
            "stock_price_start"   : round(stock_start, 4),
            "stock_price_end"     : round(stock_end, 4),
            "spy_price_start"     : round(spy_start, 4),
            "spy_price_end"       : round(spy_end, 4),
            "stock_return_30d"    : round(stock_return, 4),
            "spy_return_30d"      : round(spy_return, 4),
            "relative_return_30d" : round(relative_return, 4),
            "outperformed_spy"    : outperformed,
        }
        record.update(macro_vals)
        records.append(record)

results_df = pd.DataFrame(records)

print(f"✅ Calculations complete!")
print(f"   Rows    : {len(results_df):,}")
print(f"   Columns : {results_df.shape[1]}")

# ════════════════════════════════════════════════════════════════
# STEP 6 — Handle missing values
# ════════════════════════════════════════════════════════════════

print("\n🔍 Checking for missing values...")
missing = results_df.isnull().sum()
missing = missing[missing > 0]
if missing.empty:
    print("   No missing values found! ✅")
else:
    print(f"   Filling missing values with column medians...")
    for col in missing.index:
        results_df[col] = results_df[col].fillna(results_df[col].median())
    print("   Done. ✅")

# ════════════════════════════════════════════════════════════════
# STEP 7 — Data quality report
# ════════════════════════════════════════════════════════════════

print("\n📊 Data Quality Report:")
print(f"   Total rows           : {len(results_df):,}")
print(f"   Date range           : {results_df['meeting_date'].min().date()} → {results_df['meeting_date'].max().date()}")
print(f"   Unique tickers       : {results_df['ticker'].nunique()}")
print(f"   Unique FOMC meetings : {results_df['meeting_date'].nunique()}")

n_out = results_df["outperformed_spy"].sum()
n_tot = len(results_df)
print(f"\n   Target variable:")
print(f"   Outperformed (1) : {n_out:,} ({n_out/n_tot*100:.1f}%)")
print(f"   Did not beat (0) : {n_tot-n_out:,} ({(n_tot-n_out)/n_tot*100:.1f}%)")

print(f"\n   Results by Fed decision:")
summary = results_df.groupby("decision").agg(
    observations=("outperformed_spy", "count"),
    avg_stock_return=("stock_return_30d", "mean"),
    avg_spy_return=("spy_return_30d", "mean"),
    pct_outperformed=("outperformed_spy", "mean")
).round(2)
print(summary.to_string())

# ════════════════════════════════════════════════════════════════
# STEP 8 — Save to CSV
# ════════════════════════════════════════════════════════════════

os.makedirs("data/processed", exist_ok=True)
output_path = "data/processed/fomc_stock_analysis.csv"
results_df.to_csv(output_path, index=False)

print(f"\n💾 Saved to : {output_path}")
print(f"   Shape    : {results_df.shape[0]} rows × {results_df.shape[1]} columns")
print(f"\n🎉 Week 3 complete! Your ML-ready dataset is saved.")
print(   "   Next up  : Week 4 — Feature Engineering")