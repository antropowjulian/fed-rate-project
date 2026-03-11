# scripts/collect_fred_data.py
# ─────────────────────────────────────────────────────────────
# PURPOSE: Downloads Federal Reserve data from FRED and saves
#          it to our MySQL database.
#
# What we download:
#   1. Federal Funds Rate (daily) — the key interest rate
#   2. FOMC meeting decisions — derived from rate changes
#   3. Macro indicators — CPI, unemployment, yield curve, etc.
# ─────────────────────────────────────────────────────────────

import os
import time
import pandas as pd
import mysql.connector
from fredapi import Fred
from dotenv import load_dotenv

# ── Load secrets from .env ─────────────────────────────────────
load_dotenv()
FRED_KEY   = os.getenv("FRED_API_KEY")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD")

# ── Initialize the FRED API client ────────────────────────────
fred = Fred(api_key=FRED_KEY)

# ── Connect to our MySQL database ─────────────────────────────
def get_connection():
    """Returns a fresh connection to the database."""
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password=MYSQL_PASS,
        database="fed_rate_project"
    )

# ── Helper: save a DataFrame to a MySQL table ─────────────────
def save_to_db(df, table_name, insert_sql, row_extractor):
    """
    Generic function to insert rows into a MySQL table.
    Skips rows that already exist (won't crash on duplicates).
    """
    conn    = get_connection()
    cursor  = conn.cursor()
    saved   = 0
    skipped = 0

    for _, row in df.iterrows():
        try:
            cursor.execute(insert_sql, row_extractor(row))
            saved += 1
        except mysql.connector.errors.IntegrityError:
            skipped += 1

    conn.commit()
    cursor.close()
    conn.close()
    print(f"   Saved: {saved} rows | Skipped (already exist): {skipped}")

# ════════════════════════════════════════════════════════════════
# STEP 1 — Download & Save the Federal Funds Rate (daily)
# ════════════════════════════════════════════════════════════════

print("\n📥 Downloading Federal Funds Rate (daily)...")
ffr = fred.get_series("DFF", observation_start="2000-01-01")

ffr_df = ffr.reset_index()
ffr_df.columns = ["date", "rate"]
ffr_df = ffr_df.dropna()
ffr_df["date"] = pd.to_datetime(ffr_df["date"]).dt.date

print(f"   Downloaded {len(ffr_df)} daily records.")
print(f"   Date range: {ffr_df['date'].min()} → {ffr_df['date'].max()}")

INSERT_FFR = """
    INSERT INTO federal_funds_rate (date, rate)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE rate = VALUES(rate)
"""
save_to_db(
    ffr_df, "federal_funds_rate", INSERT_FFR,
    lambda row: (row["date"], float(row["rate"]))
)
print("✅ Federal Funds Rate saved.")

# ════════════════════════════════════════════════════════════════
# STEP 2 — Identify FOMC Meeting Dates
# ════════════════════════════════════════════════════════════════

print("\n📅 Identifying FOMC meeting dates from rate changes...")

ffr_monthly    = fred.get_series("FEDFUNDS", observation_start="2000-01-01")
ffr_monthly_df = ffr_monthly.reset_index()
ffr_monthly_df.columns = ["date", "rate"]
ffr_monthly_df = ffr_monthly_df.dropna()
ffr_monthly_df["date"] = pd.to_datetime(ffr_monthly_df["date"]).dt.date

ffr_monthly_df["prev_rate"]   = ffr_monthly_df["rate"].shift(1)
ffr_monthly_df["rate_change"] = ffr_monthly_df["rate"] - ffr_monthly_df["prev_rate"]

def classify_decision(change):
    if change > 0:   return "HIKE"
    elif change < 0: return "CUT"
    else:            return "HOLD"

ffr_monthly_df["decision"] = ffr_monthly_df["rate_change"].apply(classify_decision)
fomc_df = ffr_monthly_df.dropna(subset=["prev_rate"]).copy()

hikes = (fomc_df["decision"] == "HIKE").sum()
cuts  = (fomc_df["decision"] == "CUT").sum()
holds = (fomc_df["decision"] == "HOLD").sum()
print(f"   Found {len(fomc_df)} FOMC periods | HIKE: {hikes} | CUT: {cuts} | HOLD: {holds}")

INSERT_FOMC = """
    INSERT INTO fomc_meetings
        (meeting_date, rate_before, rate_after, rate_change, decision)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        rate_before = VALUES(rate_before),
        rate_after  = VALUES(rate_after),
        rate_change = VALUES(rate_change),
        decision    = VALUES(decision)
"""
save_to_db(
    fomc_df, "fomc_meetings", INSERT_FOMC,
    lambda row: (
        row["date"],
        float(row["prev_rate"]),
        float(row["rate"]),
        float(row["rate_change"]),
        row["decision"]
    )
)
print("✅ FOMC meetings saved.")

# ════════════════════════════════════════════════════════════════
# STEP 3 — Download Macro Indicators
# ════════════════════════════════════════════════════════════════

MACRO_SERIES = {
    "CPIAUCSL" : "Consumer Price Index (CPI) - Inflation",
    "UNRATE"   : "Unemployment Rate",
    "T10Y2Y"   : "10-Year minus 2-Year Treasury Yield Spread",
    "M2SL"     : "M2 Money Supply",
    "DPCREDIT" : "Discount Window Primary Credit Rate",
}

INSERT_MACRO = """
    INSERT INTO macro_indicators
        (indicator_code, indicator_name, date, value)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE value = VALUES(value)
"""

for series_id, series_name in MACRO_SERIES.items():
    print(f"\n📥 Downloading {series_name} ({series_id})...")
    try:
        series = fred.get_series(series_id, observation_start="2000-01-01")
        df = series.reset_index()
        df.columns = ["date", "value"]
        df = df.dropna()
        df["date"] = pd.to_datetime(df["date"]).dt.date
        print(f"   Downloaded {len(df)} records.")
        save_to_db(
            df, "macro_indicators", INSERT_MACRO,
            lambda row, sid=series_id, sname=series_name: (
                sid, sname, row["date"], float(row["value"])
            )
        )
        print(f"✅ {series_id} saved.")
        time.sleep(0.5)
    except Exception as e:
        print(f"⚠️  Could not download {series_id}: {e}")

print("\n🎉 FRED data collection complete!")