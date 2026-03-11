# database/setup_database.py
# ─────────────────────────────────────────────────────────────
# PURPOSE: Creates our MySQL database and all the tables we
#          need to store Fed rate data and stock prices.
#
# Think of this like setting up labeled folders before you
# start filing documents. Run this ONCE at the start.
# ─────────────────────────────────────────────────────────────

import mysql.connector
from dotenv import load_dotenv
import os

# Load the keys and passwords from our .env file
load_dotenv()

# ── Connect to MySQL ──────────────────────────────────────────
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password=os.getenv("MYSQL_PASSWORD")
)
cursor = connection.cursor()

# ── Create the database ───────────────────────────────────────
# A "database" is like a folder that holds all our tables.
cursor.execute("CREATE DATABASE IF NOT EXISTS fed_rate_project;")
cursor.execute("USE fed_rate_project;")
print("✅ Database 'fed_rate_project' ready.")

# ── Table 1: Federal Funds Rate ───────────────────────────────
# Stores the Fed's target interest rate for every date.
cursor.execute("""
    CREATE TABLE IF NOT EXISTS federal_funds_rate (
        id         INT AUTO_INCREMENT PRIMARY KEY,
        date       DATE NOT NULL UNIQUE,
        rate       FLOAT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""")
print("✅ Table 'federal_funds_rate' ready.")

# ── Table 2: FOMC Meeting Dates ───────────────────────────────
# Stores each Fed meeting date and what rate decision was made.
cursor.execute("""
    CREATE TABLE IF NOT EXISTS fomc_meetings (
        id           INT AUTO_INCREMENT PRIMARY KEY,
        meeting_date DATE NOT NULL UNIQUE,
        rate_before  FLOAT,
        rate_after   FLOAT,
        rate_change  FLOAT,
        decision     VARCHAR(10),   -- 'HIKE', 'CUT', or 'HOLD'
        created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""")
print("✅ Table 'fomc_meetings' ready.")

# ── Table 3: Stock Prices ─────────────────────────────────────
# Stores daily stock price data for each company.
# One row = one company on one day.
cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_prices (
        id          INT AUTO_INCREMENT PRIMARY KEY,
        ticker      VARCHAR(10) NOT NULL,
        date        DATE NOT NULL,
        open_price  FLOAT,
        high_price  FLOAT,
        low_price   FLOAT,
        close_price FLOAT NOT NULL,
        volume      BIGINT,
        created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY unique_ticker_date (ticker, date)
    );
""")
print("✅ Table 'stock_prices' ready.")

# ── Table 4: Macro Indicators ─────────────────────────────────
# Stores other economic data (inflation, unemployment, etc.)
# that we'll use as features in our ML model later.
cursor.execute("""
    CREATE TABLE IF NOT EXISTS macro_indicators (
        id             INT AUTO_INCREMENT PRIMARY KEY,
        indicator_code VARCHAR(20) NOT NULL,
        indicator_name VARCHAR(100),
        date           DATE NOT NULL,
        value          FLOAT NOT NULL,
        created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY unique_indicator_date (indicator_code, date)
    );
""")
print("✅ Table 'macro_indicators' ready.")

# ── Clean up ──────────────────────────────────────────────────
connection.commit()
cursor.close()
connection.close()

print("\n🎉 Database setup complete!")
print("   Database : fed_rate_project")
print("   Tables   : federal_funds_rate, fomc_meetings,")
print("              stock_prices, macro_indicators")