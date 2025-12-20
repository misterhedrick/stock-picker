import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# Ticker
ticker = "SPY"
spy = yf.Ticker(ticker)

# Get available option expiration dates
expirations = spy.options
if not expirations:
    raise RuntimeError("No option expirations found")

# Target date: ~2 days from now
target_date = datetime.now().date() + timedelta(days=2)

# Find the closest expiration on or after target_date
expiration_date = None
for exp in expirations:
    exp_date = datetime.strptime(exp, "%Y-%m-%d").date()
    if exp_date >= target_date:
        expiration_date = exp
        break

if expiration_date is None:
    raise RuntimeError("No suitable expiration found")

print(f"Using expiration date: {expiration_date}")

# Fetch option chain
option_chain = spy.option_chain(expiration_date)

# Calls only
calls = option_chain.calls.copy()

# Select useful columns
columns = [
    "contractSymbol",
    "strike",
    "lastPrice",
    "bid",
    "ask",
    "volume",
    "openInterest",
    "impliedVolatility",
    "inTheMoney"
]

calls = calls[columns]

# Sort by strike
calls.sort_values("strike", inplace=True)

# Display
pd.set_option("display.max_rows", None)
print(calls)
