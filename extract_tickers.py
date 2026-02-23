#!/usr/bin/env python3
import json
import re

# Read the pagination.json file
with open('pagination_new.json', 'r') as f:
    data = json.load(f)

# Extract tickers from market titles
tickers = set()

def extract_ticker_from_title(title):
    """Extract ticker from titles like 'Apple (AAPL) Up or Down'"""
    match = re.search(r'\(([A-Z]{2,5})\)', title)
    if match:
        return match.group(1)
    return None

# Process all markets
for item in data.get('data', []):
    title = item.get('title', '')
    ticker = extract_ticker_from_title(title)
    if ticker:
        tickers.add(ticker)

    # Also check in markets array
    for market in item.get('markets', []):
        question = market.get('question', '')
        ticker = extract_ticker_from_title(question)
        if ticker:
            tickers.add(ticker)

# Sort and print
sorted_tickers = sorted(tickers)
print(f"Found {len(sorted_tickers)} unique tickers:")
print()
print("# Tickers for finance_tickers set:")
for ticker in sorted_tickers:
    print(f"    '{ticker}',")
