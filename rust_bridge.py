import os
import subprocess

# import pandas as pd
import json

YAHOO_BINARIES = "target/release/yahoo-finance"
YAHOO_DEBUG_BINARIES = "target/debug/yahoo-finance"


def get_candles(tickers: list, interval: str = "1d", period: str = "max"):
    if isinstance(tickers, str):
        tickers = [tickers]

    command = [
        YAHOO_BINARIES,
        "get-candles",
        "--symbols",
        *tickers,  # unpack list directly, no need to join
        "--interval",
        interval,
        "--range",
        period,
    ]
    print(f"🚀 Python is getting candles from Rust...")

    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(f"Rust binary failed: {result.stderr}")

    # df = pd.read_json(result.stdout)
    return result.stdout


def get_options(tickers: list):
    if isinstance(tickers, str):
        tickers = [tickers]

    command = [
        YAHOO_BINARIES,
        "options",
        "--symbols",
        *tickers,
    ]
    print(f"🚀 Python is getting options from Rust...")

    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(f"Rust binary failed: {result.stderr}")

    # df = pd.read_json(result.stdout)
    return result.stdout


if __name__ == "__main__":
    tickers = ["AAPL", "MSFT", "NVDA"]

    candles_df = get_candles(tickers)
    print(candles_df)

    options_df = get_options(tickers)
    print(options_df)
