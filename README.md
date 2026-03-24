# Yahoo Finance Rust CLI

A high-performance Rust CLI for downloading, storing, and analyzing market data and options chains using Yahoo Finance. Built with async concurrency, Polars, and DuckDB for fast data processing and storage.

---

## Features

- Download historical OHLCV candle data
- Automatic caching with DuckDB (avoids redundant downloads)
- Detects stale data and refreshes incrementally
- Fetch options chains (calls + puts)
- Compute option Greeks (delta, gamma, theta, vega, Black-Scholes price)
- Parallelized data fetching using Tokio
- Outputs results as JSON (easy Python integration)

---

## Installation

### Requirements

- Rust (latest stable recommended)

### Build

```bash
cargo build --release
```

### Run

```bash
cargo run -- <COMMAND>
```

---

## CLI Usage

### Get Candles

Download historical candle data:

```bash
cargo run -- get-candles -s AAPL MSFT -i 1d -r max --timezone PST
```

**Options:**

- `-s, --symbols` → One or more tickers
- `-i, --interval` → Candle interval (default: `1d`)
- `-r, --range` → Time range (default: `max`)
- `--timezone` → Output timezone (default: `PST`)

**Behavior:**

- Uses local DuckDB cache (`rust_candles.db`)
- Downloads only missing or stale data
- Automatically chunks large historical downloads

---

### Options Chain

Download options data and compute Greeks:

```bash
cargo run -- options -s AAPL TSLA --timezone PST
```

**Options:**

- `-s, --symbols` → One or more tickers
- `--timezone` → Output timezone (default: `PST`)

**Behavior:**

- Fetches nearest expiration
- Computes:

  - Delta
  - Gamma
  - Thet
