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
  - Theta
  - Vega
  - Black-Scholes price

- Uses ^TNX (10Y Treasury) as risk-free rate

---

## Output

All commands print JSON to stdout.

Example:

```json
[
  {
    "symbol": "AAPL",
    "date": "2024-01-01T00:00:00",
    "close": 190.23
  }
]
```

Designed for piping into Python or other systems:

```bash
cargo run -- get-candles -s AAPL | python process.py
```

---

## Project Structure

```
src/
├── main.rs        # CLI entry point
├── candles.rs     # Candle download + caching + DB
├── options.rs     # Options chain + Greeks
├── greeks.rs      # Greeks calculations
├── utils.rs       # Helpers (timezone, JSON, parsing)
```

---

## Database

Uses DuckDB for local storage.

### Candles Table

- Stores OHLCV data
- Primary key: `(date, ticker)`

### Options Table

- Stores full option chain + Greeks
- Primary key: `(contract_symbol, collected_at)`

Database file:

```
rust_candles.db
```

---

## Key Design Choices

- **Polars** for fast DataFrame operations
- **DuckDB** for embedded analytics storage
- **Tokio + JoinSet** for concurrent downloads
- **Chunked downloads** to bypass Yahoo limits
- **Lazy evaluation** for efficient transformations

---

## Notes

- Intraday data does not support `Range::Max` (falls back to `5d`)
- Missing or stale symbols are automatically refreshed
- Timezones are converted after loading into Polars

---

## Future Improvements

- Multiple expiration support for options
- Streaming / real-time updates
- Backtesting integration
- More advanced filtering (IV rank, liquidity, etc.)

---

## License

MIT (or specify your license)
