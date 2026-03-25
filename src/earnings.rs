// src/earnings.rs
use duckdb::{Connection, Result as DbResult, params};
use serde::{Deserialize, Serialize};
use std::error::Error;
use yfinance_rs::{Ticker, YfClient};

/// Represents an earnings estimate.
#[derive(Debug, Serialize, Deserialize)]
pub struct EarningsEstimate {
    pub date: String,
    pub estimate: f64,
    pub actual: Option<f64>,
}

/// Downloads earnings estimates for a given ticker.
///
/// # Arguments
///
/// * `ticker` - A string slice representing the ticker symbol (e.g., "AAPL").
///
/// # Returns
///
/// A `Result` containing a vector of `EarningsEstimate` on success, or an error on failure.
pub async fn download_earnings_estimates(
    ticker: &str,
    client: YfClient,
) -> Result<Vec<EarningsEstimate>, Box<dyn Error>> {
    // Create a Ticker instance for the given symbol.
    let ticker_obj = Ticker::new(&client, ticker);

    // Fetch earnings estimates. According to yfinance-rs, the `earnings()` method returns a Vec<Earnings>.
    let estimates = ticker_obj.earnings(None).await?;

    // Convert the internal Earnings structs to our own EarningsEstimate.
    let mut result = Vec::new();
    for e in estimates.quarterly_eps {
        result.push(EarningsEstimate {
            date: e.period.to_string(),
            estimate: e
                .estimate
                .as_ref()
                .map(yfinance_rs::core::conversions::money_to_f64)
                .unwrap_or(0.0),
            actual: e
                .actual
                .as_ref()
                .map(yfinance_rs::core::conversions::money_to_f64),
        });
    }

    Ok(result)
}

// ... existing code (if any) ...
pub fn insert_earnings_estimates(
    conn: &Connection,
    ticker: &str,
    estimates: &[EarningsEstimate],
) -> DbResult<()> {
    let mut stmt = conn.prepare(
        "INSERT OR IGNORE INTO earnings_estimates (ticker, date, estimate, actual)
         VALUES (?1, ?2, ?3, ?4)",
    )?;

    for est in estimates {
        stmt.execute(params![ticker, est.date, est.estimate, est.actual])?;
    }

    Ok(())
}

pub fn read_earnings_estimates(conn: &Connection, ticker: &str) -> DbResult<Vec<EarningsEstimate>> {
    let mut stmt = conn.prepare(
        "SELECT date, estimate, actual FROM earnings_estimates WHERE ticker = ?1 ORDER BY date",
    )?;

    let rows = stmt
        .query_map(params![ticker], |row| {
            Ok(EarningsEstimate {
                date: row.get(0)?,
                estimate: row.get(1)?,
                actual: row.get(2)?,
            })
        })?
        .collect::<DbResult<Vec<_>>>()?;

    Ok(rows)
}

pub async fn get_earnings_estimates(
    conn: &Connection,
    ticker: &str,
    client: YfClient,
) -> Result<Vec<EarningsEstimate>, Box<dyn Error>> {
    // Check if we have data for this ticker
    let mut existing = read_earnings_estimates(conn, ticker)?;

    // If it's empty, we need to download it
    if existing.is_empty() {
        println!("{} earnings not found in DB, will download.", ticker);
        existing = download_earnings_estimates(ticker, client).await?;
        insert_earnings_estimates(conn, ticker, &existing)?;
    } else {
        println!("{} earnings found in DB.", ticker);
    }

    Ok(existing)
}
