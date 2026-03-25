use chrono::{DateTime, Duration, Utc};
use duckdb::{Connection, params};
use polars::prelude::*;
use yfinance_rs::core::client::{Backoff, RetryConfig};
use yfinance_rs::core::conversions::money_to_f64;
use yfinance_rs::{Ticker, YfClient, YfClientBuilder};

/// Fetches financial statements for the given symbols, using the DB cache when fresh
/// and downloading via yfinance-rs when stale or missing.
pub async fn get_statements(
    conn: &Connection,
    symbols: Vec<String>,
    stale_threshold: Duration,
    client: &YfClient,
    statement_type: &str,
    annual: bool,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let now = Utc::now();
    let period = if annual { "A" } else { "Q" };

    let placeholders = symbols
        .iter()
        .enumerate()
        .map(|(i, _)| format!("?{}", i + 1))
        .collect::<Vec<_>>()
        .join(", ");

    let stmt_type_param = format!("?{}", symbols.len() + 1);
    let period_param = format!("?{}", symbols.len() + 2);

    let sql = format!(
        "SELECT ticker, strftime(MAX(date)::TIMESTAMP, '%Y-%m-%dT%H:%M:%S+00:00') as latest
         FROM statements
         WHERE ticker IN ({}) AND statement_type = {} AND period = {}
         GROUP BY ticker",
        placeholders, stmt_type_param, period_param
    );

    let mut stmt = conn.prepare(&sql)?;

    let db_params: Vec<Box<dyn duckdb::ToSql>> = symbols
        .iter()
        .map(|t| Box::new(t.clone()) as Box<dyn duckdb::ToSql>)
        .chain(std::iter::once(
            Box::new(statement_type.to_string()) as Box<dyn duckdb::ToSql>
        ))
        .chain(std::iter::once(
            Box::new(period.to_string()) as Box<dyn duckdb::ToSql>
        ))
        .collect();

    let rows = stmt
        .query_map(duckdb::params_from_iter(db_params.iter()), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<duckdb::Result<Vec<_>>>()?;

    let mut found: std::collections::HashMap<String, DateTime<Utc>> =
        std::collections::HashMap::new();
    for (ticker, date_str) in rows {
        if let Ok(dt) = DateTime::parse_from_rfc3339(&date_str) {
            found.insert(ticker, dt.with_timezone(&Utc));
        }
    }

    let mut to_download: Vec<String> = Vec::new();

    for symbol in &symbols {
        match found.get(symbol) {
            None => {
                println!("{} not found in DB, will download.", symbol);
                to_download.push(symbol.clone());
            }
            Some(latest) => {
                let age = now - *latest;
                if age > stale_threshold {
                    println!(
                        "{} is stale (last updated: {}), will download.",
                        symbol, latest
                    );
                    to_download.push(symbol.clone());
                } else {
                    println!("{} is up to date (last updated: {}).", symbol, latest);
                }
            }
        }
    }

    if !to_download.is_empty() {
        let new_data_df = download_statements(client, to_download, statement_type, annual).await?;
        insert_statements(conn, &new_data_df)?;
    }

    let df = read_statements(conn, symbols, statement_type, period)?;

    Ok(df)
}

/// Helper: extract f64 from Option<Money>, returning None if absent.
fn money_opt_to_f64(m: &Option<paft::money::Money>) -> Option<f64> {
    m.as_ref().map(|v| money_to_f64(v))
}

/// Maximum number of retry attempts per symbol fetch.
const MAX_RETRIES: u32 = 3;
/// Initial backoff duration between retries (doubles each attempt).
const INITIAL_BACKOFF_MS: u64 = 2000;

/// Creates a YfClient with built-in retry policy for transient failures.
///
/// This handles retries at the HTTP level (e.g. 429 rate-limit, 5xx errors).
/// For higher-level JSON parsing failures (empty responses from Yahoo), we
/// additionally retry at the function level in `download_statements`.
pub fn create_client_with_retries() -> YfClient {
    YfClientBuilder::default()
        .retry_config(RetryConfig {
            enabled: true,
            max_retries: 3,
            backoff: Backoff::Exponential {
                base: std::time::Duration::from_millis(1000),
                factor: 2.0,
                max: std::time::Duration::from_secs(10),
                jitter: true,
            },
            retry_on_status: vec![429, 500, 502, 503, 504],
            retry_on_timeout: true,
            retry_on_connect: true,
        })
        .build()
        .expect("Failed to build YfClient")
}

/// Fetches a single statement type for one symbol with retries + exponential backoff.
///
/// Returns a Vec of (label, value, timestamp_ms) tuples on success, or an error
/// after all retries are exhausted.
async fn fetch_statement_rows(
    client: &YfClient,
    symbol: &str,
    statement_type: &str,
    annual: bool,
) -> Result<Vec<(String, f64, i64)>, Box<dyn std::error::Error>> {
    let mut last_err: Option<Box<dyn std::error::Error>> = None;

    for attempt in 0..MAX_RETRIES {
        if attempt > 0 {
            let backoff =
                std::time::Duration::from_millis(INITIAL_BACKOFF_MS * 2u64.pow(attempt - 1));
            eprintln!(
                "  Retry {}/{} for {} after {:.1}s...",
                attempt,
                MAX_RETRIES,
                symbol,
                backoff.as_secs_f64()
            );
            tokio::time::sleep(backoff).await;
        }

        let ticker = Ticker::new(client, symbol);

        let result: Result<Vec<(String, f64, i64)>, Box<dyn std::error::Error>> =
            match statement_type {
                "income" | "income_statement" => {
                    let rows_result = if annual {
                        ticker.income_stmt(None).await
                    } else {
                        ticker.quarterly_income_stmt(None).await
                    };
                    match rows_result {
                        Ok(rows) => {
                            let mut out = Vec::new();
                            for row in &rows {
                                let ts_ms = parse_period_to_millis(&format!("{:?}", row.period));
                                let fields: Vec<(&str, Option<f64>)> = vec![
                                    ("TotalRevenue", money_opt_to_f64(&row.total_revenue)),
                                    ("GrossProfit", money_opt_to_f64(&row.gross_profit)),
                                    ("OperatingIncome", money_opt_to_f64(&row.operating_income)),
                                    ("NetIncome", money_opt_to_f64(&row.net_income)),
                                ];
                                for (label, value_opt) in fields {
                                    if let Some(val) = value_opt {
                                        out.push((label.to_string(), val, ts_ms));
                                    }
                                }
                            }
                            Ok(out)
                        }
                        Err(e) => Err(e.into()),
                    }
                }
                "balance" => {
                    let rows_result = if annual {
                        ticker.balance_sheet(None).await
                    } else {
                        ticker.quarterly_balance_sheet(None).await
                    };
                    match rows_result {
                        Ok(rows) => {
                            let mut out = Vec::new();
                            for row in &rows {
                                let ts_ms = parse_period_to_millis(&format!("{:?}", row.period));
                                let fields: Vec<(&str, Option<f64>)> = vec![
                                    ("TotalAssets", money_opt_to_f64(&row.total_assets)),
                                    ("TotalLiabilities", money_opt_to_f64(&row.total_liabilities)),
                                    ("TotalEquity", money_opt_to_f64(&row.total_equity)),
                                ];
                                for (label, value_opt) in fields {
                                    if let Some(val) = value_opt {
                                        out.push((label.to_string(), val, ts_ms));
                                    }
                                }
                            }
                            Ok(out)
                        }
                        Err(e) => Err(e.into()),
                    }
                }
                "cashflow" => {
                    let rows_result = if annual {
                        ticker.cashflow(None).await
                    } else {
                        ticker.quarterly_cashflow(None).await
                    };
                    match rows_result {
                        Ok(rows) => {
                            let mut out = Vec::new();
                            for row in &rows {
                                let ts_ms = parse_period_to_millis(&format!("{:?}", row.period));
                                let fields: Vec<(&str, Option<f64>)> = vec![
                                    (
                                        "OperatingCashFlow",
                                        money_opt_to_f64(&row.operating_cashflow),
                                    ),
                                    (
                                        "CapitalExpenditure",
                                        money_opt_to_f64(&row.capital_expenditures),
                                    ),
                                    ("FreeCashFlow", money_opt_to_f64(&row.free_cash_flow)),
                                ];
                                for (label, value_opt) in fields {
                                    if let Some(val) = value_opt {
                                        out.push((label.to_string(), val, ts_ms));
                                    }
                                }
                            }
                            Ok(out)
                        }
                        Err(e) => Err(e.into()),
                    }
                }
                other => Err(format!("Unknown statement type: {}", other).into()),
            };

        match result {
            Ok(rows) => return Ok(rows),
            Err(e) => {
                let err_msg = e.to_string();
                let is_retryable = err_msg.contains("JSON parsing error")
                    || err_msg.contains("expected value")
                    || err_msg.contains("rate limit")
                    || err_msg.contains("429")
                    || err_msg.contains("500")
                    || err_msg.contains("503")
                    || err_msg.contains("timeout")
                    || err_msg.contains("connection");
                if !is_retryable {
                    return Err(e);
                }
                eprintln!(
                    "  Attempt {}/{} failed for {} {}: {}",
                    attempt + 1,
                    MAX_RETRIES,
                    symbol,
                    statement_type,
                    err_msg
                );
                last_err = Some(e);
            }
        }
    }

    Err(last_err.unwrap_or_else(|| {
        format!(
            "Failed to fetch {} for {} after {} retries",
            statement_type, symbol, MAX_RETRIES
        )
        .into()
    }))
}

/// Downloads financial statements using the yfinance-rs Ticker API.
///
/// Uses `Ticker::income_stmt()`, `Ticker::balance_sheet()`, `Ticker::cashflow()`
/// (and their quarterly variants) with per-symbol retry logic to handle
/// transient Yahoo Finance API failures (empty responses, rate limits, etc.).
pub async fn download_statements(
    client: &YfClient,
    symbols: Vec<String>,
    statement_type: &str,
    annual: bool,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let mut dates: Vec<i64> = Vec::new();
    let mut tickers: Vec<String> = Vec::new();
    let mut labels: Vec<String> = Vec::new();
    let mut values: Vec<f64> = Vec::new();
    let mut statement_types: Vec<String> = Vec::new();
    let mut period_col: Vec<String> = Vec::new();

    let period_tag = if annual { "A" } else { "Q" };

    for symbol in &symbols {
        println!(
            "Downloading {} {} statement ({})",
            symbol,
            statement_type,
            if annual { "annual" } else { "quarterly" }
        );

        match fetch_statement_rows(client, symbol, statement_type, annual).await {
            Ok(rows) => {
                for (label, val, ts_ms) in rows {
                    dates.push(ts_ms);
                    tickers.push(symbol.clone());
                    labels.push(label);
                    values.push(val);
                    statement_types.push(statement_type.to_string());
                    period_col.push(period_tag.to_string());
                }
            }
            Err(e) => {
                eprintln!(
                    "Warning: Failed to fetch {} for {} after all retries: {}",
                    statement_type, symbol, e
                );
            }
        }
    }

    if dates.is_empty() {
        return Err("No statement data found for the given tickers".into());
    }

    let df = DataFrame::new(vec![
        Series::new("date".into(), dates).into_column(),
        Series::new("ticker".into(), tickers).into_column(),
        Series::new("label".into(), labels).into_column(),
        Series::new("value".into(), values).into_column(),
        Series::new("statement_type".into(), statement_types).into_column(),
        Series::new("period".into(), period_col).into_column(),
    ])?;

    // Convert date column from ms timestamp to Datetime
    let df_final = df
        .lazy()
        .with_column(col("date").cast(DataType::Datetime(
            TimeUnit::Milliseconds,
            TimeZone::opt_try_new(Some("UTC"))?,
        )))
        .collect()?;

    Ok(df_final)
}

/// Parse a Period debug string (e.g. "Date(2024-09-28)" or "2024-09-28") to epoch millis.
/// Falls back to current time if parsing fails.
fn parse_period_to_millis(period_str: &str) -> i64 {
    // The Period Debug representation may vary. Try common patterns:
    // 1. Extract a YYYY-MM-DD substring
    let date_str = period_str
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '-')
        .collect::<String>();

    // Try parsing as YYYY-MM-DD
    if let Ok(nd) = chrono::NaiveDate::parse_from_str(&date_str, "%Y-%m-%d") {
        let dt = nd.and_hms_opt(0, 0, 0).unwrap().and_utc();
        return dt.timestamp_millis();
    }

    // Fallback: try parsing the raw string as RFC3339
    if let Ok(dt) = DateTime::parse_from_rfc3339(period_str) {
        return dt.timestamp_millis();
    }

    // Last resort: use current time
    eprintln!(
        "Warning: Could not parse period '{}', using current time",
        period_str
    );
    Utc::now().timestamp_millis()
}

pub fn insert_statements(
    conn: &Connection,
    df: &DataFrame,
) -> Result<(), Box<dyn std::error::Error>> {
    let dates = df.column("date")?.datetime()?.physical().clone();
    let tickers = df.column("ticker")?.str()?;
    let labels = df.column("label")?.str()?;
    let values = df.column("value")?.f64()?;
    let statement_types = df.column("statement_type")?.str()?;
    let periods = df.column("period")?.str()?;

    // Use a temp staging table + DuckDB Appender for bulk loading (much faster than row-by-row)
    conn.execute_batch(
        "CREATE OR REPLACE TEMP TABLE statements_staging (
            date            VARCHAR NOT NULL,
            ticker          VARCHAR NOT NULL,
            label           VARCHAR NOT NULL,
            value           DOUBLE NOT NULL,
            statement_type  VARCHAR NOT NULL,
            period          VARCHAR NOT NULL
        )",
    )?;

    {
        let mut appender = conn.appender("statements_staging")?;
        for i in 0..df.height() {
            let ts_ms = dates.get(i).unwrap_or(0);
            let dt = DateTime::<Utc>::from_timestamp_millis(ts_ms)
                .unwrap_or_default()
                .to_rfc3339();

            appender.append_row(params![
                dt,
                tickers.get(i).unwrap_or(""),
                labels.get(i).unwrap_or(""),
                values.get(i).unwrap_or(0.0),
                statement_types.get(i).unwrap_or(""),
                periods.get(i).unwrap_or(""),
            ])?;
        }
        // Appender flushes on drop
    }

    // Merge staging into main table, skipping duplicates
    conn.execute_batch(
        "INSERT OR IGNORE INTO statements (date, ticker, label, value, statement_type, period)
         SELECT date::TIMESTAMPTZ, ticker, label, value, statement_type, period
         FROM statements_staging;
         DROP TABLE statements_staging;",
    )?;

    Ok(())
}

pub fn read_statements(
    conn: &Connection,
    tickers: Vec<String>,
    statement_type: &str,
    period: &str,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let placeholders = tickers
        .iter()
        .enumerate()
        .map(|(i, _)| format!("?{}", i + 1))
        .collect::<Vec<_>>()
        .join(", ");

    let stmt_type_param = format!("?{}", tickers.len() + 1);
    let period_param = format!("?{}", tickers.len() + 2);

    let sql = format!(
        "SELECT
            strftime(date::TIMESTAMP, '%Y-%m-%dT%H:%M:%S+00:00') as date,
            ticker,
            label,
            value,
            statement_type,
            period
         FROM statements
         WHERE ticker IN ({}) AND statement_type = {} AND period = {}
         ORDER BY ticker, date, label",
        placeholders, stmt_type_param, period_param
    );

    let mut stmt = conn.prepare(&sql)?;

    let db_params: Vec<Box<dyn duckdb::ToSql>> = tickers
        .iter()
        .map(|t| Box::new(t.clone()) as Box<dyn duckdb::ToSql>)
        .chain(std::iter::once(
            Box::new(statement_type.to_string()) as Box<dyn duckdb::ToSql>
        ))
        .chain(std::iter::once(
            Box::new(period.to_string()) as Box<dyn duckdb::ToSql>
        ))
        .collect();

    let rows = stmt
        .query_map(duckdb::params_from_iter(db_params.iter()), |row| {
            Ok((
                row.get::<_, String>(0)?, // date
                row.get::<_, String>(1)?, // ticker
                row.get::<_, String>(2)?, // label
                row.get::<_, f64>(3)?,    // value
                row.get::<_, String>(4)?, // statement_type
                row.get::<_, String>(5)?, // period
            ))
        })?
        .collect::<duckdb::Result<Vec<_>>>()?;

    let mut dates: Vec<i64> = Vec::new();
    let mut ticker_col: Vec<String> = Vec::new();
    let mut labels_col: Vec<String> = Vec::new();
    let mut values_col: Vec<f64> = Vec::new();
    let mut stmt_types: Vec<String> = Vec::new();
    let mut periods_col: Vec<String> = Vec::new();

    for row in rows {
        let (dt_str, ticker, label, value, stmt_type, period) = row;
        if let Ok(dt) = DateTime::parse_from_rfc3339(&dt_str) {
            dates.push(dt.timestamp_millis());
            ticker_col.push(ticker);
            labels_col.push(label);
            values_col.push(value);
            stmt_types.push(stmt_type);
            periods_col.push(period);
        }
    }

    let df = DataFrame::new(vec![
        Series::new("date".into(), dates).into_column(),
        Series::new("ticker".into(), ticker_col).into_column(),
        Series::new("label".into(), labels_col).into_column(),
        Series::new("value".into(), values_col).into_column(),
        Series::new("statement_type".into(), stmt_types).into_column(),
        Series::new("period".into(), periods_col).into_column(),
    ])?;

    let df_final = df
        .lazy()
        .with_column(col("date").cast(DataType::Datetime(
            TimeUnit::Milliseconds,
            TimeZone::opt_try_new(Some("UTC"))?,
        )))
        .collect()?;

    Ok(df_final)
}

pub fn pivot_statements(df: DataFrame) -> Result<DataFrame, Box<dyn std::error::Error>> {
    if df.height() == 0 {
        return Ok(df);
    }

    let df_mapped = df
        .lazy()
        .sort(
            ["date"],
            SortMultipleOptions::default().with_order_descending(false),
        )
        .with_column(
            when(col("period").eq(lit("A")))
                .then(col("date").dt().year().cast(DataType::String))
                .otherwise(concat_str(
                    [
                        lit("Q"),
                        (col("date").dt().month().cast(DataType::Int8) + lit(2i8))
                            .floor_div(lit(3i8))
                            .cast(DataType::String),
                        lit(" "),
                        col("date").dt().year().cast(DataType::String),
                    ],
                    "",
                    true,
                ))
                .alias("nominal_period"),
        )
        .collect()?;

    let df_pivoted = pivot::pivot_stable(
        &df_mapped,
        ["nominal_period"],
        Some(["ticker", "label"]),
        Some(["value"]),
        false,
        None,
        None,
    )?;

    Ok(df_pivoted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_download_income_statements() {
        let client = create_client_with_retries();
        let df = download_statements(&client, vec!["AAPL".to_string()], "income", true)
            .await
            .expect("Download failed");

        println!("Income statement rows: {}", df.height());
        println!("{}", df);

        let unique_labels = df.column("label").unwrap().unique().unwrap();
        let labels_vec = unique_labels
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>();
        println!(
            "Unique labels found ({}): {:?}",
            labels_vec.len(),
            labels_vec
        );
    }

    #[tokio::test]
    async fn test_download_balance_sheet() {
        let client = create_client_with_retries();
        let df = download_statements(&client, vec!["AAPL".to_string()], "balance", true)
            .await
            .expect("Download failed");

        println!("Balance sheet rows: {}", df.height());
        println!("{}", df);
    }

    #[tokio::test]
    async fn test_download_cashflow() {
        let client = create_client_with_retries();
        let df = download_statements(&client, vec!["AAPL".to_string()], "cashflow", false)
            .await
            .expect("Download failed");

        println!("Cashflow rows: {}", df.height());
        println!("{}", df);
    }
}
