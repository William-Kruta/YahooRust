use chrono::{DateTime, Duration, Utc};
use duckdb::{Connection, params};
use polars::prelude::*;
use serde_json::Value;

// ════════════════════════════════════════════════════════════════════
//  Constants
// ════════════════════════════════════════════════════════════════════

const MAX_RETRIES: u32 = 3;
const INITIAL_BACKOFF_MS: u64 = 2000;

/// Keys to request from Yahoo's fundamentals-timeseries endpoint.
/// Each key is prefixed with "annual" or "quarterly" at request time.
const INCOME_KEYS: &[&str] = &[
    "TotalRevenue",
    "CostOfRevenue",
    "GrossProfit",
    "OperatingExpense",
    "OperatingIncome",
    "NetIncome",
    "EBITDA",
    "BasicEPS",
    "DilutedEPS",
    "TaxProvision",
    "InterestExpense",
];

const BALANCE_KEYS: &[&str] = &[
    "TotalAssets",
    "CurrentAssets",
    "TotalLiabilitiesNetMinorityInterest",
    "CurrentLiabilities",
    "StockholdersEquity",
    "CashAndCashEquivalents",
    "Inventory",
    "AccountsReceivable",
    "AccountsPayable",
    "TotalDebt",
    "NetDebt",
];

const CASHFLOW_KEYS: &[&str] = &[
    "OperatingCashFlow",
    "CapitalExpenditure",
    "FreeCashFlow",
    "InvestingCashFlow",
    "FinancingCashFlow",
    "RepurchaseOfCapitalStock",
    "CashDividendsPaid",
];

// ════════════════════════════════════════════════════════════════════
//  Public entry point
// ════════════════════════════════════════════════════════════════════

/// Fetch financial statements for `symbols`, using the DB as a cache.
///
/// * `statement_type` – one of `"income"`, `"balance"`, `"cashflow"`
/// * `annual`         – `true` for annual, `false` for quarterly
pub async fn get_statements(
    conn: &Connection,
    symbols: Vec<String>,
    stale_threshold: Duration,
    statement_type: &str,
    annual: bool,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let now = Utc::now();
    let period = if annual { "A" } else { "Q" };

    let db_latest = latest_dates(conn, &symbols, statement_type, period)?;

    let to_download: Vec<String> = symbols
        .iter()
        .filter(|s| match db_latest.get(*s) {
            None => {
                println!("{} not found in DB, will download.", s);
                true
            }
            Some(latest) => {
                if now - *latest > stale_threshold {
                    println!("{} is stale (last: {}), will download.", s, latest);
                    true
                } else {
                    false
                }
            }
        })
        .cloned()
        .collect();

    if !to_download.is_empty() {
        let rows = download_statements(&to_download, statement_type, annual).await;
        if !rows.is_empty() {
            insert_statements(conn, &rows)?;
        }
    }

    let df = read_statements(conn, &symbols, statement_type, period)?;
    Ok(df)
}

// ════════════════════════════════════════════════════════════════════
//  Download (raw Yahoo Finance timeseries API)
// ════════════════════════════════════════════════════════════════════

/// A single row ready for DB insertion.
pub struct StatementRow {
    pub date: String,
    pub ticker: String,
    pub label: String,
    pub value: f64,
    pub statement_type: String,
    pub period: String,
}

/// Download one statement type for many symbols.
pub async fn download_statements(
    symbols: &[String],
    statement_type: &str,
    annual: bool,
) -> Vec<StatementRow> {
    let period_tag = if annual { "A" } else { "Q" };
    let keys = match statement_type {
        "income" => INCOME_KEYS,
        "balance" => BALANCE_KEYS,
        "cashflow" => CASHFLOW_KEYS,
        _ => {
            eprintln!("Unknown statement_type: {}", statement_type);
            return Vec::new();
        }
    };

    let mut out: Vec<StatementRow> = Vec::new();

    for symbol in symbols {
        println!(
            "Downloading {} {} statement ({})…",
            symbol,
            statement_type,
            if annual { "annual" } else { "quarterly" }
        );

        match fetch_timeseries_with_retry(symbol, keys, annual).await {
            Ok(fields) => {
                for (label, value, date_rfc) in fields {
                    out.push(StatementRow {
                        date: date_rfc,
                        ticker: symbol.clone(),
                        label,
                        value,
                        statement_type: statement_type.to_string(),
                        period: period_tag.to_string(),
                    });
                }
                println!("  ✓ {} rows for {}", out.len(), symbol);
            }
            Err(e) => {
                eprintln!(
                    "Warning: {} {} failed after {} retries: {}",
                    symbol, statement_type, MAX_RETRIES, e
                );
            }
        }
    }

    out
}

/// Retry wrapper around the raw timeseries fetch.
async fn fetch_timeseries_with_retry(
    symbol: &str,
    keys: &[&str],
    annual: bool,
) -> Result<Vec<(String, f64, String)>, Box<dyn std::error::Error>> {
    let mut last_err: Option<Box<dyn std::error::Error>> = None;

    for attempt in 0..MAX_RETRIES {
        if attempt > 0 {
            let ms = INITIAL_BACKOFF_MS * 2u64.pow(attempt - 1);
            eprintln!(
                "  Retry {}/{} for {} after {:.1}s…",
                attempt,
                MAX_RETRIES,
                symbol,
                ms as f64 / 1000.0
            );
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        }

        match fetch_timeseries_once(symbol, keys, annual).await {
            Ok(rows) if !rows.is_empty() => return Ok(rows),
            Ok(_) => {
                let msg = format!("Empty response for {}", symbol);
                eprintln!(
                    "  Attempt {}/{} for {}: {}",
                    attempt + 1,
                    MAX_RETRIES,
                    symbol,
                    msg
                );
                last_err = Some(msg.into());
            }
            Err(e) => {
                let msg = e.to_string();
                eprintln!(
                    "  Attempt {}/{} for {} failed: {}",
                    attempt + 1,
                    MAX_RETRIES,
                    symbol,
                    msg
                );
                last_err = Some(e);
            }
        }
    }

    Err(last_err.unwrap_or_else(|| format!("All retries exhausted for {}", symbol).into()))
}

/// Single attempt: hit Yahoo Finance's fundamentals-timeseries endpoint.
///
/// This manually handles cookie + crumb authentication, which is more
/// reliable than the yfinance-rs Ticker API for fundamentals.
async fn fetch_timeseries_once(
    symbol: &str,
    keys: &[&str],
    annual: bool,
) -> Result<Vec<(String, f64, String)>, Box<dyn std::error::Error>> {
    let http = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    // 1. Get cookie
    let resp = http.get("https://fc.yahoo.com").send().await?;
    let cookie = resp
        .headers()
        .get_all("set-cookie")
        .iter()
        .filter_map(|c| c.to_str().ok())
        .collect::<Vec<_>>()
        .join("; ");

    // 2. Get crumb
    let crumb = http
        .get("https://query1.finance.yahoo.com/v1/test/getcrumb")
        .header("cookie", &cookie)
        .send()
        .await?
        .text()
        .await?;

    if crumb.is_empty() || crumb.contains("<!DOCTYPE") {
        return Err("Failed to obtain crumb (got HTML or empty response)".into());
    }

    // 3. Build type string
    let prefix = if annual { "annual" } else { "quarterly" };
    let type_str = keys
        .iter()
        .map(|k| format!("{}{}", prefix, k))
        .collect::<Vec<_>>()
        .join(",");

    let now = Utc::now().timestamp();
    let start = now - (10 * 365 * 24 * 60 * 60); // ~10 years back

    let url = format!(
        "https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/\
         {sym}?symbol={sym}&type={types}&period1={start}&period2={end}&crumb={crumb}",
        sym = symbol,
        types = type_str,
        start = start,
        end = now,
        crumb = crumb,
    );

    // 4. Fetch
    let resp = http.get(&url).header("cookie", &cookie).send().await?;

    let status = resp.status();
    let body = resp.text().await?;

    if !status.is_success() {
        return Err(format!(
            "HTTP {} for {}: {}",
            status,
            symbol,
            &body[..body.len().min(200)]
        )
        .into());
    }

    let json: Value = serde_json::from_str(&body).map_err(|e| {
        format!(
            "JSON parse error for {}: {} (body starts: {})",
            symbol,
            e,
            &body[..body.len().min(100)]
        )
    })?;

    // 5. Parse timeseries result
    let mut out = Vec::new();

    if let Some(results) = json["timeseries"]["result"].as_array() {
        for item in results {
            if let Some(meta_type) = item["meta"]["type"].as_array() {
                for type_name in meta_type {
                    let full_key = type_name.as_str().unwrap_or("");
                    let label = full_key.strip_prefix(prefix).unwrap_or(full_key);

                    if let Some(data_points) = item[full_key].as_array() {
                        for point in data_points {
                            let date_str = point["asOfDate"].as_str().unwrap_or("");
                            let value = extract_number(&point["reportedValue"]);

                            if let (Some(val), false) = (value, date_str.is_empty()) {
                                let date_rfc = date_to_rfc3339(date_str);
                                out.push((label.to_string(), val, date_rfc));
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(out)
}

/// Extract a number from a Yahoo timeseries value object.
/// Handles `{"raw": 123.0}`, `{"fmt": "123", "raw": 123.0}`, or plain numbers.
fn extract_number(v: &Value) -> Option<f64> {
    if let Some(n) = v.as_f64() {
        return Some(n);
    }
    if let Some(n) = v.as_i64() {
        return Some(n as f64);
    }
    if let Some(obj) = v.as_object() {
        if let Some(raw) = obj.get("raw") {
            return extract_number(raw);
        }
    }
    None
}

/// Convert "2024-09-28" to RFC-3339 "2024-09-28T00:00:00+00:00".
fn date_to_rfc3339(date_str: &str) -> String {
    if let Ok(nd) = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
        return nd.and_hms_opt(0, 0, 0).unwrap().and_utc().to_rfc3339();
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(date_str) {
        return dt.to_rfc3339();
    }
    Utc::now().to_rfc3339()
}

// ════════════════════════════════════════════════════════════════════
//  Database
// ════════════════════════════════════════════════════════════════════

/// Bulk-insert via a staging table + DuckDB Appender.
pub fn insert_statements(
    conn: &Connection,
    rows: &[StatementRow],
) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute_batch(
        "CREATE OR REPLACE TEMP TABLE stmts_staging (
            date            VARCHAR NOT NULL,
            ticker          VARCHAR NOT NULL,
            label           VARCHAR NOT NULL,
            value           DOUBLE  NOT NULL,
            statement_type  VARCHAR NOT NULL,
            period          VARCHAR NOT NULL
        )",
    )?;

    {
        let mut app = conn.appender("stmts_staging")?;
        for r in rows {
            app.append_row(params![
                r.date,
                r.ticker,
                r.label,
                r.value,
                r.statement_type,
                r.period,
            ])?;
        }
    }

    conn.execute_batch(
        "INSERT OR IGNORE INTO statements (date, ticker, label, value, statement_type, period)
         SELECT date::TIMESTAMPTZ, ticker, label, value, statement_type, period
         FROM stmts_staging;
         DROP TABLE stmts_staging;",
    )?;

    Ok(())
}

/// Read statements from DB into a Polars DataFrame.
pub fn read_statements(
    conn: &Connection,
    tickers: &[String],
    statement_type: &str,
    period: &str,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let placeholders = tickers
        .iter()
        .enumerate()
        .map(|(i, _)| format!("?{}", i + 1))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "SELECT strftime(date::TIMESTAMP, '%Y-%m-%dT%H:%M:%S+00:00') AS date,
                ticker, label, value, statement_type, period
         FROM   statements
         WHERE  ticker IN ({ph})
           AND  statement_type = ?{st}
           AND  period         = ?{p}
         ORDER BY ticker, date, label",
        ph = placeholders,
        st = tickers.len() + 1,
        p = tickers.len() + 2,
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
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, f64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?
        .collect::<duckdb::Result<Vec<_>>>()?;

    let mut dates: Vec<i64> = Vec::with_capacity(rows.len());
    let mut ticker_c: Vec<String> = Vec::with_capacity(rows.len());
    let mut label_c: Vec<String> = Vec::with_capacity(rows.len());
    let mut value_c: Vec<f64> = Vec::with_capacity(rows.len());
    let mut type_c: Vec<String> = Vec::with_capacity(rows.len());
    let mut period_c: Vec<String> = Vec::with_capacity(rows.len());

    for (dt_str, ticker, label, value, st, per) in rows {
        if let Ok(dt) = DateTime::parse_from_rfc3339(&dt_str) {
            dates.push(dt.timestamp_millis());
            ticker_c.push(ticker);
            label_c.push(label);
            value_c.push(value);
            type_c.push(st);
            period_c.push(per);
        }
    }

    let df = DataFrame::new(vec![
        Series::new("date".into(), dates).into_column(),
        Series::new("ticker".into(), ticker_c).into_column(),
        Series::new("label".into(), label_c).into_column(),
        Series::new("value".into(), value_c).into_column(),
        Series::new("statement_type".into(), type_c).into_column(),
        Series::new("period".into(), period_c).into_column(),
    ])?;

    let df = df
        .lazy()
        .with_column(col("date").cast(DataType::Datetime(
            TimeUnit::Milliseconds,
            TimeZone::opt_try_new(Some("UTC"))?,
        )))
        .collect()?;

    Ok(df)
}

/// Most-recent date per ticker in the DB for a given type+period.
fn latest_dates(
    conn: &Connection,
    symbols: &[String],
    statement_type: &str,
    period: &str,
) -> Result<std::collections::HashMap<String, DateTime<Utc>>, Box<dyn std::error::Error>> {
    let placeholders = symbols
        .iter()
        .enumerate()
        .map(|(i, _)| format!("?{}", i + 1))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "SELECT ticker,
                strftime(MAX(date)::TIMESTAMP, '%Y-%m-%dT%H:%M:%S+00:00') AS latest
         FROM   statements
         WHERE  ticker IN ({ph})
           AND  statement_type = ?{st}
           AND  period         = ?{p}
         GROUP BY ticker",
        ph = placeholders,
        st = symbols.len() + 1,
        p = symbols.len() + 2,
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

    let mut map = std::collections::HashMap::new();
    for (ticker, date_str) in rows {
        if let Ok(dt) = DateTime::parse_from_rfc3339(&date_str) {
            map.insert(ticker, dt.with_timezone(&Utc));
        }
    }
    Ok(map)
}

// ════════════════════════════════════════════════════════════════════
//  Pivot
// ════════════════════════════════════════════════════════════════════

/// Pivot long-form into wide form.
///
/// Result: `ticker | label | <period1> | <period2> | …`
pub fn pivot_statements(df: DataFrame) -> Result<DataFrame, Box<dyn std::error::Error>> {
    if df.height() == 0 {
        return Ok(df);
    }

    let df = df
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

    let pivoted = pivot::pivot_stable(
        &df,
        ["nominal_period"],
        Some(["ticker", "label"]),
        Some(["value"]),
        false,
        None,
        None,
    )?;

    Ok(pivoted)
}

// ════════════════════════════════════════════════════════════════════
//  Margins
// ════════════════════════════════════════════════════════════════════

/// Margin definitions: (output_label, numerator_label, denominator_label)
const MARGIN_DEFS: &[(&str, &str, &str)] = &[
    ("GrossMargin", "GrossProfit", "TotalRevenue"),
    ("OperatingMargin", "OperatingIncome", "TotalRevenue"),
    ("NetMargin", "NetIncome", "TotalRevenue"),
    ("FCFMargin", "FreeCashFlow", "TotalRevenue"),
];

/// Calculate margin ratios from a **pivoted** (wide-form) statements DataFrame.
///
/// Input shape:  `ticker | label | 2021 | 2022 | 2023 | …`
/// Output shape: same columns, but `label` contains margin names and the
/// period columns contain the ratio (0.0–1.0, or negative).
///
/// Rows where the numerator or denominator is missing become `null`.
/// Tickers that lack either component are silently skipped.
pub fn calculate_margins(pivoted: &DataFrame) -> Result<DataFrame, Box<dyn std::error::Error>> {
    // Period columns are everything except "ticker" and "label"
    let period_cols: Vec<String> = pivoted
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .filter(|c| c != "ticker" && c != "label")
        .collect();

    // Get unique tickers
    let tickers = pivoted.column("ticker")?.unique()?;
    let tickers = tickers.str()?;

    let mut all_frames: Vec<DataFrame> = Vec::new();

    for opt_ticker in tickers.into_iter() {
        let ticker = match opt_ticker {
            Some(t) => t,
            None => continue,
        };

        // Filter rows for this ticker
        let ticker_df = pivoted
            .clone()
            .lazy()
            .filter(col("ticker").eq(lit(ticker)))
            .collect()?;

        for &(margin_name, numer_label, denom_label) in MARGIN_DEFS {
            // Find numerator and denominator rows
            let numer_row = ticker_df
                .clone()
                .lazy()
                .filter(col("label").eq(lit(numer_label)))
                .collect()?;
            let denom_row = ticker_df
                .clone()
                .lazy()
                .filter(col("label").eq(lit(denom_label)))
                .collect()?;

            if numer_row.height() == 0 || denom_row.height() == 0 {
                continue; // skip if data missing for this ticker
            }

            // Build margin values for each period column
            let mut margin_series: Vec<Column> = Vec::new();
            margin_series
                .push(Series::new("ticker".into(), vec![ticker.to_string()]).into_column());
            margin_series
                .push(Series::new("label".into(), vec![margin_name.to_string()]).into_column());

            for period_col in &period_cols {
                let numer_val = numer_row
                    .column(period_col.as_str())
                    .ok()
                    .and_then(|s| s.f64().ok())
                    .and_then(|ca| ca.get(0));

                let denom_val = denom_row
                    .column(period_col.as_str())
                    .ok()
                    .and_then(|s| s.f64().ok())
                    .and_then(|ca| ca.get(0));

                let margin: Option<f64> = match (numer_val, denom_val) {
                    (Some(n), Some(d)) if d != 0.0 => Some(n / d),
                    _ => None,
                };

                let series = Series::new(period_col.as_str().into(), &[margin]);
                margin_series.push(series.into_column());
            }

            let row_df = DataFrame::new(margin_series)?;
            all_frames.push(row_df);
        }
    }

    if all_frames.is_empty() {
        return Err("No margins could be calculated (missing revenue or numerator data)".into());
    }

    // Stack all margin rows into one DataFrame
    let mut result = all_frames.remove(0);
    for frame in all_frames {
        result = result.vstack(&frame)?;
    }

    // Sort by ticker, then label
    let result = result
        .lazy()
        .sort(["ticker", "label"], SortMultipleOptions::default())
        .collect()?;

    Ok(result)
}

// ════════════════════════════════════════════════════════════════════
//  Tests
// ════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_download_income() {
        let rows = download_statements(&["AAPL".into()], "income", true).await;
        assert!(!rows.is_empty(), "Expected income rows for AAPL");
        for r in &rows {
            println!("{} | {} | {} | {}", r.date, r.ticker, r.label, r.value);
        }
    }

    #[tokio::test]
    async fn test_download_balance() {
        let rows = download_statements(&["RKLB".into()], "balance", true).await;
        assert!(!rows.is_empty(), "Expected balance rows for RKLB");
        for r in &rows {
            println!("{} | {} | {} | {}", r.date, r.ticker, r.label, r.value);
        }
    }

    #[tokio::test]
    async fn test_download_cashflow() {
        let rows = download_statements(&["MSFT".into()], "cashflow", false).await;
        assert!(!rows.is_empty(), "Expected cashflow rows for MSFT");
        for r in &rows {
            println!("{} | {} | {} | {}", r.date, r.ticker, r.label, r.value);
        }
    }
}
