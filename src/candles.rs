//use polars_core::prelude::DataType;
//use polars_core::prelude::{DataFrame, DataType};
use chrono::{DateTime, Duration, TimeZone as ChronoTimeZone, Utc};
use duckdb::{Connection, Result, params};
use paft::market::responses::download::DownloadEntry;
use polars::prelude::TimeZone;
use polars::prelude::*;
use tokio::task::JoinSet;
use yfinance_rs::core::conversions::money_to_f64;
use yfinance_rs::{DownloadBuilder, DownloadResponse, Interval, Range, YfClient};

pub async fn get_candles(
    conn: &Connection,
    symbols: Vec<String>,
    interval: Interval,
    range: Range,
    stale_threshold: Duration,
    client: &YfClient,
    timezone: &str,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let now = Utc::now();
    let interval_str = format!("{:?}", interval).to_lowercase();

    let placeholders = symbols
        .iter()
        .enumerate()
        .map(|(i, _)| format!("?{}", i + 1))
        .collect::<Vec<_>>()
        .join(", ");

    let interval_param = format!("?{}", symbols.len() + 1);

    let sql = format!(
        "SELECT ticker, strftime(MAX(date)::TIMESTAMP, '%Y-%m-%dT%H:%M:%S+00:00') as latest
         FROM candles
         WHERE ticker IN ({}) AND interval = {}
         GROUP BY ticker",
        placeholders, interval_param
    );

    let mut stmt = conn.prepare(&sql)?;

    let params: Vec<Box<dyn duckdb::ToSql>> = symbols
        .iter()
        .map(|t| Box::new(t.clone()) as Box<dyn duckdb::ToSql>)
        .chain(std::iter::once(
            Box::new(interval_str) as Box<dyn duckdb::ToSql>
        ))
        .collect();

    let rows = stmt
        .query_map(duckdb::params_from_iter(params.iter()), |row| {
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

    let mut missing: Vec<String> = Vec::new();
    let mut stale: Vec<String> = Vec::new();

    for symbol in &symbols {
        match found.get(symbol) {
            None => {
                println!("{} not found in DB, will download.", symbol);
                missing.push(symbol.clone());
            }
            Some(latest) => {
                let age = now - *latest;
                if age > stale_threshold {
                    println!(
                        "{} is stale (last updated: {}), will download.",
                        symbol, latest
                    );
                    stale.push(symbol.clone());
                } else {
                    println!("{} is up to date (last updated: {}).", symbol, latest);
                }
            }
        }
    }

    if !missing.is_empty() {
        let data = match (&range, &interval) {
            (Range::Max, Interval::D1) => download_chunked(missing, interval, client, None).await?,
            (Range::Max, _) => {
                println!(
                    "Warning: Range::Max is not supported for intraday intervals, defaulting to 5d"
                );
                download(missing, interval, Range::D5, client.clone()).await?
            }
            _ => download(missing, interval, range, client.clone()).await?,
        };
        insert_candles(conn, &data, interval)?;
    }

    if !stale.is_empty() {
        let earliest_start = stale
            .iter()
            .map(|s| found[s] - Duration::days(1))
            .min()
            .unwrap();

        let data = DownloadBuilder::new(client)
            .symbols(stale.clone())
            .between(earliest_start, now)
            .interval(interval)
            .run()
            .await?;

        insert_candles(conn, &data, interval)?;
    }

    let rows = read_candles(conn, symbols, interval)?;
    let df = candles_to_dataframe(rows, timezone)?;

    Ok(df)
}
/*
symbols: Vector of strings
interval: yfinance_rs Interval object.
range: yfinance_rs Range object
clent: yfinance_rs Client object
*/
pub async fn download(
    symbols: Vec<String>,
    interval: Interval,
    range: Range,
    client: YfClient,
) -> Result<DownloadResponse, Box<dyn std::error::Error>> {
    // 2. Now the `?` at the end of `.await?` is allowed!
    let results = DownloadBuilder::new(&client)
        .symbols(symbols)
        .range(range)
        .interval(interval)
        .run()
        .await?;

    println!("Successfully downloaded the data!");

    // 3. We must end the function with `Ok(())` to prove it succeeded if it makes it this far.
    Ok(results)
}

pub async fn download_chunked(
    symbols: Vec<String>,
    interval: Interval,
    client: &YfClient,
    end: Option<DateTime<Utc>>,
) -> Result<DownloadResponse, Box<dyn std::error::Error>> {
    let chunk_size_days = 729;
    let end = end.unwrap_or_else(|| Utc::now() - Duration::minutes(5));
    let start = Utc.with_ymd_and_hms(1980, 1, 1, 0, 0, 0).unwrap();

    // Build list of (chunk_start, chunk_end) pairs upfront
    let mut chunks = Vec::new();
    let mut chunk_start = start;
    while chunk_start < end {
        let chunk_end = (chunk_start + Duration::days(chunk_size_days)).min(end);
        chunks.push((chunk_start, chunk_end));
        chunk_start = chunk_end;
    }

    println!("Downloading {} chunks in parallel...", chunks.len());

    let mut set = JoinSet::new();

    for (chunk_start, chunk_end) in chunks {
        let symbols = symbols.clone();
        let client = client.clone();
        set.spawn(async move {
            println!("Downloading chunk: {} -> {}", chunk_start, chunk_end);
            let result = DownloadBuilder::new(&client)
                .symbols(symbols)
                .between(chunk_start, chunk_end)
                .interval(interval)
                .run()
                .await;
            (chunk_start, chunk_end, result)
        });
    }

    let mut all_entries: Vec<DownloadEntry> = Vec::new();
    while let Some(res) = set.join_next().await {
        let (chunk_start, chunk_end, result) = res?;
        match result {
            Ok(data) => {
                for new_entry in data.entries {
                    let symbol = new_entry.instrument.symbol_str().to_string();
                    if let Some(existing) = all_entries
                        .iter_mut()
                        .find(|e| e.instrument.symbol_str() == symbol)
                    {
                        existing.history.candles.extend(new_entry.history.candles);
                    } else {
                        all_entries.push(new_entry);
                    }
                }
            }
            Err(e) => {
                println!("Skipping chunk {} -> {}: {}", chunk_start, chunk_end, e);
            }
        }
    }

    // Sort each symbol's candles by timestamp since parallel chunks may arrive out of order
    for entry in &mut all_entries {
        entry.history.candles.sort_by_key(|c| c.ts);
    }

    Ok(DownloadResponse {
        entries: all_entries,
    })
}
/* ========================================================
                    Conversion
======================================================== */

pub fn to_dataframe(
    data: &DownloadResponse,
    timezone: &str,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    // 1. Create empty vectors (columns) to hold our data
    let mut dates: Vec<i64> = Vec::new();
    let mut symbols: Vec<String> = Vec::new();
    let mut opens: Vec<f64> = Vec::new();
    let mut highs: Vec<f64> = Vec::new();
    let mut lows: Vec<f64> = Vec::new();
    let mut closes: Vec<f64> = Vec::new();
    let mut volumes: Vec<f64> = Vec::new();

    // 2. Loop through the downloaded Yahoo data
    for entry in &data.entries {
        // Grab the ticker symbol for this specific batch of candles
        let symbol = entry.instrument.symbol().to_string();

        for candle in &entry.history.candles {
            // Push the values into their respective columns
            let unix_timestamp = candle.ts.timestamp();
            dates.push(unix_timestamp * 1000);
            //dates.push(candle.ts as i64);
            symbols.push(symbol.clone());

            // Note: I am using the money_to_f64 converter you used earlier.
            // If your version of yfinance-rs exposes f64 directly, just use `candle.close`.
            opens.push(yfinance_rs::core::conversions::money_to_f64(&candle.open));
            highs.push(yfinance_rs::core::conversions::money_to_f64(&candle.high));
            lows.push(yfinance_rs::core::conversions::money_to_f64(&candle.low));
            closes.push(yfinance_rs::core::conversions::money_to_f64(&candle.close));

            volumes.push(candle.volume.unwrap_or(0) as f64);
        }
    }

    // 3. Assemble the vectors into Polars Series, and build the DataFrame
    let df = DataFrame::new(vec![
        Series::new("date".into(), dates).into_column(),
        Series::new("symbol".into(), symbols).into_column(),
        Series::new("open".into(), opens).into_column(),
        Series::new("high".into(), highs).into_column(),
        Series::new("low".into(), lows).into_column(),
        Series::new("close".into(), closes).into_column(),
        Series::new("volume".into(), volumes).into_column(),
    ])?;
    let df_final = df
        .lazy()
        .with_column(col("date").cast(DataType::Datetime(
            TimeUnit::Milliseconds,
            TimeZone::opt_try_new(Some("UTC"))?,
        )))
        .with_column(
            col("date")
                .dt()
                .convert_time_zone(TimeZone::opt_try_new(Some(timezone))?.unwrap()),
        )
        .collect()?;

    Ok(df_final)
}

pub fn candles_to_dataframe(
    rows: Vec<(String, String, f64, f64, f64, f64, f64)>,
    timezone: &str,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let mut dates: Vec<i64> = Vec::new();
    let mut symbols: Vec<String> = Vec::new();
    let mut opens: Vec<f64> = Vec::new();
    let mut highs: Vec<f64> = Vec::new();
    let mut lows: Vec<f64> = Vec::new();
    let mut closes: Vec<f64> = Vec::new();
    let mut volumes: Vec<f64> = Vec::new();

    for (date_str, ticker, open, high, low, close, volume) in rows {
        if let Ok(dt) = DateTime::parse_from_rfc3339(&date_str) {
            dates.push(dt.timestamp_millis());
            symbols.push(ticker);
            opens.push(open);
            highs.push(high);
            lows.push(low);
            closes.push(close);
            volumes.push(volume);
        }
    }

    let df = DataFrame::new(vec![
        Series::new("date".into(), dates).into_column(),
        Series::new("symbol".into(), symbols).into_column(),
        Series::new("open".into(), opens).into_column(),
        Series::new("high".into(), highs).into_column(),
        Series::new("low".into(), lows).into_column(),
        Series::new("close".into(), closes).into_column(),
        Series::new("volume".into(), volumes).into_column(),
    ])?;

    let df_final = df
        .lazy()
        .with_column(col("date").cast(DataType::Datetime(
            TimeUnit::Milliseconds,
            TimeZone::opt_try_new(Some("UTC"))?,
        )))
        .with_column(
            col("date")
                .dt()
                .convert_time_zone(TimeZone::opt_try_new(Some(timezone))?.unwrap()),
        )
        .collect()?;

    Ok(df_final)
}
/* ========================================================
                    Database
======================================================== */
pub fn create_db() -> Result<Connection> {
    let conn = Connection::open("rust_candles.db")?;

    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS candles (
            date      TIMESTAMPTZ NOT NULL,
            ticker    VARCHAR     NOT NULL,
            interval  VARCHAR     NOT NULL,
            open      DOUBLE      NOT NULL,
            high      DOUBLE      NOT NULL,
            low       DOUBLE      NOT NULL,
            close     DOUBLE      NOT NULL,
            volume    DOUBLE      NOT NULL,
            PRIMARY KEY (date, ticker)
        );
    ",
    )?;

    Ok(conn)
}

pub fn insert_candles(
    conn: &Connection,
    data: &DownloadResponse,
    interval: Interval,
) -> Result<()> {
    let interval_str = format!("{:?}", interval).to_lowercase();

    let mut stmt = conn.prepare(
        "INSERT OR IGNORE INTO candles (date, ticker, interval, open, high, low, close, volume)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
    )?;

    for entry in &data.entries {
        let ticker = entry.instrument.symbol_str();
        for candle in &entry.history.candles {
            stmt.execute(params![
                candle.ts.to_rfc3339(),
                ticker,
                interval_str,
                money_to_f64(&candle.open),
                money_to_f64(&candle.high),
                money_to_f64(&candle.low),
                money_to_f64(&candle.close),
                candle.volume.unwrap_or(0) as f64,
            ])?;
        }
    }

    Ok(())
}

pub fn read_candles(
    conn: &Connection,
    tickers: Vec<String>,
    interval: Interval,
) -> Result<Vec<(String, String, f64, f64, f64, f64, f64)>> {
    let interval_str = format!("{:?}", interval).to_lowercase();

    let placeholders = tickers
        .iter()
        .enumerate()
        .map(|(i, _)| format!("?{}", i + 1))
        .collect::<Vec<_>>()
        .join(", ");

    // interval is the last param, after all the ticker placeholders
    let interval_param = format!("?{}", tickers.len() + 1);

    let sql = format!(
        "SELECT strftime(date::TIMESTAMP, '%Y-%m-%dT%H:%M:%S+00:00'), ticker, open, high, low, close, volume
         FROM candles
         WHERE ticker IN ({}) AND interval = {}
         ORDER BY ticker, date",
        placeholders, interval_param
    );

    let mut stmt = conn.prepare(&sql)?;

    let params: Vec<Box<dyn duckdb::ToSql>> = tickers
        .iter()
        .map(|t| Box::new(t.clone()) as Box<dyn duckdb::ToSql>)
        .chain(std::iter::once(
            Box::new(interval_str) as Box<dyn duckdb::ToSql>
        ))
        .collect();

    let rows = stmt
        .query_map(duckdb::params_from_iter(params.iter()), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, f64>(2)?,
                row.get::<_, f64>(3)?,
                row.get::<_, f64>(4)?,
                row.get::<_, f64>(5)?,
                row.get::<_, f64>(6)?,
            ))
        })?
        .collect::<Result<Vec<_>>>()?;

    Ok(rows)
}
