//use polars_core::prelude::DataType;
//use polars_core::prelude::{DataFrame, DataType};
use chrono::{DateTime, Duration, TimeZone as ChronoTimeZone, Utc};
use duckdb::{Connection, Result, params};
use paft::market::responses::download::DownloadEntry;
use polars::prelude::TimeZone;
use polars::prelude::*;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
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
    force_update: bool,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let mut ipo_dates = std::collections::HashMap::new();
    for sym in &symbols {
        if let Ok(Some(dt)) = crate::info::get_first_trading_day(conn, sym) {
            ipo_dates.insert(sym.clone(), dt);
        }
    }

    // Pre-fetch IPO dates for symbols we don't have yet.
    if matches!(range, Range::Max) {
        let unknown_symbols: Vec<&String> = symbols
            .iter()
            .filter(|s| !ipo_dates.contains_key(*s))
            .collect();
        for sym in unknown_symbols {
            if let Ok(_info) = crate::info::get_company_info(conn, sym, client, 30).await {
                if let Ok(Some(dt)) = crate::info::get_first_trading_day(conn, sym) {
                    ipo_dates.insert(sym.clone(), dt);
                }
            }
        }
    }

    // Get latest dates from DB to enable incremental updates
    let db_latest = get_latest_dates(conn, &symbols, interval)?;
    let now = Utc::now();

    // Determine which symbols actually need a download (stale or missing or forced)
    let symbols_to_download: Vec<String> = if force_update {
        symbols.clone()
    } else {
        symbols
            .iter()
            .filter(|s| match db_latest.get(*s) {
                None => {
                    println!("{} not found in DB, will download.", s);
                    true
                }
                Some(latest) => {
                    let age = now - *latest;
                    if age > stale_threshold {
                        println!("{} is stale (updated: {}), will delta-download.", s, latest);
                        true
                    } else {
                        // println!("{} is up to date.", s);
                        false
                    }
                }
            })
            .cloned()
            .collect()
    };

    if !symbols_to_download.is_empty() {
        let t0 = Instant::now();
        let data = match (&range, &interval) {
            (Range::Max, Interval::D1) => {
                download_chunked(
                    symbols_to_download,
                    interval,
                    client,
                    None,
                    &ipo_dates,
                    &db_latest,
                )
                .await?
            }
            (Range::Max, _) => {
                // For non-daily Range::Max, use 'between' from earliest needed date
                let overall_start = symbols_to_download
                    .iter()
                    .map(|s| {
                        db_latest
                            .get(s)
                            .copied()
                            .unwrap_or_else(|| Utc.with_ymd_and_hms(1980, 1, 1, 0, 0, 0).unwrap())
                    })
                    .min()
                    .unwrap();

                DownloadBuilder::new(client)
                    .symbols(symbols_to_download)
                    .between(overall_start, now)
                    .interval(interval)
                    .run()
                    .await?
            }
            _ => download(symbols_to_download, interval, range, client.clone()).await?,
        };
        eprintln!("[TIMING] Download completed in {:.2?}", t0.elapsed());

        let t1 = Instant::now();
        insert_candles(conn, &data, interval)?;
        eprintln!("[TIMING] DB insert completed in {:.2?}", t1.elapsed());
    }

    // After download or if up to date, read all requested data back for the DataFrame
    let t_read = Instant::now();
    let final_rows = read_candles(conn, symbols.clone(), interval)?;
    eprintln!(
        "[TIMING] read_candles ({} rows) completed in {:.2?}",
        final_rows.len(),
        t_read.elapsed()
    );

    // Update IPO dates in DB if we found earlier candles
    let t_min = Instant::now();
    let mut min_dates: std::collections::HashMap<String, DateTime<Utc>> =
        std::collections::HashMap::new();
    for (date_str, ticker, _, _, _, _, _) in &final_rows {
        if let Ok(dt) = DateTime::parse_from_rfc3339(date_str) {
            let dt = dt.with_timezone(&Utc);
            min_dates
                .entry(ticker.clone())
                .and_modify(|existing| {
                    if dt < *existing {
                        *existing = dt;
                    }
                })
                .or_insert(dt);
        }
    }

    for (ticker, min_dt) in min_dates {
        let existing = ipo_dates.get(&ticker);
        if existing.is_none() || Some(&min_dt) < existing {
            let _ = crate::info::update_first_trading_day(conn, &ticker, min_dt);
        }
    }
    eprintln!(
        "[TIMING] IPO dates update completed in {:.2?}",
        t_min.elapsed()
    );

    let t_df = Instant::now();
    let df = candles_to_dataframe(final_rows, timezone)?;
    eprintln!(
        "[TIMING] candles_to_dataframe completed in {:.2?}",
        t_df.elapsed()
    );

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

    println!("Successfully downloaded the candles!");

    // 3. We must end the function with `Ok(())` to prove it succeeded if it makes it this far.
    Ok(results)
}

pub async fn download_chunked(
    symbols: Vec<String>,
    interval: Interval,
    client: &YfClient,
    end: Option<DateTime<Utc>>,
    ipo_dates: &std::collections::HashMap<String, DateTime<Utc>>,
    existing_latest: &std::collections::HashMap<String, DateTime<Utc>>,
) -> Result<DownloadResponse, Box<dyn std::error::Error>> {
    let chunk_size_days = 3650;
    let end = end.unwrap_or_else(|| Utc::now() - Duration::minutes(5));

    // Earliest possible start is 1980, or the earliest known IPO date among symbols
    let overall_start = ipo_dates
        .values()
        .min()
        .copied()
        .unwrap_or_else(|| Utc.with_ymd_and_hms(1980, 1, 1, 0, 0, 0).unwrap());

    let mut chunks = Vec::new();
    let mut chunk_start = overall_start;
    while chunk_start < end {
        let chunk_end = (chunk_start + Duration::days(chunk_size_days)).min(end);
        chunks.push((chunk_start, chunk_end));
        chunk_start = chunk_end;
    }

    println!("Downloading {} candle chunks in parallel...", chunks.len());

    let mut set = JoinSet::new();
    let semaphore = Arc::new(Semaphore::new(4)); // Limit to 4 concurrent API requests

    for (chunk_start, chunk_end) in chunks {
        let client = client.clone();
        let sem = semaphore.clone();

        // Filter symbols that actually need this chunk (IPO checked + DB delta checked)
        let active_symbols: Vec<String> = symbols
            .iter()
            .filter(|s| {
                let ipo_ok = match ipo_dates.get(*s) {
                    Some(sd) => *sd <= chunk_end,
                    None => true, // Unknown IPO, assume always active
                };
                let delta_ok = match existing_latest.get(*s) {
                    Some(ld) => chunk_end > *ld, // Download if chunk end is after DB latest
                    None => true,                // Missing from DB, assume always active
                };
                ipo_ok && delta_ok
            })
            .cloned()
            .collect();

        if active_symbols.is_empty() {
            continue;
        }

        set.spawn(async move {
            let _permit = sem.acquire().await.expect("semaphore closed");
            let t_chunk = Instant::now();
            eprintln!(
                "[TIMING] Starting chunk: {} -> {} for {} symbols",
                chunk_start,
                chunk_end,
                active_symbols.len()
            );
            let result = DownloadBuilder::new(&client)
                .symbols(active_symbols)
                .between(chunk_start, chunk_end)
                .interval(interval)
                .run()
                .await;
            eprintln!(
                "[TIMING] Chunk {} -> {} finished in {:.2?}",
                chunk_start,
                chunk_end,
                t_chunk.elapsed()
            );
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

pub fn _to_dataframe(
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

pub fn insert_candles(
    conn: &Connection,
    data: &DownloadResponse,
    interval: Interval,
) -> Result<()> {
    let interval_str = format!("{:?}", interval).to_lowercase();

    // Use a temp staging table + DuckDB Appender for bulk loading (orders of magnitude faster)
    conn.execute_batch(
        "CREATE OR REPLACE TEMP TABLE candles_staging (
            date      VARCHAR NOT NULL,
            ticker    VARCHAR NOT NULL,
            interval  VARCHAR NOT NULL,
            open      DOUBLE NOT NULL,
            high      DOUBLE NOT NULL,
            low       DOUBLE NOT NULL,
            close     DOUBLE NOT NULL,
            volume    DOUBLE NOT NULL
        )",
    )?;

    {
        let mut appender = conn.appender("candles_staging")?;
        for entry in &data.entries {
            let ticker = entry.instrument.symbol_str();
            for candle in &entry.history.candles {
                appender.append_row(params![
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
        // Appender flushes on drop
    }

    // Merge staging into main table, skipping duplicates
    conn.execute_batch(
        "INSERT OR IGNORE INTO candles (date, ticker, interval, open, high, low, close, volume)
         SELECT date::TIMESTAMPTZ, ticker, interval, open, high, low, close, volume
         FROM candles_staging;
         DROP TABLE candles_staging;",
    )?;

    Ok(())
}

pub fn get_latest_dates(
    conn: &Connection,
    symbols: &[String],
    interval: Interval,
) -> Result<std::collections::HashMap<String, DateTime<Utc>>, Box<dyn std::error::Error>> {
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
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })?
        .collect::<duckdb::Result<Vec<_>>>()?;

    let mut found: std::collections::HashMap<String, DateTime<Utc>> =
        std::collections::HashMap::new();
    for (ticker, date_str_opt) in rows {
        if let Some(date_str) = date_str_opt {
            if let Ok(dt) = DateTime::parse_from_rfc3339(&date_str) {
                found.insert(ticker, dt.with_timezone(&Utc));
            }
        }
    }

    Ok(found)
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
