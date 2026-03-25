use crate::candles::get_candles;
use crate::greeks::add_greeks_to_df;
use chrono::{DateTime, Duration, Utc};
use duckdb::{Connection, Result, params};
use paft::prelude::OptionContract as PaftOptionContract;
use polars::prelude::*;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use yfinance_rs::core::conversions::money_to_f64;
use yfinance_rs::{Interval, Range};
use yfinance_rs::{Ticker, YfClient};

#[derive(Debug)]
pub struct OptionContract {
    pub contract_symbol: String,
    pub strike: f64,
    pub last_price: f64,
    pub bid: f64,
    pub ask: f64,
    pub volume: u64,
    pub open_interest: u64,
    pub implied_volatility: f64,
    pub in_the_money: bool,
    pub expiration: i64,
}

#[derive(Copy, Clone)]
pub enum ExpirationMode {
    Nearest,
    All,
}
// Easy to extend later, e.g.:
// Next(usize),  // next N expirations
// Before(DateTime<Utc>),  // all expirations before a date

fn extract_latest_prices(
    df: &DataFrame,
) -> Result<std::collections::HashMap<String, f64>, Box<dyn std::error::Error>> {
    let mut prices = std::collections::HashMap::new();
    let symbols = df.column("symbol")?.str()?;
    let closes = df.column("close")?.f64()?;

    for i in 0..df.height() {
        if let Some(sym) = symbols.get(i) {
            if let Some(close) = closes.get(i) {
                prices.insert(sym.to_string(), close);
            }
        }
    }

    Ok(prices)
}

pub async fn get_options(
    conn: &Connection,
    symbols: Vec<String>,
    stale_threshold: Duration,
    client: &YfClient,
    risk_free_rate: Option<f64>,
    expiration_mode: ExpirationMode,
    force_update: bool,
    update_candles: bool,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let candles_df = get_candles(
        conn,
        symbols.clone(),
        Interval::D1,
        Range::Max, // Changed to Max for historical prob analysis
        stale_threshold,
        client,
        "UTC",
        update_candles,
    )
    .await?;
    let stock_prices = extract_latest_prices(&candles_df)?;

    // Get latest collection dates for incremental updates
    let db_latest = get_latest_options_dates(conn, &symbols)?;
    let now = Utc::now();

    // Determine which symbols actually need a download (stale or missing or forced)
    let symbols_to_download: Vec<String> = if force_update {
        symbols.clone()
    } else {
        symbols
            .iter()
            .filter(|s| match db_latest.get(*s) {
                None => {
                    println!("{} options not found in DB, will download.", s);
                    true
                }
                Some(latest) => {
                    let age = now - *latest;
                    if age > stale_threshold {
                        println!(
                            "{} options are stale (updated: {}), will download.",
                            s, latest
                        );
                        true
                    } else {
                        // println!("{} options are up to date.", s);
                        false
                    }
                }
            })
            .cloned()
            .collect()
    };

    if !symbols_to_download.is_empty() {
        let t0 = Instant::now();
        let mut new_data_df = download_chain_chunked(
            client,
            symbols_to_download,
            risk_free_rate,
            expiration_mode,
            &stock_prices,
        )
        .await?;
        eprintln!(
            "[TIMING] Options download completed in {:.2?}",
            t0.elapsed()
        );

        // Add historical probability of profit
        let t_hist = Instant::now();
        new_data_df = calculate_historical_probs(new_data_df, candles_df)?;
        eprintln!(
            "[TIMING] Historical PoP calculation completed in {:.2?}",
            t_hist.elapsed()
        );

        let t1 = Instant::now();
        insert_options(conn, &new_data_df)?;
        eprintln!(
            "[TIMING] Options DB insert completed in {:.2?}",
            t1.elapsed()
        );
    }

    let t_read = Instant::now();
    let df = read_options(conn, symbols)?;
    eprintln!(
        "[TIMING] Options read_options ({} rows) completed in {:.2?}",
        df.height(),
        t_read.elapsed()
    );

    Ok(df)
}

#[allow(dead_code)]
pub async fn download_chain(
    client: &YfClient,
    symbol: &str,
    risk_free_rate: Option<f64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let obj = Ticker::new(client, symbol);
    let expirations = obj.options().await?;

    // Resolve risk_free_rate once, before the main logic
    let risk_free_rate: f64 = match risk_free_rate {
        Some(r) => r,
        None => {
            let rr_obj = Ticker::new(client, "^TNX");
            let fi = rr_obj.fast_info().await?;
            fi.last
                .as_ref()
                .map(money_to_f64)
                .or_else(|| fi.previous_close.as_ref().map(money_to_f64))
                .unwrap_or(0.05)
        }
    };

    if let Some(nearest) = expirations.first() {
        let chain = obj.option_chain(Some(*nearest)).await?;

        let calls = parse_contracts(&chain.calls);
        let puts = parse_contracts(&chain.puts);

        let mut calls_df = options_to_dataframe(calls)?;
        let mut puts_df = options_to_dataframe(puts)?;

        calls_df = calls_df
            .lazy()
            .with_column(lit("call").alias("option_type"))
            .with_column(lit(symbol).alias("ticker"))
            .collect()?;

        puts_df = puts_df
            .lazy()
            .with_column(lit("put").alias("option_type"))
            .with_column(lit(symbol).alias("ticker"))
            .collect()?;

        let now = Utc::now();
        let collected_at_ms = now.timestamp_millis();

        let fi = obj.fast_info().await?;
        let underlying_price = fi
            .last
            .as_ref()
            .map(money_to_f64)
            .or_else(|| fi.previous_close.as_ref().map(money_to_f64))
            .unwrap_or(0.0);

        let combined_df = concat([calls_df.lazy(), puts_df.lazy()], UnionArgs::default())?
            .with_columns([
                lit(collected_at_ms)
                    .cast(DataType::Datetime(
                        TimeUnit::Milliseconds,
                        TimeZone::opt_try_new(Some("UTC"))?,
                    ))
                    .alias("collected_at"),
                lit(underlying_price).alias("stock_price"),
            ])
            .with_column(
                (col("expiration").cast(DataType::Date) - col("collected_at").cast(DataType::Date))
                    .dt()
                    .total_days()
                    .cast(DataType::Int32)
                    .alias("dte"),
            )
            .collect()?;

        let combined_df = add_greeks_to_df(combined_df, underlying_price, risk_free_rate)?;
        println!("Combined:\n{}", combined_df);
    }

    Ok(())
}

pub async fn download_chain_chunked(
    client: &YfClient,
    symbols: Vec<String>,
    risk_free_rate: Option<f64>,
    expiration_mode: ExpirationMode,
    stock_prices: &std::collections::HashMap<String, f64>,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    // Resolve risk_free_rate once upfront for all symbols
    let risk_free_rate: f64 = match risk_free_rate {
        Some(r) => r,
        None => {
            let rr_obj = Ticker::new(client, "^TNX");
            let fi = rr_obj.fast_info().await?;
            fi.last
                .as_ref()
                .map(money_to_f64)
                .or_else(|| fi.previous_close.as_ref().map(money_to_f64))
                .unwrap_or(0.05)
        }
    };

    let now = Utc::now();
    let collected_at_ms = now.timestamp_millis();

    let mut set = JoinSet::new();
    let semaphore = Arc::new(Semaphore::new(4)); // Limit to 4 concurrent ticker requests

    for symbol in symbols {
        let client = client.clone();
        let sem = semaphore.clone();
        let mut underlying_price = stock_prices.get(&symbol).copied().unwrap_or(0.0);

        set.spawn(async move {
            let _permit = sem.acquire().await.expect("semaphore closed");
            let t_ticker = Instant::now();
            println!("[TIMING] Starting download for: {}", symbol);

            let obj = Ticker::new(&client, &symbol);

            let expirations = obj.options().await?;
            if expirations.is_empty() {
                return Ok::<Option<DataFrame>, Box<dyn std::error::Error + Send + Sync>>(None);
            }

            // Either get all expirations or just the nearest
            let expirations_to_fetch: Vec<i64> = match expiration_mode {
                ExpirationMode::Nearest => vec![*expirations.first().unwrap()],
                ExpirationMode::All => expirations.clone(),
            };
            let mut expiration_dfs: Vec<LazyFrame> = Vec::new();

            for expiration in &expirations_to_fetch {
                let chain = obj.option_chain(Some(*expiration)).await?;

                let calls = parse_contracts(&chain.calls);
                let puts = parse_contracts(&chain.puts);

                let mut calls_df = options_to_dataframe(calls).map_err(|e| format!("{}", e))?;
                let mut puts_df = options_to_dataframe(puts).map_err(|e| format!("{}", e))?;

                calls_df = calls_df
                    .lazy()
                    .with_column(lit("call").alias("option_type"))
                    .with_column(lit(symbol.as_str()).alias("ticker"))
                    .collect()?;

                puts_df = puts_df
                    .lazy()
                    .with_column(lit("put").alias("option_type"))
                    .with_column(lit(symbol.as_str()).alias("ticker"))
                    .collect()?;

                expiration_dfs.push(calls_df.lazy());
                expiration_dfs.push(puts_df.lazy());
            }

            if underlying_price == 0.0 {
                if let Ok(fi) = obj.fast_info().await {
                    underlying_price = fi
                        .last
                        .as_ref()
                        .map(money_to_f64)
                        .or_else(|| fi.previous_close.as_ref().map(money_to_f64))
                        .unwrap_or(0.0);
                }
            }

            let combined_df = concat(expiration_dfs, UnionArgs::default())?
                .with_columns([
                    lit(collected_at_ms)
                        .cast(DataType::Datetime(
                            TimeUnit::Milliseconds,
                            TimeZone::opt_try_new(Some("UTC"))?,
                        ))
                        .alias("collected_at"),
                    lit(underlying_price).alias("stock_price"),
                ])
                .with_column(
                    (col("expiration").cast(DataType::Date)
                        - col("collected_at").cast(DataType::Date))
                    .dt()
                    .total_days()
                    .cast(DataType::Int32)
                    .alias("dte"),
                )
                .collect()?;

            let combined_df = add_greeks_to_df(combined_df, underlying_price, risk_free_rate)
                .map_err(|e| format!("{}", e))?;

            eprintln!(
                "[TIMING] Options chain for {} ({} exp) finished in {:.2?}",
                symbol,
                expirations_to_fetch.len(),
                t_ticker.elapsed()
            );

            Ok(Some(combined_df))
        });
    }

    // Collect all results and concatenate into one DataFrame
    let mut all_dfs: Vec<LazyFrame> = Vec::new();
    while let Some(res) = set.join_next().await {
        match res? {
            Ok(Some(df)) => {
                all_dfs.push(df.lazy());
            }
            Ok(None) => eprintln!("No expirations found for symbol, skipping."),
            Err(e) => eprintln!("Error downloading chain: {}", e),
        }
    }

    // Concatenate all symbol DataFrames into one
    let final_df = concat(all_dfs, UnionArgs::default())?.collect()?;

    Ok(final_df)
}
#[allow(dead_code)]
pub async fn get_expirations(
    client: &YfClient,
    ticker: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use chrono::DateTime;

    let obj = Ticker::new(client, ticker);

    let expirations = obj.options().await?;
    for exp in &expirations {
        let dt = DateTime::from_timestamp(*exp as i64, 0).ok_or("Invalid timestamp")?;
        println!(
            "{} - Expiration: {}",
            ticker,
            dt.format("%Y-%m-%d %H:%M:%S UTC")
        );
    }

    Ok(())
}

/* ========================================================
                    Utilities
======================================================== */
pub fn parse_contracts(contracts: &[PaftOptionContract]) -> Vec<OptionContract> {
    contracts
        .iter()
        .map(|c| OptionContract {
            contract_symbol: c.contract_symbol.to_string().clone(),
            strike: money_to_f64(&c.strike),
            last_price: c.price.as_ref().map(money_to_f64).unwrap_or(0.0),
            bid: c.bid.as_ref().map(money_to_f64).unwrap_or(0.0),
            ask: c.ask.as_ref().map(money_to_f64).unwrap_or(0.0),
            volume: c.volume.unwrap_or(0),
            open_interest: c.open_interest.unwrap_or(0),
            implied_volatility: c.implied_volatility.unwrap_or(0.0),
            in_the_money: c.in_the_money,
            expiration: c
                .expiration_date
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp(),
        })
        .collect()
}

pub fn options_to_dataframe(
    contracts: Vec<OptionContract>,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let mut contract_symbols: Vec<String> = Vec::new();
    let mut strikes: Vec<f64> = Vec::new();
    let mut last_prices: Vec<f64> = Vec::new();
    let mut bids: Vec<f64> = Vec::new();
    let mut asks: Vec<f64> = Vec::new();
    let mut volumes: Vec<u64> = Vec::new();
    let mut open_interests: Vec<u64> = Vec::new();
    let mut implied_volatilities: Vec<f64> = Vec::new();
    let mut in_the_moneys: Vec<bool> = Vec::new();
    let mut expirations: Vec<i64> = Vec::new();

    for c in contracts {
        contract_symbols.push(c.contract_symbol);
        strikes.push(c.strike);
        last_prices.push(c.last_price);
        bids.push(c.bid);
        asks.push(c.ask);
        volumes.push(c.volume);
        open_interests.push(c.open_interest);
        implied_volatilities.push(c.implied_volatility);
        in_the_moneys.push(c.in_the_money);
        expirations.push(c.expiration * 1000); // convert to milliseconds
    }

    let df = DataFrame::new(vec![
        Series::new("contract_symbol".into(), contract_symbols).into_column(),
        Series::new("strike".into(), strikes).into_column(),
        Series::new("last_price".into(), last_prices).into_column(),
        Series::new("bid".into(), bids).into_column(),
        Series::new("ask".into(), asks).into_column(),
        Series::new("volume".into(), volumes).into_column(),
        Series::new("open_interest".into(), open_interests).into_column(),
        Series::new("implied_volatility".into(), implied_volatilities).into_column(),
        Series::new("in_the_money".into(), in_the_moneys).into_column(),
        Series::new("expiration".into(), expirations).into_column(),
    ])?;

    let df_final = df
        .lazy()
        .with_column(col("expiration").cast(DataType::Datetime(
            TimeUnit::Milliseconds,
            TimeZone::opt_try_new(Some("UTC"))?,
        )))
        .collect()?;

    Ok(df_final)
}

/* ========================================================
                    Database
======================================================== */

pub fn insert_options(conn: &Connection, df: &DataFrame) -> Result<(), Box<dyn std::error::Error>> {
    let contract_symbols = df.column("contract_symbol")?.str()?;
    let strikes = df.column("strike")?.f64()?;
    let stock_prices = df.column("stock_price")?.f64()?;
    let last_prices = df.column("last_price")?.f64()?;
    let bids = df.column("bid")?.f64()?;
    let asks = df.column("ask")?.f64()?;
    let volumes = df.column("volume")?.u64()?;
    let open_interests = df.column("open_interest")?.u64()?;
    let implied_volatilities = df.column("implied_volatility")?.f64()?;
    let in_the_moneys = df.column("in_the_money")?.bool()?;
    let expirations = df.column("expiration")?.datetime()?.physical().clone();
    let option_types = df.column("option_type")?.str()?;
    let tickers = df.column("ticker")?.str()?;
    let dte_series = df.column("dte")?.cast(&DataType::Int32)?;
    let dtes = dte_series.i32()?;
    let collected_ats = df.column("collected_at")?.datetime()?.physical().clone();
    let deltas = df.column("delta")?.f64()?;
    let gammas = df.column("gamma")?.f64()?;
    let thetas = df.column("theta")?.f64()?;
    let vegas = df.column("vega")?.f64()?;
    let bs_prices = df.column("bs_price")?.f64()?;
    let prob_profit = df.column("prob_profit")?.f64()?;
    let hist_prob_profits = df.column("hist_prob_profit")?.f64()?;

    // Use a temp staging table + DuckDB Appender for bulk loading (much faster)
    conn.execute_batch(
        "CREATE OR REPLACE TEMP TABLE options_staging (
            contract_symbol     VARCHAR NOT NULL,
            strike              DOUBLE NOT NULL,
            stock_price         DOUBLE NOT NULL,
            last_price          DOUBLE NOT NULL,
            bid                 DOUBLE NOT NULL,
            ask                 DOUBLE NOT NULL,
            volume              BIGINT NOT NULL,
            open_interest       BIGINT NOT NULL,
            implied_volatility  DOUBLE NOT NULL,
            in_the_money        BOOLEAN NOT NULL,
            expiration          VARCHAR NOT NULL,
            option_type         VARCHAR NOT NULL,
            ticker              VARCHAR NOT NULL,
            dte                 INTEGER NOT NULL,
            collected_at        VARCHAR NOT NULL,
            delta               DOUBLE NOT NULL,
            gamma               DOUBLE NOT NULL,
            theta               DOUBLE NOT NULL,
            vega                DOUBLE NOT NULL,
            bs_price            DOUBLE NOT NULL,
            prob_profit         DOUBLE NOT NULL,
            hist_prob_profit    DOUBLE
        )",
    )?;

    {
        let mut appender = conn.appender("options_staging")?;
        for i in 0..df.height() {
            appender.append_row(params![
                contract_symbols.get(i).unwrap_or(""),
                strikes.get(i).unwrap_or(0.0),
                stock_prices.get(i).unwrap_or(0.0),
                last_prices.get(i).unwrap_or(0.0),
                bids.get(i).unwrap_or(0.0),
                asks.get(i).unwrap_or(0.0),
                volumes.get(i).unwrap_or(0) as i64,
                open_interests.get(i).unwrap_or(0) as i64,
                implied_volatilities.get(i).unwrap_or(0.0),
                in_the_moneys.get(i).unwrap_or(false),
                DateTime::<Utc>::from_timestamp_millis(expirations.get(i).unwrap_or(0))
                    .unwrap_or_default()
                    .to_rfc3339(),
                option_types.get(i).unwrap_or(""),
                tickers.get(i).unwrap_or(""),
                dtes.get(i).unwrap_or(0),
                DateTime::<Utc>::from_timestamp_millis(collected_ats.get(i).unwrap_or(0))
                    .unwrap_or_default()
                    .to_rfc3339(),
                deltas.get(i).unwrap_or(0.0),
                gammas.get(i).unwrap_or(0.0),
                thetas.get(i).unwrap_or(0.0),
                vegas.get(i).unwrap_or(0.0),
                bs_prices.get(i).unwrap_or(0.0),
                prob_profit.get(i).unwrap_or(0.0),
                hist_prob_profits.get(i),
            ])?;
        }
    }

    // Merge staging into main table, upserting duplicates
    conn.execute_batch(
        "INSERT OR REPLACE INTO options (
            contract_symbol, strike, stock_price, last_price, bid, ask, volume, open_interest,
            implied_volatility, in_the_money, expiration, option_type, ticker, dte,
            collected_at, delta, gamma, theta, vega, bs_price, prob_profit, hist_prob_profit
        )
        SELECT 
            contract_symbol, strike, stock_price, last_price, bid, ask, volume, open_interest,
            implied_volatility, in_the_money, expiration::TIMESTAMPTZ, option_type, ticker, dte,
            collected_at::TIMESTAMPTZ, delta, gamma, theta, vega, bs_price, prob_profit, hist_prob_profit
        FROM options_staging;
        DROP TABLE options_staging;",
    )?;

    Ok(())
}

pub fn calculate_historical_probs(
    options_df: DataFrame,
    candles_all_df: DataFrame,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let ticker_col = options_df.column("ticker")?.str()?;
    let dte_col = options_df.column("dte")?.i32()?;
    let strike_col = options_df.column("strike")?.f64()?;
    let type_col = options_df.column("option_type")?.str()?;
    let bid_col = options_df.column("bid")?.f64()?;
    let ask_col = options_df.column("ask")?.f64()?;
    let last_price_col = options_df.column("last_price")?.f64()?;
    let stock_price_col = options_df.column("stock_price")?.f64()?;

    let mut hist_probs: Vec<Option<f64>> = Vec::with_capacity(options_df.height());

    // Pre-extract closing prices for each ticker
    let tickers_df = candles_all_df.partition_by(["symbol"], true)?; // Changed from "ticker" to "symbol" to match candles_df schema
    let mut ticker_data: std::collections::HashMap<String, Vec<f64>> =
        std::collections::HashMap::new();
    for df in tickers_df {
        let name = df.column("symbol")?.str()?.get(0).unwrap_or("").to_string();
        let closes: Vec<f64> = df.column("close")?.f64()?.into_no_null_iter().collect();
        ticker_data.insert(name, closes);
    }

    // Cache distributions: (ticker, dte) -> returns_vec
    let mut dist_cache: std::collections::HashMap<(String, i32), Vec<f64>> =
        std::collections::HashMap::new();

    for i in 0..options_df.height() {
        let ticker = ticker_col.get(i).unwrap_or("");
        let dte = dte_col.get(i).unwrap_or(0);
        let strike = strike_col.get(i).unwrap_or(0.0);
        let opt_type = type_col.get(i).unwrap_or("");
        let current_price = stock_price_col.get(i).unwrap_or(0.0);

        let bid = bid_col.get(i).unwrap_or(0.0);
        let ask = ask_col.get(i).unwrap_or(0.0);
        let last = last_price_col.get(i).unwrap_or(0.0);

        let premium = if ask > 0.0 && bid > 0.0 {
            (bid + ask) / 2.0
        } else if ask > 0.0 {
            ask
        } else if bid > 0.0 {
            bid
        } else {
            last
        };
        let breakeven = if opt_type == "call" {
            strike + premium
        } else {
            strike - premium
        };

        if dte <= 0 || current_price <= 0.0 {
            let is_itm = if opt_type == "call" {
                current_price >= strike
            } else {
                current_price <= strike
            };
            hist_probs.push(Some(if is_itm { 1.0 } else { 0.0 }));
            continue;
        }

        let key = (ticker.to_string(), dte);
        if !dist_cache.contains_key(&key) {
            if let Some(closes_vec) = ticker_data.get(ticker) {
                if closes_vec.len() > dte as usize {
                    let rets: Vec<f64> = (dte as usize..closes_vec.len())
                        .map(|t| (closes_vec[t] / closes_vec[t - dte as usize]) - 1.0)
                        .collect();
                    dist_cache.insert(key.clone(), rets);
                }
            }
        }

        let returns = dist_cache.get(&key);
        match returns {
            Some(returns) if !returns.is_empty() => {
                let target_ret = (breakeven / current_price) - 1.0;
                let hits = if opt_type == "call" {
                    returns.iter().filter(|&&r| r >= target_ret).count()
                } else {
                    returns.iter().filter(|&&r| r <= target_ret).count()
                };
                hist_probs.push(Some(hits as f64 / returns.len() as f64));
            }
            _ => {
                hist_probs.push(None);
            }
        }
    }

    let mut df = options_df;
    df.with_column(Series::new("hist_prob_profit".into(), hist_probs))?;

    Ok(df)
}

pub fn get_latest_options_dates(
    conn: &Connection,
    symbols: &[String],
) -> Result<std::collections::HashMap<String, DateTime<Utc>>, Box<dyn std::error::Error>> {
    let placeholders = symbols
        .iter()
        .enumerate()
        .map(|(i, _)| format!("?{}", i + 1))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "SELECT ticker, strftime(MAX(collected_at)::TIMESTAMP, '%Y-%m-%dT%H:%M:%S+00:00') as latest
         FROM options
         WHERE ticker IN ({})
         GROUP BY ticker",
        placeholders
    );

    let mut stmt = conn.prepare(&sql)?;

    let params: Vec<Box<dyn duckdb::ToSql>> = symbols
        .iter()
        .map(|t| Box::new(t.clone()) as Box<dyn duckdb::ToSql>)
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

pub fn read_options(
    conn: &Connection,
    tickers: Vec<String>,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let placeholders = tickers
        .iter()
        .enumerate()
        .map(|(i, _)| format!("?{}", i + 1))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "SELECT 
        contract_symbol,
        strike,
        stock_price,
        last_price,
        bid,
        ask,
        volume,
        open_interest,
        implied_volatility,
        in_the_money,
        strftime(expiration::TIMESTAMP, '%Y-%m-%dT%H:%M:%S+00:00') as expiration,
        option_type,
        ticker,
        dte,
        strftime(collected_at::TIMESTAMP, '%Y-%m-%dT%H:%M:%S+00:00') as collected_at,
        delta,
        gamma,
        theta,
        vega,
        bs_price,
        prob_profit,
        hist_prob_profit
     FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY contract_symbol
                ORDER BY collected_at DESC
            ) as rn
        FROM options
        WHERE ticker IN ({})
    ) ranked
    WHERE rn = 1
    ORDER BY ticker, expiration, option_type, strike",
        placeholders
    );
    // let sql = format!(
    //     "SELECT
    //         contract_symbol,
    //         strike,
    //         last_price,
    //         bid,
    //         ask,
    //         volume,
    //         open_interest,
    //         implied_volatility,
    //         in_the_money,
    //         strftime(expiration::TIMESTAMP, '%Y-%m-%dT%H:%M:%S+00:00') as expiration,
    //         option_type,
    //         ticker,
    //         dte,
    //         strftime(collected_at::TIMESTAMP, '%Y-%m-%dT%H:%M:%S+00:00') as collected_at,
    //         delta,
    //         gamma,
    //         theta,
    //         vega,
    //         bs_price
    //      FROM options
    //      WHERE ticker IN ({})
    //      ORDER BY ticker, expiration, option_type, strike",
    //     placeholders
    // );

    let mut stmt = conn.prepare(&sql)?;

    let rows = stmt
        .query_map(duckdb::params_from_iter(tickers.iter()), |row| {
            Ok((
                row.get::<_, String>(0)?,       // contract_symbol
                row.get::<_, f64>(1)?,          // strike
                row.get::<_, f64>(2)?,          // stock price
                row.get::<_, f64>(3)?,          // last_price
                row.get::<_, f64>(4)?,          // bid
                row.get::<_, f64>(5)?,          // ask
                row.get::<_, i64>(6)?,          // volume
                row.get::<_, i64>(7)?,          // open_interest
                row.get::<_, f64>(8)?,          // implied_volatility
                row.get::<_, bool>(9)?,         // in_the_money
                row.get::<_, String>(10)?,      // expiration
                row.get::<_, String>(11)?,      // option_type
                row.get::<_, String>(12)?,      // ticker
                row.get::<_, i32>(13)?,         // dte
                row.get::<_, String>(14)?,      // collected_at
                row.get::<_, f64>(15)?,         // delta
                row.get::<_, f64>(16)?,         // gamma
                row.get::<_, f64>(17)?,         // theta
                row.get::<_, f64>(18)?,         // vega
                row.get::<_, f64>(19)?,         // bs_price
                row.get::<_, f64>(20)?,         // prob_profit
                row.get::<_, Option<f64>>(21)?, // hist_prob_profit
            ))
        })?
        .collect::<duckdb::Result<Vec<_>>>()?;

    // Build DataFrame from rows
    let mut contract_symbols: Vec<String> = Vec::new();
    let mut strikes: Vec<f64> = Vec::new();
    let mut stock_prices: Vec<f64> = Vec::new();
    let mut last_prices: Vec<f64> = Vec::new();
    let mut bids: Vec<f64> = Vec::new();
    let mut asks: Vec<f64> = Vec::new();
    let mut volumes: Vec<i64> = Vec::new();
    let mut open_interests: Vec<i64> = Vec::new();
    let mut implied_volatilities: Vec<f64> = Vec::new();
    let mut in_the_moneys: Vec<bool> = Vec::new();
    let mut expirations: Vec<i64> = Vec::new();
    let mut option_types: Vec<String> = Vec::new();
    let mut ticker_col: Vec<String> = Vec::new();
    let mut dtes: Vec<i32> = Vec::new();
    let mut collected_ats: Vec<i64> = Vec::new();
    let mut deltas: Vec<f64> = Vec::new();
    let mut gammas: Vec<f64> = Vec::new();
    let mut thetas: Vec<f64> = Vec::new();
    let mut vegas: Vec<f64> = Vec::new();
    let mut bs_prices: Vec<f64> = Vec::new();
    let mut prob_profits: Vec<f64> = Vec::new();
    let mut hist_prob_profits: Vec<Option<f64>> = Vec::new();

    for row in rows {
        let (
            contract_symbol,
            strike,
            stock_price,
            last_price,
            bid,
            ask,
            volume,
            open_interest,
            implied_volatility,
            in_the_money,
            expiration_str,
            option_type,
            ticker,
            dte,
            collected_at_str,
            delta,
            gamma,
            theta,
            vega,
            bs_price,
            prob_profit,
            hist_prob_profit,
        ) = row;

        contract_symbols.push(contract_symbol);
        strikes.push(strike);
        stock_prices.push(stock_price);
        last_prices.push(last_price);
        bids.push(bid);
        asks.push(ask);
        volumes.push(volume);
        open_interests.push(open_interest);
        implied_volatilities.push(implied_volatility);
        in_the_moneys.push(in_the_money);
        option_types.push(option_type);
        ticker_col.push(ticker);
        dtes.push(dte);
        deltas.push(delta);
        gammas.push(gamma);
        thetas.push(theta);
        vegas.push(vega);
        bs_prices.push(bs_price);
        prob_profits.push(prob_profit);
        hist_prob_profits.push(hist_prob_profit);

        // Parse timestamps to milliseconds
        if let Ok(dt) = DateTime::parse_from_rfc3339(&expiration_str) {
            expirations.push(dt.timestamp_millis());
        } else {
            expirations.push(0);
        }
        if let Ok(dt) = DateTime::parse_from_rfc3339(&collected_at_str) {
            collected_ats.push(dt.timestamp_millis());
        } else {
            collected_ats.push(0);
        }
    }

    let df = DataFrame::new(vec![
        Series::new("contract_symbol".into(), contract_symbols).into_column(),
        Series::new("strike".into(), strikes).into_column(),
        Series::new("stock_price".into(), stock_prices).into_column(),
        Series::new("last_price".into(), last_prices).into_column(),
        Series::new("bid".into(), bids).into_column(),
        Series::new("ask".into(), asks).into_column(),
        Series::new("volume".into(), volumes).into_column(),
        Series::new("open_interest".into(), open_interests).into_column(),
        Series::new("implied_volatility".into(), implied_volatilities).into_column(),
        Series::new("in_the_money".into(), in_the_moneys).into_column(),
        Series::new("expiration".into(), expirations).into_column(),
        Series::new("option_type".into(), option_types).into_column(),
        Series::new("ticker".into(), ticker_col).into_column(),
        Series::new("dte".into(), dtes).into_column(),
        Series::new("collected_at".into(), collected_ats).into_column(),
        Series::new("delta".into(), deltas).into_column(),
        Series::new("gamma".into(), gammas).into_column(),
        Series::new("theta".into(), thetas).into_column(),
        Series::new("vega".into(), vegas).into_column(),
        Series::new("bs_price".into(), bs_prices).into_column(),
        Series::new("prob_profit".into(), prob_profits).into_column(),
        Series::new("hist_prob_profit".into(), hist_prob_profits).into_column(),
    ])?;

    // Cast timestamp columns to UTC datetime
    let df_final = df
        .lazy()
        .with_column(col("expiration").cast(DataType::Datetime(
            TimeUnit::Milliseconds,
            TimeZone::opt_try_new(Some("UTC"))?,
        )))
        .with_column(col("collected_at").cast(DataType::Datetime(
            TimeUnit::Milliseconds,
            TimeZone::opt_try_new(Some("UTC"))?,
        )))
        .collect()?;

    Ok(df_final)
}
