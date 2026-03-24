use crate::greeks::add_greeks_to_df;
use chrono::{DateTime, Utc};
use duckdb::{Connection, Result, params};
use paft::prelude::OptionContract as PaftOptionContract;
use polars::prelude::*;
use tokio::task::JoinSet;
use yfinance_rs::core::conversions::money_to_f64;
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
            .with_columns([lit(collected_at_ms)
                .cast(DataType::Datetime(
                    TimeUnit::Milliseconds,
                    TimeZone::opt_try_new(Some("UTC"))?,
                ))
                .alias("collected_at")])
            .with_column(
                (col("expiration").cast(DataType::Date) - col("collected_at").cast(DataType::Date))
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

    for symbol in symbols {
        let client = client.clone();
        set.spawn(async move {
            let obj = Ticker::new(&client, &symbol);

            let expirations = obj.options().await?;
            let nearest = match expirations.first() {
                Some(e) => *e,
                None => {
                    return Ok::<Option<DataFrame>, Box<dyn std::error::Error + Send + Sync>>(None);
                }
            };

            let chain = obj.option_chain(Some(nearest)).await?;

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

            let fi = obj.fast_info().await?;
            let underlying_price = fi
                .last
                .as_ref()
                .map(money_to_f64)
                .or_else(|| fi.previous_close.as_ref().map(money_to_f64))
                .unwrap_or(0.0);

            let combined_df = concat([calls_df.lazy(), puts_df.lazy()], UnionArgs::default())?
                .with_columns([lit(collected_at_ms)
                    .cast(DataType::Datetime(
                        TimeUnit::Milliseconds,
                        TimeZone::opt_try_new(Some("UTC"))?,
                    ))
                    .alias("collected_at")])
                .with_column(
                    (col("expiration").cast(DataType::Date)
                        - col("collected_at").cast(DataType::Date))
                    .alias("dte"),
                )
                .collect()?;

            let combined_df = add_greeks_to_df(combined_df, underlying_price, risk_free_rate)
                .map_err(|e| format!("{}", e))?;

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
            Ok(None) => println!("No expirations found for symbol, skipping."),
            Err(e) => println!("Error downloading chain: {}", e),
        }
    }

    // Concatenate all symbol DataFrames into one
    let final_df = concat(all_dfs, UnionArgs::default())?.collect()?;

    Ok(final_df)
}

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
pub fn create_options_table(conn: &Connection) -> duckdb::Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS options (
            contract_symbol    VARCHAR        NOT NULL,
            strike             DOUBLE         NOT NULL,
            last_price         DOUBLE         NOT NULL,
            bid                DOUBLE         NOT NULL,
            ask                DOUBLE         NOT NULL,
            volume             UBIGINT        NOT NULL,
            open_interest      UBIGINT        NOT NULL,
            implied_volatility DOUBLE         NOT NULL,
            in_the_money       BOOLEAN        NOT NULL,
            expiration         TIMESTAMPTZ    NOT NULL,
            option_type        VARCHAR        NOT NULL,
            ticker             VARCHAR        NOT NULL,
            dte                INTEGER        NOT NULL,
            collected_at       TIMESTAMPTZ    NOT NULL,
            delta              DOUBLE         NOT NULL,
            gamma              DOUBLE         NOT NULL,
            theta              DOUBLE         NOT NULL,
            vega               DOUBLE         NOT NULL,
            bs_price           DOUBLE         NOT NULL,
            PRIMARY KEY (contract_symbol, collected_at)
        );
        ",
    )?;

    Ok(())
}

pub fn insert_options(conn: &Connection, df: &DataFrame) -> Result<(), Box<dyn std::error::Error>> {
    let contract_symbols = df.column("contract_symbol")?.str()?;
    let strikes = df.column("strike")?.f64()?;
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
    let dtes = df.column("dte")?.i32()?;
    let collected_ats = df.column("collected_at")?.datetime()?.physical().clone();
    let deltas = df.column("delta")?.f64()?;
    let gammas = df.column("gamma")?.f64()?;
    let thetas = df.column("theta")?.f64()?;
    let vegas = df.column("vega")?.f64()?;
    let bs_prices = df.column("bs_price")?.f64()?;

    conn.execute_batch("BEGIN;")?;

    let mut stmt = conn.prepare(
        "INSERT OR IGNORE INTO options (
            contract_symbol,
            strike,
            last_price,
            bid,
            ask,
            volume,
            open_interest,
            implied_volatility,
            in_the_money,
            expiration,
            option_type,
            ticker,
            dte,
            collected_at,
            delta,
            gamma,
            theta,
            vega,
            bs_price
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)",
    )?;

    for i in 0..df.height() {
        stmt.execute(params![
            contract_symbols.get(i).unwrap_or(""),
            strikes.get(i).unwrap_or(0.0),
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
        ])?;
    }

    conn.execute_batch("COMMIT;")?;

    Ok(())
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
            bs_price
         FROM options
         WHERE ticker IN ({})
         ORDER BY ticker, expiration, option_type, strike",
        placeholders
    );

    let mut stmt = conn.prepare(&sql)?;

    let rows = stmt
        .query_map(duckdb::params_from_iter(tickers.iter()), |row| {
            Ok((
                row.get::<_, String>(0)?,  // contract_symbol
                row.get::<_, f64>(1)?,     // strike
                row.get::<_, f64>(2)?,     // last_price
                row.get::<_, f64>(3)?,     // bid
                row.get::<_, f64>(4)?,     // ask
                row.get::<_, i64>(5)?,     // volume
                row.get::<_, i64>(6)?,     // open_interest
                row.get::<_, f64>(7)?,     // implied_volatility
                row.get::<_, bool>(8)?,    // in_the_money
                row.get::<_, String>(9)?,  // expiration
                row.get::<_, String>(10)?, // option_type
                row.get::<_, String>(11)?, // ticker
                row.get::<_, i32>(12)?,    // dte
                row.get::<_, String>(13)?, // collected_at
                row.get::<_, f64>(14)?,    // delta
                row.get::<_, f64>(15)?,    // gamma
                row.get::<_, f64>(16)?,    // theta
                row.get::<_, f64>(17)?,    // vega
                row.get::<_, f64>(18)?,    // bs_price
            ))
        })?
        .collect::<duckdb::Result<Vec<_>>>()?;

    // Build DataFrame from rows
    let mut contract_symbols: Vec<String> = Vec::new();
    let mut strikes: Vec<f64> = Vec::new();
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

    for row in rows {
        let (
            contract_symbol,
            strike,
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
        ) = row;

        contract_symbols.push(contract_symbol);
        strikes.push(strike);
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
