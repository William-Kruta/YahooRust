use chrono::Duration;
use clap::{Parser, Subcommand};
use duckdb::Connection;
use polars::prelude::DataFrame;
use yahoo_finance::{
    candles, db, earnings, info, options, screener, statements, statements2, technicals, utils,
};
use yfinance_rs::{Interval, Range, YfClient};

use crate::utils::{dataframe_to_json, map_timezone_abbr};

#[derive(Parser, Debug)]
#[command(author, version, about = "Yfinance Rust")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}
#[derive(Subcommand, Debug)]
enum Commands {
    Earnings {
        #[arg(short, long)]
        ticker: String,
    },
    GetCandles {
        #[arg(num_args = 1..)]
        symbols: Vec<String>,
        #[arg(short, long, value_parser=utils::parse_interval, default_value = "1d")]
        interval: Interval,
        #[arg(short, long, value_parser = utils::parse_range, default_value = "max")]
        range: Range,
        #[arg(long, default_value = "PST")]
        timezone: String,
        #[arg(long)]
        indicators: bool,
        #[arg(long, default_value = "false", action = clap::ArgAction::Set)]
        force_update: bool,
    },

    Options {
        #[arg(num_args = 1..)]
        symbols: Vec<String>,
        #[arg(long, default_value = "PST")]
        timezone: String,
    },

    OptionsScreener {
        #[arg(short, long, num_args = 1..)]
        symbols: Vec<String>,
        #[arg(long, default_value_t = 0)]
        min_dte: u32,
        #[arg(long, default_value_t = 7)]
        max_dte: u32,
        #[arg(long, default_value = "false", action = clap::ArgAction::Set)]
        in_the_money: bool,
        #[arg(long, default_value = "true", action = clap::ArgAction::Set)]
        long: bool,
        #[arg(long, default_value_t = 0.0)]
        min_collateral: f64,
        #[arg(long, default_value_t = 1_000_000.0)]
        max_collateral: f64,
        #[arg(long, default_value = "PST")]
        timezone: String,
        #[arg(long, action = clap::ArgAction::SetTrue)]
        force_update: bool,
        #[arg(long, action = clap::ArgAction::SetTrue)]
        update_candles: bool,
    },

    Statements {
        #[arg(num_args = 1..)]
        symbols: Vec<String>,
        #[arg(short, long)]
        annual: bool,
        #[arg(short, long)]
        quarterly: bool,
    },

    Info {
        #[arg(short, long)]
        ticker: String,
    },
}

async fn get_options_df(
    symbols: Vec<String>,
    timezone: String,
    client: &YfClient,
    candle_conn: &Connection,
    force_update: bool,
    update_candles: bool,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let tz = map_timezone_abbr(&timezone.as_str());
    let rr_candle = vec!["^TNX".to_string()];
    let df = candles::get_candles(
        &candle_conn,
        rr_candle,
        Interval::D1,
        Range::Max,
        Duration::hours(24),
        &client,
        &tz,
        update_candles,
    )
    .await?;
    let risk_free_rate = df.column("close")?.f64()?.last().unwrap_or(0.05) / 100.0;
    let df = options::get_options(
        &candle_conn,
        symbols,
        Duration::hours(24),
        &client,
        Some(risk_free_rate),
        options::ExpirationMode::Nearest,
        force_update,
        update_candles,
    )
    .await?;
    Ok(df)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let client = YfClient::default();
    let candle_conn = db::init_db()?;
    match cli.command {
        Commands::Earnings { ticker } => {
            let est =
                earnings::get_earnings_estimates(&candle_conn, &ticker, client.clone()).await?;
            let json = serde_json::to_string_pretty(&est)?;
            println!("{}", json);
        }

        Commands::GetCandles {
            symbols,
            interval,
            range,
            timezone,
            indicators,
            force_update,
        } => {
            let tz = map_timezone_abbr(&timezone.as_str());
            let df = candles::get_candles(
                &candle_conn,
                symbols,
                interval,
                range,
                Duration::hours(24),
                &client,
                &tz,
                force_update,
            )
            .await?;

            let df = if indicators {
                technicals::add_indicators(df)?
            } else {
                df
            };

            let json = dataframe_to_json(&df)?;
            println!("{}", json); // Python reads this from stdout
        }

        Commands::Options { symbols, timezone } => {
            let df = get_options_df(symbols, timezone, &client, &candle_conn, false, false).await?;
            let json = dataframe_to_json(&df)?;
            println!("{}", json);
        }

        Commands::OptionsScreener {
            symbols,
            min_dte,
            max_dte,
            in_the_money,
            long,
            min_collateral,
            max_collateral,
            timezone,
            force_update,
            update_candles,
        } => {
            let df = get_options_df(
                symbols,
                timezone,
                &client,
                &candle_conn,
                force_update,
                update_candles,
            )
            .await?;
            let filtered_df = screener::options_screener(
                df,
                min_dte,
                max_dte,
                in_the_money,
                long,
                min_collateral,
                max_collateral,
            )
            .await?;
            println!(
                "{}",
                filtered_df
                    .select([
                        "contract_symbol",
                        "side",
                        "strike",
                        "stock_price",
                        "bid",
                        "ask",
                        "dte",
                        "bs_price",
                        "prob_profit",
                        "hist_prob_profit",
                        "expected_return",
                    ])
                    .unwrap()
            );
        }

        Commands::Statements {
            symbols,
            annual: _,
            quarterly,
        } => {
            let is_annual = !quarterly;
            let df = statements2::get_statements(
                &candle_conn,
                symbols,
                if is_annual {
                    Duration::days(365)
                } else {
                    Duration::days(90)
                },
                "income",
                is_annual,
            )
            .await?;
            let df = statements::pivot_statements(df)?;
            let margins = statements2::calculate_margins(&df);
            //let json = dataframe_to_json(&df)?;
            println!("{:?}", margins);
        }

        Commands::Info { ticker } => {
            let details = info::get_company_info(&candle_conn, &ticker, &client, 30).await?;
            let json = serde_json::to_string_pretty(&details)?;
            println!("{}", json);
        }
    }
    Ok(())
}
