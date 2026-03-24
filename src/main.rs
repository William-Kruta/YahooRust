mod candles;
mod greeks;
mod options;
mod utils;

use chrono::Duration;
use clap::{Parser, Subcommand};
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
    /// Extracts frames from a video using FFmpeg
    GetCandles {
        #[arg(short, long, num_args = 1..)]
        symbols: Vec<String>,
        #[arg(short, long, value_parser=utils::parse_interval, default_value = "1d")]
        interval: Interval,
        #[arg(short, long, value_parser = utils::parse_range, default_value = "max")]
        range: Range,
        #[arg(long, default_value = "PST")]
        timezone: String,
    },

    Options {
        #[arg(short, long, num_args = 1..)]
        symbols: Vec<String>,
        #[arg(long, default_value = "PST")]
        timezone: String,
    },
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let client = YfClient::default();
    let candle_conn = candles::create_db()?;
    match cli.command {
        Commands::GetCandles {
            symbols,
            interval,
            range,
            timezone,
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
            )
            .await?;
            let json = dataframe_to_json(&df)?;
            println!("{}", json); // Python reads this from stdout
        }

        Commands::Options { symbols, timezone } => {
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
            )
            .await?;
            let risk_free_rate = df.column("close")?.f64()?.last().unwrap_or(0.05) / 100.0;
            let df =
                options::download_chain_chunked(&client, symbols, Some(risk_free_rate)).await?;
            let json = dataframe_to_json(&df)?;
            println!("{}", json);
        }
    }
    Ok(())
}
