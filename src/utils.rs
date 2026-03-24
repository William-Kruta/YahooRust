use polars::io::json::{JsonFormat, JsonWriter};
use polars::prelude::DataFrame;
use polars::prelude::SerWriter;
use yfinance_rs::{Interval, Range};
/// Maps a string from the CLI to a yfinance_rs Interval.
/// Returns a Result so we can easily handle invalid user input.
pub fn parse_interval(cli_input: &str) -> Result<Interval, String> {
    // 1. Clean the input: remove whitespace and make it lowercase
    // This ensures "1D", "1d", and " 1d " all work perfectly.
    let cleaned_input = cli_input.trim().to_lowercase();

    // 2. Match the cleaned string against known patterns
    match cleaned_input.as_str() {
        "1m" => Ok(Interval::I1m),
        "2m" => Ok(Interval::I2m),
        "5m" => Ok(Interval::I5m),
        "15m" => Ok(Interval::I15m),
        "30m" => Ok(Interval::I30m),

        // You can map multiple strings to the same Interval!
        "60m" | "1h" => Ok(Interval::I1h),
        "90m" => Ok(Interval::I90m),

        "1d" => Ok(Interval::D1),
        "5d" => Ok(Interval::D5),

        "1wk" | "1w" => Ok(Interval::W1),
        "1mo" => Ok(Interval::M1),
        "3mo" => Ok(Interval::M3),

        // 3. The Catch-All (_): If they typed something weird, return an Error
        _ => Err(format!(
            "'{}' is not a valid Yahoo Finance interval.",
            cli_input
        )),
    }
}

pub fn parse_range(cli_input: &str) -> Result<Range, String> {
    let cleaned_input = cli_input.trim().to_lowercase();

    match cleaned_input.as_str() {
        "1d" => Ok(Range::D1),
        "5d" => Ok(Range::D5),
        "1mo" => Ok(Range::M1),
        "3mo" => Ok(Range::M3),
        "6mo" => Ok(Range::M6),
        "1y" => Ok(Range::Y1),
        "5y" => Ok(Range::Y5),
        "10y" => Ok(Range::Y10),
        "ytd" => Ok(Range::Ytd),
        "max" => Ok(Range::Max),
        _ => Err(format!(
            "'{}' is not a valid Yahoo Finance interval.",
            cli_input
        )),
    }
}

pub fn map_timezone_abbr(abbr: &str) -> String {
    match abbr.to_uppercase().as_str() {
        "PST" | "PDT" | "PT" => "America/Los_Angeles".to_string(),
        "EST" | "EDT" | "ET" => "America/New_York".to_string(),
        "CST" | "CDT" | "CT" => "America/Chicago".to_string(),
        "MST" | "MDT" | "MT" => "America/Denver".to_string(),
        "UTC" => "UTC".to_string(),
        "GMT" | "BST" => "Europe/London".to_string(),
        "HKT" => "Asia/Hong_Kong".to_string(),
        "JST" => "Asia/Tokyo".to_string(),
        // Default to UTC if we don't recognize the abbreviation
        _ => "UTC".to_string(),
    }
}

pub fn dataframe_to_json(df: &DataFrame) -> Result<String, Box<dyn std::error::Error>> {
    let mut buf = Vec::new();
    JsonWriter::new(&mut buf)
        .with_json_format(JsonFormat::Json)
        .finish(&mut df.clone())?;
    Ok(String::from_utf8(buf)?)
}
