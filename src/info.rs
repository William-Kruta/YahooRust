use chrono::{DateTime, Utc};
use duckdb::{Connection, Result as DbResult, params};
use serde::{Deserialize, Serialize};
use std::error::Error;
use yfinance_rs::YfClient;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CompanyMetadata {
    pub symbol: String,
    pub name: String,
    pub isin: Option<String>,
    pub sector: Option<String>,
    pub industry: Option<String>,
    pub website: Option<String>,
    pub business_summary: Option<String>,
    pub first_trading_day: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

pub async fn download_company_info(
    ticker: &str,
    client: &YfClient,
) -> Result<CompanyMetadata, Box<dyn Error>> {
    // We only need the profile for static data (sector, industry, name, isin, etc.)
    let profile_res = yfinance_rs::profile::load_profile(client, ticker).await;

    let (name, isin, sector, industry, website, summary) = match profile_res {
        Ok(yfinance_rs::profile::Profile::Company(ref c)) => (
            c.name.clone(),
            c.isin.as_ref().map(|i| i.to_string()),
            c.sector.clone(),
            c.industry.clone(),
            c.website.clone(),
            c.summary.clone(),
        ),
        Ok(yfinance_rs::profile::Profile::Fund(ref f)) => (
            f.name.clone(),
            f.isin.as_ref().map(|i| i.to_string()),
            Some("Financial Services".to_string()),
            Some(format!("{:?}", f.kind)),
            None,
            None,
        ),
        Err(_) => (
            ticker.to_string(), // Default Name to Ticker
            None,
            if ticker.starts_with('^') {
                Some("Index".to_string())
            } else {
                None
            },
            None,
            None,
            None,
        ),
    };

    Ok(CompanyMetadata {
        symbol: ticker.to_string(),
        name,
        isin,
        sector,
        industry,
        website,
        business_summary: summary,
        first_trading_day: None, // Will be populated by candles module if it fetches full history
        updated_at: Utc::now(),
    })
}

pub fn insert_company_info(conn: &Connection, info: &CompanyMetadata) -> DbResult<()> {
    conn.execute(
        "INSERT OR REPLACE INTO company_info (
            symbol, name, isin, sector, industry, website, business_summary, first_trading_day, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            info.symbol,
            info.name,
            info.isin,
            info.sector,
            info.industry,
            info.website,
            info.business_summary,
            info.first_trading_day.map(|dt| dt.to_rfc3339()),
            info.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

pub fn read_company_info(conn: &Connection, ticker: &str) -> DbResult<Option<CompanyMetadata>> {
    let mut stmt = conn.prepare(
        "SELECT symbol, name, isin, sector, industry, website, business_summary, 
                strftime(first_trading_day::TIMESTAMP, '%Y-%m-%dT%H:%M:%S+00:00'),
                strftime(updated_at::TIMESTAMP, '%Y-%m-%dT%H:%M:%S+00:00')
         FROM company_info WHERE symbol = ?1",
    )?;

    let mut rows = stmt.query_map(params![ticker], |row| {
        let first_day_str: Option<String> = row.get(7)?;
        let first_trading_day = first_day_str.and_then(|s| {
            DateTime::parse_from_rfc3339(&s)
                .map(|dt| dt.with_timezone(&Utc))
                .ok()
        });

        let updated_at_str: String = row.get(8)?;
        let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        Ok(CompanyMetadata {
            symbol: row.get(0)?,
            name: row.get(1)?,
            isin: row.get(2)?,
            sector: row.get(3)?,
            industry: row.get(4)?,
            website: row.get(5)?,
            business_summary: row.get(6)?,
            first_trading_day,
            updated_at,
        })
    })?;

    if let Some(res) = rows.next() {
        Ok(Some(res?))
    } else {
        Ok(None)
    }
}

pub async fn get_company_info(
    conn: &Connection,
    ticker: &str,
    client: &YfClient,
    stale_threshold_days: i64,
) -> Result<CompanyMetadata, Box<dyn Error>> {
    if let Some(existing) = read_company_info(conn, ticker)? {
        let age = Utc::now() - existing.updated_at;
        if age.num_days() < stale_threshold_days {
            println!("{} info found in DB ({} days old).", ticker, age.num_days());
            return Ok(existing);
        }
    }

    println!("{} info not found or stale in DB, will download.", ticker);
    let downloaded = download_company_info(ticker, client).await?;
    insert_company_info(conn, &downloaded)?;

    Ok(downloaded)
}

pub fn update_first_trading_day(
    conn: &Connection,
    ticker: &str,
    first_day: DateTime<Utc>,
) -> DbResult<()> {
    conn.execute(
        "UPDATE company_info SET first_trading_day = ?1 WHERE symbol = ?2",
        params![first_day.to_rfc3339(), ticker],
    )?;
    Ok(())
}

pub fn get_first_trading_day(conn: &Connection, ticker: &str) -> DbResult<Option<DateTime<Utc>>> {
    let mut stmt = conn.prepare("SELECT strftime(first_trading_day::TIMESTAMP, '%Y-%m-%dT%H:%M:%S+00:00') FROM company_info WHERE symbol = ?1")?;
    let mut rows = stmt.query_map(params![ticker], |row| {
        let date_str: Option<String> = row.get(0)?;
        Ok(date_str.and_then(|s| {
            DateTime::parse_from_rfc3339(&s)
                .map(|dt| dt.with_timezone(&Utc))
                .ok()
        }))
    })?;

    if let Some(res) = rows.next() {
        Ok(res?)
    } else {
        Ok(None)
    }
}
