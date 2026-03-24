use polars::prelude::*;
use std::f64::consts::{E, PI};

/// Standard normal probability density function
fn pdf(x: f64) -> f64 {
    (-0.5 * x * x).exp() / (2.0 * PI).sqrt()
}

/// Standard normal cumulative distribution function (approximation)
fn cdf(x: f64) -> f64 {
    let t = 1.0 / (1.0 + 0.2315419 * x.abs());
    let poly = t
        * (0.319381530
            + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))));
    let approx = 1.0 - pdf(x) * poly;
    if x >= 0.0 { approx } else { 1.0 - approx }
}

/// Calculate d1 and d2 for Black-Scholes
fn d1_d2(s: f64, k: f64, t: f64, r: f64, sigma: f64) -> (f64, f64) {
    let d1 = ((s / k).ln() + (r + 0.5 * sigma * sigma) * t) / (sigma * t.sqrt());
    let d2 = d1 - sigma * t.sqrt();
    (d1, d2)
}

#[derive(Debug)]
pub struct Greeks {
    pub delta: f64,
    pub gamma: f64,
    pub theta: f64,
    pub vega: f64,
    pub bs_price: f64,
}

/// Calculate Greeks for a single option
/// s     = current underlying price
/// k     = strike price
/// t     = time to expiration in years (DTE / 365.0)
/// r     = risk-free rate (e.g. 0.05 for 5%)
/// sigma = implied volatility (e.g. 0.30 for 30%)
/// is_call = true for call, false for put
pub fn calculate_greeks(s: f64, k: f64, t: f64, r: f64, sigma: f64, is_call: bool) -> Greeks {
    if t <= 0.0 || sigma <= 0.0 {
        return Greeks {
            delta: 0.0,
            gamma: 0.0,
            theta: 0.0,
            vega: 0.0,
            bs_price: 0.0,
        };
    }

    let (d1, d2) = d1_d2(s, k, t, r, sigma);
    let discount = E.powf(-r * t);

    let gamma = pdf(d1) / (s * sigma * t.sqrt());
    let vega = s * pdf(d1) * t.sqrt() / 100.0; // per 1% change in IV

    let (delta, theta, bs_price) = if is_call {
        let delta = cdf(d1);
        let theta =
            (-(s * pdf(d1) * sigma) / (2.0 * t.sqrt()) - r * k * discount * cdf(d2)) / 365.0;
        // Call price: S * N(d1) - K * e^(-rT) * N(d2)
        let bs_price = s * cdf(d1) - k * discount * cdf(d2);
        (delta, theta, bs_price)
    } else {
        let delta = cdf(d1) - 1.0;
        let theta =
            (-(s * pdf(d1) * sigma) / (2.0 * t.sqrt()) + r * k * discount * cdf(-d2)) / 365.0;
        // Put price: K * e^(-rT) * N(-d2) - S * N(-d1)
        let bs_price = k * discount * cdf(-d2) - s * cdf(-d1);
        (delta, theta, bs_price)
    };

    Greeks {
        delta,
        gamma,
        theta,
        vega,
        bs_price,
    }
}

/// Add delta, gamma, theta, vega, bs_price columns to an options DataFrame
/// Requires columns: strike, implied_volatility, dte, option_type
/// s = current underlying price
/// r = risk-free rate (default 0.05)
pub fn add_greeks_to_df(
    df: DataFrame,
    underlying_price: f64,
    risk_free_rate: f64,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let strikes = df.column("strike")?.f64()?;
    let ivs = df.column("implied_volatility")?.f64()?;
    let dtes = df.column("dte")?.cast(&DataType::Int32)?;
    let dtes = dtes.i32()?;
    let option_types = df.column("option_type")?.str()?;

    let mut deltas: Vec<f64> = Vec::new();
    let mut gammas: Vec<f64> = Vec::new();
    let mut thetas: Vec<f64> = Vec::new();
    let mut vegas: Vec<f64> = Vec::new();
    let mut bs_prices: Vec<f64> = Vec::new();

    for i in 0..df.height() {
        let strike = strikes.get(i).unwrap_or(0.0);
        let iv = ivs.get(i).unwrap_or(0.0);
        let dte = dtes.get(i).unwrap_or(0) as f64;
        let is_call = option_types.get(i).unwrap_or("") == "call";

        let t = dte / 365.0;
        let greeks = calculate_greeks(underlying_price, strike, t, risk_free_rate, iv, is_call);

        deltas.push(greeks.delta);
        gammas.push(greeks.gamma);
        thetas.push(greeks.theta);
        vegas.push(greeks.vega);
        bs_prices.push(greeks.bs_price);
    }

    let df_final = df
        .lazy()
        .with_columns([
            Series::new("delta".into(), deltas).lit(),
            Series::new("gamma".into(), gammas).lit(),
            Series::new("theta".into(), thetas).lit(),
            Series::new("vega".into(), vegas).lit(),
            Series::new("bs_price".into(), bs_prices).lit(),
        ])
        .collect()?;

    Ok(df_final)
}
