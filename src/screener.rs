use polars::prelude::*;
pub async fn options_screener(
    options_df: DataFrame,
    min_dte: u32,
    max_dte: u32,
    in_the_money: bool,
    long: bool,
    min_collateral: f64,
    max_collateral: f64,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    println!("Options DF: {}", options_df);
    let mut lazy_df = options_df.lazy();
    let side = if long { "long" } else { "short" };
    lazy_df = lazy_df.with_column(lit(side).alias("side"));

    if !long {
        lazy_df = lazy_df.with_column((lit(1.0) - col("prob_profit")).alias("prob_profit"));
    }

    let filtered_df = lazy_df
        .with_columns([(col("strike") * lit(100)).alias("collateral")])
        .filter(
            col("dte")
                .gt_eq(lit(min_dte))
                .and(col("dte").lt_eq(lit(max_dte))),
        )
        .filter(col("in_the_money").eq(lit(in_the_money)))
        .filter(col("bid").gt(lit(0.0)).or(col("ask").gt(lit(0.0))))
        .filter(
            col("collateral")
                .gt_eq(lit(min_collateral))
                .and(col("collateral").lt_eq(lit(max_collateral))),
        )
        // Premium: prefer bid/ask midpoint, fall back to whichever exists, then last_price
        .with_columns([(when(col("bid").gt(lit(0.0)).and(col("ask").gt(lit(0.0))))
            .then((col("bid") + col("ask")) / lit(2.0))
            .when(col("ask").gt(lit(0.0)))
            .then(col("ask"))
            .when(col("bid").gt(lit(0.0)))
            .then(col("bid"))
            .otherwise(col("last_price")))
        .alias("premium")])
        .with_columns([
            // ROC: premium per share / strike (since collateral = strike * 100)
            (col("premium") / col("strike")).alias("roc"),
            // Annualized ROC
            (col("premium") / col("strike") * lit(365.0) / col("dte")).alias("annualized_roc"),
        ])
        .with_columns([
            // Max loss per share:
            //   Short put:  strike - premium (assigned at strike, keep premium)
            //   Short call: stock_price - premium (covered call, lose upside but shares go to 0)
            //   Long:       premium paid
            (when(lit(long))
                .then(col("premium"))
                .when(col("option_type").eq(lit("put")))
                .then(col("strike") - col("premium"))
                .otherwise(col("stock_price") - col("premium")))
            .alias("max_loss_per_share"),
        ])
        .with_columns([
            // Expected return per share:
            //   premium * prob_profit - max_loss * (1 - prob_profit)
            // Normalized by strike to make it comparable across different priced underlyings
            ((col("premium") * col("prob_profit")
                - col("max_loss_per_share") * (lit(1.0) - col("prob_profit")))
                / col("strike"))
            .alias("expected_return"),
        ])
        .filter(col("premium").gt(lit(0.10)))
        .filter(col("roc").gt(lit(0.005))) // at least 0.5% return on collateral
        .sort_by_exprs(
            [col("expected_return")],
            SortMultipleOptions::default().with_order_descending(true),
        )
        .collect()?;

    Ok(filtered_df)
}
