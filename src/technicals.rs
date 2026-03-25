use polars::prelude::*;
use polars::series::ops::NullBehavior;
pub fn add_indicators(df: DataFrame) -> PolarsResult<DataFrame> {
    let lf = df.lazy();

    // EMA helper (Standard EMA uses alpha = 2 / (window + 1))
    // Polars ewm_mean uses span, which is window size, so alpha = 2 / (span + 1). Perfect.

    let lf = lf
        // SMA
        .with_column(
            col("close")
                .rolling_mean(RollingOptionsFixedWindow {
                    window_size: 20,
                    ..Default::default()
                })
                .alias("sma_20"),
        )
        .with_column(
            col("close")
                .rolling_mean(RollingOptionsFixedWindow {
                    window_size: 50,
                    ..Default::default()
                })
                .alias("sma_50"),
        )
        .with_column(
            col("close")
                .rolling_mean(RollingOptionsFixedWindow {
                    window_size: 200,
                    ..Default::default()
                })
                .alias("sma_200"),
        )
        // EMA
        .with_column(
            col("close")
                .ewm_mean(EWMOptions {
                    alpha: 2.0 / (10.0 + 1.0),
                    adjust: true,
                    bias: false,
                    min_periods: 1,
                    ignore_nulls: false,
                })
                .alias("ema_10"),
        )
        .with_column(
            col("close")
                .ewm_mean(EWMOptions {
                    alpha: 2.0 / (20.0 + 1.0),
                    adjust: true,
                    bias: false,
                    min_periods: 1,
                    ignore_nulls: false,
                })
                .alias("ema_20"),
        )
        // Bollinger Bands
        .with_column(
            col("close")
                .rolling_std(RollingOptionsFixedWindow {
                    window_size: 20,
                    ..Default::default()
                })
                .alias("std_20"),
        )
        .with_columns([
            (col("sma_20") + (col("std_20") * lit(2.0))).alias("bb_upper"),
            (col("sma_20") - (col("std_20") * lit(2.0))).alias("bb_lower"),
        ])
        // MACD
        // 1. 12-period EMA
        .with_column(
            col("close")
                .ewm_mean(EWMOptions {
                    alpha: 2.0 / (12.0 + 1.0),
                    adjust: true,
                    ..Default::default()
                })
                .alias("ema_12"),
        )
        // 2. 26-period EMA
        .with_column(
            col("close")
                .ewm_mean(EWMOptions {
                    alpha: 2.0 / (26.0 + 1.0),
                    adjust: true,
                    ..Default::default()
                })
                .alias("ema_26"),
        )
        // 3. MACD line = EMA12 - EMA26
        .with_column((col("ema_12") - col("ema_26")).alias("macd"))
        // 4. Signal line = 9-period EMA of MACD line
        .with_column(
            col("macd")
                .ewm_mean(EWMOptions {
                    alpha: 2.0 / (9.0 + 1.0),
                    adjust: true,
                    ..Default::default()
                })
                .alias("macd_signal"),
        )
        // 5. Histogram = MACD - Signal
        .with_column((col("macd") - col("macd_signal")).alias("macd_histogram"))
        // RSI
        .with_column(
            col("close")
                .diff(lit(1), NullBehavior::Ignore)
                .alias("diff"),
        )
        .with_column(
            when(col("diff").gt(0.0))
                .then(col("diff"))
                .otherwise(lit(0.0))
                .alias("gain"),
        )
        .with_column(
            when(col("diff").lt(lit(0.0)))
                .then(col("diff") * lit(-1))
                .otherwise(lit(0.0))
                .alias("loss"),
        )
        .with_column(
            col("gain")
                .ewm_mean(EWMOptions {
                    alpha: 2.0 / (14.0 + 1.0),
                    adjust: true,
                    ..Default::default()
                })
                .alias("avg_gain"),
        )
        .with_column(
            col("loss")
                .ewm_mean(EWMOptions {
                    alpha: 2.0 / (14.0 + 1.0),
                    adjust: true,
                    ..Default::default()
                })
                .alias("avg_loss"),
        )
        .with_column(
            (lit(100.0) - (lit(100.0) / (lit(1.0) + (col("avg_gain") / col("avg_loss")))))
                .alias("rsi"),
        )
        // ATR
        .with_column(col("close").shift(lit(1)).alias("prev_close"))
        .with_column((col("high") - col("low")).alias("tr1"))
        .with_column(
            when((col("high") - col("prev_close")).lt(lit(0.0)))
                .then((col("high") - col("prev_close")) * lit(-1))
                .otherwise(col("high") - col("prev_close"))
                .alias("tr2"),
        )
        .with_column(
            when((col("low") - col("prev_close")).lt(lit(0.0)))
                .then((col("low") - col("prev_close")) * lit(-1))
                .otherwise(col("low") - col("prev_close"))
                .alias("tr3"),
        )
        .with_column(
            // Manual max because vertical max of 3 columns is cleaner with a helper or concat
            when(col("tr1").gt(col("tr2")))
                .then(
                    when(col("tr1").gt(col("tr3")))
                        .then(col("tr1"))
                        .otherwise(col("tr3")),
                )
                .otherwise(
                    when(col("tr2").gt(col("tr3")))
                        .then(col("tr2"))
                        .otherwise(col("tr3")),
                )
                .alias("tr"),
        )
        .with_column(
            col("tr")
                .rolling_mean(RollingOptionsFixedWindow {
                    window_size: 14,
                    ..Default::default()
                })
                .alias("atr"),
        )
        // Clean up temporary columns
        .select([
            col("date"),
            col("symbol"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            col("sma_20"),
            col("sma_50"),
            col("sma_200"),
            col("ema_12"),
            col("ema_26"),
            col("macd"),
            col("macd_signal"),
            col("macd_histogram"),
            col("rsi"),
            col("bb_upper"),
            col("bb_middle"),
            col("bb_lower"),
            col("atr"),
        ]);

    lf.collect()
}
