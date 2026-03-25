use duckdb::{Connection, Result};

pub fn init_db() -> Result<Connection> {
    let conn = Connection::open("rust_candles.db")?;

    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS candles (
            date      TIMESTAMPTZ NOT NULL,
            ticker    VARCHAR     NOT NULL,
            interval  VARCHAR     NOT NULL,
            open      DOUBLE      NOT NULL,
            high      DOUBLE      NOT NULL,
            low       DOUBLE      NOT NULL,
            close     DOUBLE      NOT NULL,
            volume    DOUBLE      NOT NULL,
            PRIMARY KEY (date, ticker)
        );
        
        CREATE TABLE IF NOT EXISTS earnings_estimates (
            ticker   VARCHAR NOT NULL,
            date     VARCHAR NOT NULL,
            estimate DOUBLE NOT NULL,
            actual   DOUBLE,
            PRIMARY KEY (ticker, date)
        );

        CREATE TABLE IF NOT EXISTS options (
            contract_symbol    VARCHAR        NOT NULL,
            strike             DOUBLE         NOT NULL,
            stock_price        DOUBLE         NOT NULL,
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
            prob_profit        DOUBLE         NOT NULL,
            hist_prob_profit   DOUBLE,
            PRIMARY KEY (contract_symbol, collected_at)
        );

        CREATE TABLE IF NOT EXISTS statements (
            date            TIMESTAMPTZ NOT NULL,
            ticker          VARCHAR     NOT NULL,
            label           VARCHAR     NOT NULL,
            value           DOUBLE      NOT NULL,
            statement_type  VARCHAR     NOT NULL,
            period          VARCHAR     NOT NULL DEFAULT 'A',
            PRIMARY KEY (date, ticker, label, statement_type, period)
        );

        CREATE TABLE IF NOT EXISTS company_info (
            symbol             VARCHAR PRIMARY KEY,
            name               VARCHAR NOT NULL,
            isin               VARCHAR,
            sector             VARCHAR,
            industry           VARCHAR,
            website            VARCHAR,
            business_summary   TEXT,
            first_trading_day  TIMESTAMPTZ,
            updated_at         TIMESTAMPTZ NOT NULL
        );
        ",
    )?;

    // Migration: Add first_trading_day if it doesn't exist
    let _ = conn.execute(
        "ALTER TABLE company_info ADD COLUMN first_trading_day TIMESTAMPTZ",
        [],
    );

    // Migration: Add hist_prob_profit if it doesn't exist
    let _ = conn.execute("ALTER TABLE options ADD COLUMN hist_prob_profit DOUBLE", []);

    // Migration: Add period to statements if it doesn't exist
    let _ = conn.execute(
        "ALTER TABLE statements ADD COLUMN period VARCHAR DEFAULT 'A'",
        [],
    );

    Ok(conn)
}
