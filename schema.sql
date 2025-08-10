-- This schema is designed for PostgreSQL.
-- The tables and relationships are structured to handle the provided CSV data.

-- The `company_profile` table stores static information about the company.
-- 'ticker' is now used as the primary key.
CREATE TABLE IF NOT EXISTS company_profile (
    ticker VARCHAR(10) PRIMARY KEY,
    id VARCHAR(50) NULL,
    issue_share BIGINT NULL,
    history TEXT NULL,
    company_profile TEXT NULL,
    icb_name3 VARCHAR(50) NULL,
    icb_name2 VARCHAR(50) NULL,
    icb_name4 VARCHAR(50) NULL,
    financial_ratio_issue_share BIGINT NULL,
    charter_capital BIGINT NULL
);

-- The `daily_chart` table stores the daily trading data for the company.
-- A composite primary key of `ticker` and `time` is used.
CREATE TABLE IF NOT EXISTS daily_chart (
    time DATE NOT NULL,
    open FLOAT NULL,
    high FLOAT NULL,
    low FLOAT NULL,
    close FLOAT NULL,
    volume BIGINT NULL,
    ticker VARCHAR(10),
    PRIMARY KEY (ticker, time),
    FOREIGN KEY (ticker) REFERENCES company_profile(ticker)
);

-- The `balance_sheet` table contains quarterly balance sheet data.
-- A composite primary key of `ticker`, `year`, and `quarter` is used.
CREATE TABLE IF NOT EXISTS balance_sheet (
    cash FLOAT NULL,
    fixed_asset FLOAT NULL,
    asset FLOAT NULL,
    debt FLOAT NULL,
    equity FLOAT NULL,
    capital FLOAT NULL,
    central_bank_deposit FLOAT NULL,
    other_bank_deposit FLOAT NULL,
    other_bank_loan FLOAT NULL,
    stock_invest FLOAT NULL,
    customer_loan FLOAT NULL,
    bad_loan FLOAT NULL,
    provision FLOAT NULL,
    net_customer_loan FLOAT NULL,
    other_asset FLOAT NULL,
    other_bank_credit FLOAT NULL,
    owe_other_bank FLOAT NULL,
    owe_central_bank FLOAT NULL,
    valuable_paper FLOAT NULL,
    payable_interest FLOAT NULL,
    receivable_interest FLOAT NULL,
    deposit FLOAT NULL,
    other_debt FLOAT NULL,
    fund FLOAT NULL,
    un_distributed_income FLOAT NULL,
    minor_share_holder_profit FLOAT NULL,
    payable FLOAT NULL,
    ticker VARCHAR(10),
    year INT NOT NULL,
    quarter INT NOT NULL,
    PRIMARY KEY (ticker, year, quarter),
    FOREIGN KEY (ticker) REFERENCES company_profile(ticker)
);

-- The `cash_flow` table stores quarterly cash flow data.
CREATE TABLE IF NOT EXISTS  cash_flow (
    invest_cost FLOAT NULL,
    from_invest FLOAT NULL,
    from_financial FLOAT NULL,
    from_sale FLOAT NULL,
    free_cash_flow FLOAT NULL,
    ticker VARCHAR(10),
    year INT NOT NULL,
    quarter INT NOT NULL,
    PRIMARY KEY (ticker, year, quarter),
    FOREIGN KEY (ticker) REFERENCES company_profile(ticker)
);

-- The `income_statement` table holds quarterly income statement data.
CREATE TABLE IF NOT EXISTS income_statement (
    revenue FLOAT NULL,
    year_revenue_growth FLOAT NULL,
    operation_expense FLOAT NULL,
    operation_profit FLOAT NULL,
    year_operation_profit_growth FLOAT NULL,
    pre_tax_profit FLOAT NULL,
    post_tax_profit FLOAT NULL,
    share_holder_income FLOAT NULL,
    year_share_holder_income_growth FLOAT NULL,
    invest_profit FLOAT NULL,
    service_profit FLOAT NULL,
    other_profit FLOAT NULL,
    provision_expense FLOAT NULL,
    operation_income FLOAT NULL,
    ticker VARCHAR(10),
    year INT NOT NULL,
    quarter INT NOT NULL,
    quarter_revenue_growth FLOAT NULL,
    quarter_operation_profit_growth FLOAT NULL,
    quarter_share_holder_income_growth FLOAT NULL,
    PRIMARY KEY (ticker, year, quarter),
    FOREIGN KEY (ticker) REFERENCES company_profile(ticker)
);

-- The `ratios` table stores quarterly financial ratio data.
CREATE TABLE IF NOT EXISTS ratios (
    price_to_earning FLOAT NULL,
    price_to_book FLOAT NULL,
    dividend FLOAT NULL,
    roe FLOAT NULL,
    roa FLOAT NULL,
    earning_per_share FLOAT NULL,
    book_value_per_share FLOAT NULL,
    interest_margin FLOAT NULL,
    non_interest_on_toi FLOAT NULL,
    bad_debt_percentage FLOAT NULL,
    provision_on_bad_debt FLOAT NULL,
    cost_of_financing FLOAT NULL,
    equity_on_total_asset FLOAT NULL,
    equity_on_loan FLOAT NULL,
    cost_to_income FLOAT NULL,
    equity_on_liability FLOAT NULL,
    eps_change FLOAT NULL,
    asset_on_equity FLOAT NULL,
    pre_provision_on_toi FLOAT NULL,
    post_tax_on_toi FLOAT NULL,
    loan_on_earn_asset FLOAT NULL,
    loan_on_asset FLOAT NULL,
    loan_on_deposit FLOAT NULL,
    deposit_on_earn_asset FLOAT NULL,
    bad_debt_on_asset FLOAT NULL,
    liquidity_on_liability FLOAT NULL,
    payable_on_equity FLOAT NULL,
    cancel_debt FLOAT NULL,
    book_value_per_share_change FLOAT NULL,
    credit_growth FLOAT NULL,
    ticker VARCHAR(10),
    year INT NOT NULL,
    quarter INT NOT NULL,
    PRIMARY KEY (ticker, year, quarter),
    FOREIGN KEY (ticker) REFERENCES company_profile(ticker)
);
