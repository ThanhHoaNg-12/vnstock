
-- The `companies` table stores static information about each company.
-- 'ticker' serves as the primary key.
CREATE TABLE IF NOT EXISTS companies (
    ticker VARCHAR(10),
    id VARCHAR(50) NULL,
    issue_share BIGINT NULL,
    history TEXT NULL,
    company_profile TEXT NULL,
    icb_name3 VARCHAR(50) NULL,
    icb_name2 VARCHAR(50) NULL,
    icb_name4 VARCHAR(50) NULL,
    financial_ratio_issue_share BIGINT NULL,
    charter_capital BIGINT NULL,
    PRIMARY KEY (ticker)
);

-- The `years` table stores unique years for time-based analysis.
CREATE TABLE IF NOT EXISTS years (
    year INT,
    PRIMARY KEY (year)
);

-- The `quarters` table stores unique quarters.
CREATE TABLE IF NOT EXISTS quarters (
    quarter INT,
    PRIMARY KEY (quarter)
);

-- The `dates` table stores unique dates, including a foreign key to the `years` table.
CREATE TABLE IF NOT EXISTS dates (
    date DATE,
    year INT NOT NULL,
    PRIMARY KEY (date),
    FOREIGN KEY (year) REFERENCES years(year) ON DELETE CASCADE
);

-- =========================================================================
-- FACT TABLES
-- =========================================================================

-- The `daily_prices` table holds daily trading data, linking to `companies` and `dates`.
CREATE TABLE IF NOT EXISTS daily_prices (
    date DATE NOT NULL,
    open FLOAT NULL,
    high FLOAT NULL,
    low FLOAT NULL,
    close FLOAT NULL,
    volume BIGINT NULL,
    ticker VARCHAR(10),
    PRIMARY KEY (ticker, date),
    FOREIGN KEY (ticker) REFERENCES companies(ticker) ON DELETE CASCADE,
    FOREIGN KEY (date) REFERENCES dates(date) ON DELETE CASCADE
);

-- The `balance_sheets` table contains quarterly balance sheet data, linking to `companies`, `years`, and `quarters`.
CREATE TABLE IF NOT EXISTS balance_sheets (
    ticker VARCHAR(10),
    year INT NOT NULL,
    quarter INT NOT NULL,
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
    PRIMARY KEY (ticker, year, quarter),
    FOREIGN KEY (ticker) REFERENCES companies(ticker) ON DELETE CASCADE,
    FOREIGN KEY (year) REFERENCES years(year) ON DELETE CASCADE,
    FOREIGN KEY (quarter) REFERENCES quarters(quarter) ON DELETE CASCADE
);

-- The `cash_flows` table stores quarterly cash flow data, linking to `companies`, `years`, and `quarters`.
CREATE TABLE IF NOT EXISTS cash_flows (
    ticker VARCHAR(10),
    year INT NOT NULL,
    quarter INT NOT NULL,
    invest_cost FLOAT NULL,
    from_invest FLOAT NULL,
    from_financial FLOAT NULL,
    from_sale FLOAT NULL,
    free_cash_flow FLOAT NULL,
    PRIMARY KEY (ticker, year, quarter),
    FOREIGN KEY (ticker) REFERENCES companies(ticker) ON DELETE CASCADE,
    FOREIGN KEY (year) REFERENCES years(year) ON DELETE CASCADE,
    FOREIGN KEY (quarter) REFERENCES quarters(quarter) ON DELETE CASCADE
);

-- The `income_statements` table holds quarterly income statement data, linking to `companies`, `years`, and `quarters`.
CREATE TABLE IF NOT EXISTS income_statements (
    ticker VARCHAR(10),
    year INT NOT NULL,
    quarter INT NOT NULL,
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
    quarter_revenue_growth FLOAT NULL,
    quarter_operation_profit_growth FLOAT NULL,
    quarter_share_holder_income_growth FLOAT NULL,
    PRIMARY KEY (ticker, year, quarter),
    FOREIGN KEY (ticker) REFERENCES companies(ticker) ON DELETE CASCADE,
    FOREIGN KEY (year) REFERENCES years(year) ON DELETE CASCADE,
    FOREIGN KEY (quarter) REFERENCES quarters(quarter) ON DELETE CASCADE
);

-- The `ratios` table stores quarterly financial ratio data, linking to `companies`, `years`, and `quarters`.
CREATE TABLE IF NOT EXISTS ratios (
    ticker VARCHAR(10),
    year INT NOT NULL,
    quarter INT NOT NULL,
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
    PRIMARY KEY (ticker, year, quarter),
    FOREIGN KEY (ticker) REFERENCES companies(ticker) ON DELETE CASCADE,
    FOREIGN KEY (year) REFERENCES years(year) ON DELETE CASCADE,
    FOREIGN KEY (quarter) REFERENCES quarters(quarter) ON DELETE CASCADE
);
