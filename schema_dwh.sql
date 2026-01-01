-- Tạo schema cho Data Warehouse
CREATE SCHEMA IF NOT EXISTS dwh;

-- ==================================================================
-- BƯỚC 1: XÓA SẠCH CÁC BẢNG CŨ
-- ==================================================================
DROP TABLE IF EXISTS dwh.fact_daily_price CASCADE;
DROP TABLE IF EXISTS dwh.fact_balance_sheet CASCADE;
DROP TABLE IF EXISTS dwh.fact_income_statement CASCADE;
DROP TABLE IF EXISTS dwh.fact_cash_flow CASCADE;
DROP TABLE IF EXISTS dwh.fact_ratio CASCADE;
DROP TABLE IF EXISTS dwh.dim_company CASCADE; 
DROP TABLE IF EXISTS dwh.dim_date CASCADE;
-- Xóa luôn 2 bảng này vì đã gộp vào dim_date
DROP TABLE IF EXISTS dwh.dim_quarter CASCADE;
DROP TABLE IF EXISTS dwh.dim_year CASCADE;
DROP TABLE IF EXISTS dwh.etl_trigger_log CASCADE;
DROP TABLE IF EXISTS dwh.dim_exchange CASCADE;
DROP TABLE IF EXISTS dwh.dim_industry CASCADE;

-- ==================================================================
-- BƯỚC 2: TẠO VÀ NẠP DIMENSION THỜI GIAN (GỘP DATE, QUARTER, YEAR)
-- ==================================================================
CREATE TABLE dwh.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    -- Thông tin Ngày
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    day_of_month INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    -- Thông tin Quý (Đã gộp từ dim_quarter cũ)
    quarter INT NOT NULL,
    quarter_name VARCHAR(20) NOT NULL, -- Ví dụ: Quý 1 2024
    -- Thông tin Năm (Đã gộp từ dim_year cũ)
    year INT NOT NULL,
    is_leap_year BOOLEAN NOT NULL,
    -- Flags
    is_weekend BOOLEAN NOT NULL,
    is_trading_day BOOLEAN NOT NULL DEFAULT TRUE
);

INSERT INTO dwh.dim_date
SELECT
    TO_CHAR(datum, 'yyyymmdd')::INT AS date_key,
    datum::DATE AS full_date,
    EXTRACT(ISODOW FROM datum) AS day_of_week,
    TO_CHAR(datum, 'Day') AS day_name,
    EXTRACT(DAY FROM datum) AS day_of_month,
    EXTRACT(MONTH FROM datum) AS month,
    TO_CHAR(datum, 'Month') AS month_name,
    -- Gộp Quarter
    EXTRACT(QUARTER FROM datum)::INT AS quarter,
    'Quý ' || EXTRACT(QUARTER FROM datum)::INT || ' ' || EXTRACT(YEAR FROM datum)::INT AS quarter_name,
    -- Gộp Year
    EXTRACT(YEAR FROM datum)::INT AS year,
    ((EXTRACT(YEAR FROM datum)::INT % 4 = 0 AND EXTRACT(YEAR FROM datum)::INT % 100 <> 0) OR (EXTRACT(YEAR FROM datum)::INT % 400 = 0)) AS is_leap_year,
    -- Weekend check
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series('2000-01-01'::timestamp, '3000-01-01'::timestamp, '1 day'::interval) AS datum
ORDER BY 1;

CREATE INDEX idx_dim_date_full_date ON dwh.dim_date(full_date);
CREATE INDEX idx_dim_date_year ON dwh.dim_date(year);    -- Index cho cột year mới
CREATE INDEX idx_dim_date_quarter ON dwh.dim_date(quarter); -- Index cho cột quarter mới

-- ==================================================================
-- BƯỚC 3: TẠO BẢNG DIM_COMPANY (GIỮ NGUYÊN)
-- ==================================================================
CREATE TABLE dwh.dim_company (
    company_key SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL UNIQUE,
    short_name VARCHAR(500),
    company_type VARCHAR(30),
    established_year INT,
    no_employees INT,
    no_shareholders BIGINT,
    website VARCHAR(500),
    exchange_code VARCHAR(10),
    industry_name VARCHAR(100),
    industry_id INT,
    industry_id_v2 INT,
    foreign_percent FLOAT,
    outstanding_share FLOAT,
    issue_share FLOAT,
    stock_rating FLOAT,
    delta_in_week FLOAT,
    delta_in_month FLOAT,
    delta_in_year FLOAT
);

INSERT INTO dwh.dim_company (
    ticker, short_name, company_type, established_year, no_employees, no_shareholders, website, 
    exchange_code, industry_name, industry_id, industry_id_v2,
    foreign_percent, outstanding_share, issue_share, stock_rating, delta_in_week, delta_in_month, delta_in_year
)
SELECT 
    ticker, short_name, company_type, established_year, no_employees, no_shareholders, website, 
    exchange, industry, industry_id, industry_id_v2,
    foreign_percent, outstanding_share, issue_share, stock_rating, delta_in_week, delta_in_month, delta_in_year
FROM public.company;

-- ==================================================================
-- BƯỚC 4: TẠO CÁC BẢNG FACT (GIỮ NGUYÊN)
-- ==================================================================

-- 4.1. Fact Daily Price
CREATE TABLE dwh.fact_daily_price (
    date_key INT NOT NULL REFERENCES dwh.dim_date(date_key),
    company_key INT NOT NULL REFERENCES dwh.dim_company(company_key),
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    volume BIGINT,
    PRIMARY KEY (date_key, company_key)
);

INSERT INTO dwh.fact_daily_price (date_key, company_key, open_price, high_price, low_price, close_price, volume)
SELECT
    d.date_key,
    c.company_key,
    p.open, p.high, p.low, p.close, p.volume
FROM public.daily_price p
JOIN dwh.dim_company c ON p.ticker = c.ticker
JOIN dwh.dim_date d ON d.full_date = p.date
ON CONFLICT (date_key, company_key) DO UPDATE SET
    open_price = EXCLUDED.open_price,
    high_price = EXCLUDED.high_price,
    low_price = EXCLUDED.low_price,
    close_price = EXCLUDED.close_price,
    volume = EXCLUDED.volume;

-- 4.2. Fact Balance Sheet
CREATE TABLE dwh.fact_balance_sheet (
    date_key INT NOT NULL REFERENCES dwh.dim_date(date_key),
    company_key INT NOT NULL REFERENCES dwh.dim_company(company_key),
    is_annual BOOLEAN NOT NULL DEFAULT FALSE,
    cash FLOAT, fixed_asset FLOAT, asset FLOAT, debt FLOAT, equity FLOAT, capital FLOAT,
    central_bank_deposit FLOAT, other_bank_deposit FLOAT, other_bank_loan FLOAT,
    stock_invest FLOAT, customer_loan FLOAT, bad_loan FLOAT, provision FLOAT,
    net_customer_loan FLOAT, other_asset FLOAT, other_bank_credit FLOAT,
    owe_other_bank FLOAT, owe_central_bank FLOAT, valuable_paper FLOAT,
    payable_interest FLOAT, receivable_interest FLOAT, deposit FLOAT, other_debt FLOAT,
    fund FLOAT, un_distributed_income FLOAT, minor_share_holder_profit FLOAT, payable FLOAT,
    PRIMARY KEY (date_key, company_key, is_annual)
);

INSERT INTO dwh.fact_balance_sheet (date_key, company_key, is_annual, cash, fixed_asset, asset, debt, equity, capital, central_bank_deposit, other_bank_deposit, other_bank_loan, stock_invest, customer_loan, bad_loan, provision, net_customer_loan, other_asset, other_bank_credit, owe_other_bank, owe_central_bank, valuable_paper, payable_interest, receivable_interest, deposit, other_debt, fund, un_distributed_income, minor_share_holder_profit, payable)
SELECT
    d.date_key,
    c.company_key,
    (CASE WHEN b.quarter = 5 THEN TRUE ELSE FALSE END),
    b.cash, b.fixed_asset, b.asset, b.debt, b.equity, b.capital, b.central_bank_deposit, b.other_bank_deposit, b.other_bank_loan, b.stock_invest, b.customer_loan, b.bad_loan, b.provision, b.net_customer_loan, b.other_asset, b.other_bank_credit, b.owe_other_bank, b.owe_central_bank, b.valuable_paper, b.payable_interest, b.receivable_interest, b.deposit, b.other_debt, b.fund, b.un_distributed_income, b.minor_share_holder_profit, b.payable
FROM public.balance_sheet b
JOIN dwh.dim_company c ON b.ticker = c.ticker
JOIN dwh.dim_date d ON d.full_date = (
    CASE WHEN b.quarter = 1 THEN make_date(b.year, 3, 31)
         WHEN b.quarter = 2 THEN make_date(b.year, 6, 30)
         WHEN b.quarter = 3 THEN make_date(b.year, 9, 30)
         WHEN b.quarter IN (4, 5) THEN make_date(b.year, 12, 31)
    END
)
WHERE b.quarter IN (1, 2, 3, 4, 5)
ON CONFLICT (date_key, company_key, is_annual) DO UPDATE SET
    cash = EXCLUDED.cash, fixed_asset = EXCLUDED.fixed_asset, asset = EXCLUDED.asset, debt = EXCLUDED.debt, equity = EXCLUDED.equity, capital = EXCLUDED.capital, central_bank_deposit = EXCLUDED.central_bank_deposit, other_bank_deposit = EXCLUDED.other_bank_deposit, other_bank_loan = EXCLUDED.other_bank_loan, stock_invest = EXCLUDED.stock_invest, customer_loan = EXCLUDED.customer_loan, bad_loan = EXCLUDED.bad_loan, provision = EXCLUDED.provision, net_customer_loan = EXCLUDED.net_customer_loan, other_asset = EXCLUDED.other_asset, other_bank_credit = EXCLUDED.other_bank_credit, owe_other_bank = EXCLUDED.owe_other_bank, owe_central_bank = EXCLUDED.owe_central_bank, valuable_paper = EXCLUDED.valuable_paper, payable_interest = EXCLUDED.payable_interest, receivable_interest = EXCLUDED.receivable_interest, deposit = EXCLUDED.deposit, other_debt = EXCLUDED.other_debt, fund = EXCLUDED.fund, un_distributed_income = EXCLUDED.un_distributed_income, minor_share_holder_profit = EXCLUDED.minor_share_holder_profit, payable = EXCLUDED.payable;

-- 4.3. Fact Cash Flow
CREATE TABLE dwh.fact_cash_flow (
    date_key INT NOT NULL REFERENCES dwh.dim_date(date_key),
    company_key INT NOT NULL REFERENCES dwh.dim_company(company_key),
    is_annual BOOLEAN NOT NULL DEFAULT FALSE,
    invest_cost FLOAT, from_invest FLOAT, from_financial FLOAT,
    from_sale FLOAT, free_cash_flow FLOAT,
    PRIMARY KEY (date_key, company_key, is_annual)
);

INSERT INTO dwh.fact_cash_flow (date_key, company_key, is_annual, invest_cost, from_invest, from_financial, from_sale, free_cash_flow)
SELECT
    d.date_key,
    c.company_key,
    (CASE WHEN cf.quarter = 5 THEN TRUE ELSE FALSE END),
    cf.invest_cost, cf.from_invest, cf.from_financial, cf.from_sale, cf.free_cash_flow
FROM public.cash_flow cf
JOIN dwh.dim_company c ON cf.ticker = c.ticker
JOIN dwh.dim_date d ON d.full_date = (
    CASE WHEN cf.quarter = 1 THEN make_date(cf.year, 3, 31)
         WHEN cf.quarter = 2 THEN make_date(cf.year, 6, 30)
         WHEN cf.quarter = 3 THEN make_date(cf.year, 9, 30)
         WHEN cf.quarter IN (4, 5) THEN make_date(cf.year, 12, 31)
    END
)
WHERE cf.quarter IN (1, 2, 3, 4, 5)
ON CONFLICT (date_key, company_key, is_annual) DO UPDATE SET
    invest_cost = EXCLUDED.invest_cost, from_invest = EXCLUDED.from_invest, from_financial = EXCLUDED.from_financial, from_sale = EXCLUDED.from_sale, free_cash_flow = EXCLUDED.free_cash_flow;

-- 4.4. Fact Income Statement
CREATE TABLE dwh.fact_income_statement (
    date_key INT NOT NULL REFERENCES dwh.dim_date(date_key),
    company_key INT NOT NULL REFERENCES dwh.dim_company(company_key),
    is_annual BOOLEAN NOT NULL DEFAULT FALSE,
    revenue FLOAT, year_revenue_growth FLOAT, operation_expense FLOAT,
    operation_profit FLOAT, year_operation_profit_growth FLOAT, pre_tax_profit FLOAT,
    post_tax_profit FLOAT, share_holder_income FLOAT, year_share_holder_income_growth FLOAT,
    invest_profit FLOAT, service_profit FLOAT, other_profit FLOAT, provision_expense FLOAT,
    operation_income FLOAT, quarter_revenue_growth FLOAT,
    quarter_operation_profit_growth FLOAT, quarter_share_holder_income_growth FLOAT,
    PRIMARY KEY (date_key, company_key, is_annual)
);

INSERT INTO dwh.fact_income_statement (date_key, company_key, is_annual, revenue, year_revenue_growth, operation_expense, operation_profit, year_operation_profit_growth, pre_tax_profit, post_tax_profit, share_holder_income, year_share_holder_income_growth, invest_profit, service_profit, other_profit, provision_expense, operation_income, quarter_revenue_growth, quarter_operation_profit_growth, quarter_share_holder_income_growth)
SELECT
    d.date_key,
    c.company_key,
    (CASE WHEN i.quarter = 5 THEN TRUE ELSE FALSE END),
    i.revenue, i.year_revenue_growth, i.operation_expense, i.operation_profit, i.year_operation_profit_growth, i.pre_tax_profit, i.post_tax_profit, i.share_holder_income, i.year_share_holder_income_growth, i.invest_profit, i.service_profit, i.other_profit, i.provision_expense, i.operation_income, i.quarter_revenue_growth, i.quarter_operation_profit_growth, i.quarter_share_holder_income_growth
FROM public.income_statement i
JOIN dwh.dim_company c ON i.ticker = c.ticker
JOIN dwh.dim_date d ON d.full_date = (
    CASE WHEN i.quarter = 1 THEN make_date(i.year, 3, 31)
         WHEN i.quarter = 2 THEN make_date(i.year, 6, 30)
         WHEN i.quarter = 3 THEN make_date(i.year, 9, 30)
         WHEN i.quarter IN (4, 5) THEN make_date(i.year, 12, 31)
    END
)
WHERE i.quarter IN (1, 2, 3, 4, 5)
ON CONFLICT (date_key, company_key, is_annual) DO UPDATE SET
    revenue = EXCLUDED.revenue, year_revenue_growth = EXCLUDED.year_revenue_growth, operation_expense = EXCLUDED.operation_expense, operation_profit = EXCLUDED.operation_profit, year_operation_profit_growth = EXCLUDED.year_operation_profit_growth, pre_tax_profit = EXCLUDED.pre_tax_profit, post_tax_profit = EXCLUDED.post_tax_profit, share_holder_income = EXCLUDED.share_holder_income, year_share_holder_income_growth = EXCLUDED.year_share_holder_income_growth, invest_profit = EXCLUDED.invest_profit, service_profit = EXCLUDED.service_profit, other_profit = EXCLUDED.other_profit, provision_expense = EXCLUDED.provision_expense, operation_income = EXCLUDED.operation_income, quarter_revenue_growth = EXCLUDED.quarter_revenue_growth, quarter_operation_profit_growth = EXCLUDED.quarter_operation_profit_growth, quarter_share_holder_income_growth = EXCLUDED.quarter_share_holder_income_growth;

-- 4.5. Fact Ratio
CREATE TABLE dwh.fact_ratio (
    date_key INT NOT NULL REFERENCES dwh.dim_date(date_key),
    company_key INT NOT NULL REFERENCES dwh.dim_company(company_key),
    is_annual BOOLEAN NOT NULL DEFAULT FALSE,
    price_to_earning FLOAT, price_to_book FLOAT, dividend FLOAT, roe FLOAT, roa FLOAT,
    earning_per_share FLOAT, book_value_per_share FLOAT, interest_margin FLOAT,
    non_interest_on_toi FLOAT, bad_debt_percentage FLOAT, provision_on_bad_debt FLOAT,
    cost_of_financing FLOAT, equity_on_total_asset FLOAT, equity_on_loan FLOAT,
    cost_to_income FLOAT, equity_on_liability FLOAT, eps_change FLOAT,
    asset_on_equity FLOAT, pre_provision_on_toi FLOAT, post_tax_on_toi FLOAT,
    loan_on_earn_asset FLOAT, loan_on_asset FLOAT, loan_on_deposit FLOAT,
    deposit_on_earn_asset FLOAT, bad_debt_on_asset FLOAT, liquidity_on_liability FLOAT,
    payable_on_equity FLOAT, cancel_debt FLOAT, book_value_per_share_change FLOAT,
    credit_growth FLOAT,
    PRIMARY KEY (date_key, company_key, is_annual)
);

INSERT INTO dwh.fact_ratio (date_key, company_key, is_annual, price_to_earning, price_to_book, dividend, roe, roa, earning_per_share, book_value_per_share, interest_margin, non_interest_on_toi, bad_debt_percentage, provision_on_bad_debt, cost_of_financing, equity_on_total_asset, equity_on_loan, cost_to_income, equity_on_liability, eps_change, asset_on_equity, pre_provision_on_toi, post_tax_on_toi, loan_on_earn_asset, loan_on_asset, loan_on_deposit, deposit_on_earn_asset, bad_debt_on_asset, liquidity_on_liability, payable_on_equity, cancel_debt, book_value_per_share_change, credit_growth)
SELECT
    d.date_key,
    c.company_key,
    (CASE WHEN r.quarter = 5 THEN TRUE ELSE FALSE END),
    r.price_to_earning, r.price_to_book, r.dividend, r.roe, r.roa, r.earning_per_share, r.book_value_per_share, r.interest_margin, r.non_interest_on_toi, r.bad_debt_percentage, r.provision_on_bad_debt, r.cost_of_financing, r.equity_on_total_asset, r.equity_on_loan, r.cost_to_income, r.equity_on_liability, r.eps_change, r.asset_on_equity, r.pre_provision_on_toi, r.post_tax_on_toi, r.loan_on_earn_asset, r.loan_on_asset, r.loan_on_deposit, r.deposit_on_earn_asset, r.bad_debt_on_asset, r.liquidity_on_liability, r.payable_on_equity, r.cancel_debt, r.book_value_per_share_change, r.credit_growth
FROM public.ratio r
JOIN dwh.dim_company c ON r.ticker = c.ticker
JOIN dwh.dim_date d ON d.full_date = (
    CASE WHEN r.quarter = 1 THEN make_date(r.year, 3, 31)
         WHEN r.quarter = 2 THEN make_date(r.year, 6, 30)
         WHEN r.quarter = 3 THEN make_date(r.year, 9, 30)
         WHEN r.quarter IN (4, 5) THEN make_date(r.year, 12, 31)
    END
)
WHERE r.quarter IN (1, 2, 3, 4, 5)
ON CONFLICT (date_key, company_key, is_annual) DO UPDATE SET
    price_to_earning = EXCLUDED.price_to_earning, price_to_book = EXCLUDED.price_to_book, dividend = EXCLUDED.dividend, roe = EXCLUDED.roe, roa = EXCLUDED.roa, earning_per_share = EXCLUDED.earning_per_share, book_value_per_share = EXCLUDED.book_value_per_share, interest_margin = EXCLUDED.interest_margin, non_interest_on_toi = EXCLUDED.non_interest_on_toi, bad_debt_percentage = EXCLUDED.bad_debt_percentage, provision_on_bad_debt = EXCLUDED.provision_on_bad_debt, cost_of_financing = EXCLUDED.cost_of_financing, equity_on_total_asset = EXCLUDED.equity_on_total_asset, equity_on_loan = EXCLUDED.equity_on_loan, cost_to_income = EXCLUDED.cost_to_income, equity_on_liability = EXCLUDED.equity_on_liability, eps_change = EXCLUDED.eps_change, asset_on_equity = EXCLUDED.asset_on_equity, pre_provision_on_toi = EXCLUDED.pre_provision_on_toi, post_tax_on_toi = EXCLUDED.post_tax_on_toi, loan_on_earn_asset = EXCLUDED.loan_on_earn_asset, loan_on_asset = EXCLUDED.loan_on_asset, loan_on_deposit = EXCLUDED.loan_on_deposit, deposit_on_earn_asset = EXCLUDED.deposit_on_earn_asset, bad_debt_on_asset = EXCLUDED.bad_debt_on_asset, liquidity_on_liability = EXCLUDED.liquidity_on_liability, payable_on_equity = EXCLUDED.payable_on_equity, cancel_debt = EXCLUDED.cancel_debt, book_value_per_share_change = EXCLUDED.book_value_per_share_change, credit_growth = EXCLUDED.credit_growth;

-- 4.6. Bảng log
CREATE TABLE IF NOT EXISTS dwh.etl_trigger_log (
    log_id SERIAL PRIMARY KEY,
    trigger_name VARCHAR(100),
    source_table VARCHAR(50),
    ticker VARCHAR(50),
    error_message TEXT,
    payload JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);