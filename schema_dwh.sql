-- Tạo schema cho Data Warehouse
CREATE SCHEMA IF NOT EXISTS dwh;

-- Xóa sạch các bảng cũ trong schema dwh để làm lại từ đầu (nếu cần)
DROP TABLE IF EXISTS dwh.fact_daily_price CASCADE;
DROP TABLE IF EXISTS dwh.fact_balance_sheet CASCADE;
DROP TABLE IF EXISTS dwh.fact_income_statement CASCADE;
DROP TABLE IF EXISTS dwh.fact_cash_flow CASCADE;
DROP TABLE IF EXISTS dwh.fact_ratio CASCADE;
DROP TABLE IF EXISTS dwh.dim_company CASCADE;
DROP TABLE IF EXISTS dwh.dim_date CASCADE;
DROP TABLE IF EXISTS dwh.dim_industry CASCADE;
DROP TABLE IF EXISTS dwh.dim_exchange CASCADE;

-- ==================================================================
-- BƯỚC 1: XÓA BẢNG LỊCH CŨ (VÀ CÁC BẢNG FACT LIÊN QUAN DO CASCADE)
-- ==================================================================
-- CẢNH BÁO: HÀNH ĐỘNG NÀY SẼ XÓA DỮ LIỆU CÁC BẢNG FACT ĐANG CÓ

-- ==================================================================
-- BƯỚC 2: TẠO LẠI CẤU TRÚC BẢNG
-- ==================================================================
CREATE TABLE dwh.dim_date (
    date_key INT PRIMARY KEY,         -- YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    day_of_month INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_trading_day BOOLEAN NOT NULL DEFAULT TRUE
);

-- ==================================================================
-- BƯỚC 3: NẠP DỮ LIỆU ĐỘNG (TỪ 2000-01-01 ĐẾN CURRENT_DATE)
-- ==================================================================
-- Sử dụng generate_series với timestamp để tạo dải ngày động
INSERT INTO dwh.dim_date
SELECT
    TO_CHAR(datum, 'yyyymmdd')::INT AS date_key,
    datum::DATE AS full_date, -- Ép kiểu từ timestamp về date
    EXTRACT(ISODOW FROM datum) AS day_of_week,
    TO_CHAR(datum, 'Day') AS day_name,
    EXTRACT(DAY FROM datum) AS day_of_month,
    EXTRACT(MONTH FROM datum) AS month,
    TO_CHAR(datum, 'Month') AS month_name,
    EXTRACT(QUARTER FROM datum) AS quarter,
    EXTRACT(YEAR FROM datum) AS year,
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series(
    '2000-01-01'::timestamp,  -- Ngày bắt đầu cố định
    '3000-01-01'::timestamp,  -- Ngày kết thúc là 1000 năm nữa (tự động cập nhật khi chạy lệnh)
    '1 day'::interval         -- Bước nhảy là 1 ngày
) AS datum
ORDER BY 1;

CREATE TABLE dwh.dim_exchange (
    exchange_key SERIAL PRIMARY KEY,  -- Khóa tự tăng (Surrogate Key)
    exchange_code VARCHAR(10) NOT NULL UNIQUE, -- Mã sàn (HOSE, HNX, UPCOM)
    exchange_name VARCHAR(100)        -- Tên đầy đủ (có thể cập nhật sau)
);

-- Nạp dữ liệu từ bảng gốc (Chỉ lấy các sàn duy nhất)
INSERT INTO dwh.dim_exchange (exchange_code)
SELECT DISTINCT exchange
FROM public.company
WHERE exchange IS NOT NULL
ORDER BY exchange;

CREATE TABLE dwh.dim_industry (
    industry_key SERIAL PRIMARY KEY, -- Khóa tự tăng (Surrogate Key)
    industry_name VARCHAR(50) NOT NULL UNIQUE, -- Tên ngành
    industry_id INT,                 -- ID ngành từ hệ thống nguồn
    industry_id_v2 INT               -- ID ngành phiên bản 2
);

-- Nạp dữ liệu từ bảng gốc
INSERT INTO dwh.dim_industry (industry_name, industry_id, industry_id_v2)
SELECT DISTINCT industry, industry_id, industry_id_v2
FROM public.company
WHERE industry IS NOT NULL
ORDER BY industry;

CREATE TABLE dwh.dim_company (
    company_key SERIAL PRIMARY KEY,   -- Khóa tự tăng (Surrogate Key)
    ticker VARCHAR(10) NOT NULL UNIQUE, -- Mã chứng khoán (Business Key)
    short_name VARCHAR(500),          -- Tên ngắn gọn
    company_type VARCHAR(30),         -- Loại hình công ty
    established_year INT,             -- Năm thành lập
    no_employees INT,                 -- Số lượng nhân viên
    no_shareholders BIGINT,           -- Số lượng cổ đông
    website VARCHAR(500),             -- Website
    -- Các cột khóa ngoại trỏ tới bảng dimension khác
    exchange_key INT REFERENCES dwh.dim_exchange(exchange_key),
    industry_key INT REFERENCES dwh.dim_industry(industry_key)
);

-- Nạp dữ liệu từ bảng gốc, thực hiện JOIN để lấy khóa ngoại của Exchange và Industry
INSERT INTO dwh.dim_company (ticker, short_name, company_type, established_year, no_employees, no_shareholders, website, exchange_key, industry_key)
SELECT
    c.ticker,
    c.short_name,
    c.company_type,
    c.established_year,
    c.no_employees,
    c.no_shareholders,
    c.website,
    e.exchange_key,
    i.industry_key
FROM public.company c
LEFT JOIN dwh.dim_exchange e ON c.exchange = e.exchange_code
LEFT JOIN dwh.dim_industry i ON c.industry = i.industry_name;


-- ==================================================================
-- BƯỚC 4: TẠO CÁC BẢNG FACT VÀ NẠP DỮ LIỆU
-- ==================================================================
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

-- Nạp dữ liệu từ bảng daily_price, thực hiện JOIN để lấy company_key
INSERT INTO dwh.fact_daily_price (date_key, company_key, open_price, high_price, low_price, close_price, volume)
SELECT
    TO_CHAR(p.date, 'yyyymmdd')::INT AS date_key,
    c.company_key,
    p.open, p.high, p.low, p.close, p.volume
FROM public.daily_price p
JOIN dwh.dim_company c ON p.ticker = c.ticker
ON CONFLICT (date_key, company_key) DO UPDATE SET
    open_price = EXCLUDED.open_price,
    high_price = EXCLUDED.high_price,
    low_price = EXCLUDED.low_price,
    close_price = EXCLUDED.close_price,
    volume = EXCLUDED.volume;


-- Balance sheet: Bảng balance_sheet bao gồm cả dữ liệu quý và cả năm. Dữ liệu cả năm được đánh dấu bằng  is_annual = TRUE
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

-- Nạp dữ liệu từ bảng balance_sheet, thực hiện JOIN để lấy company_key
INSERT INTO dwh.fact_balance_sheet (date_key, company_key, is_annual, cash, fixed_asset, asset, debt, equity, capital, central_bank_deposit, other_bank_deposit, other_bank_loan, stock_invest, customer_loan, bad_loan, provision, net_customer_loan, other_asset, other_bank_credit, owe_other_bank, owe_central_bank, valuable_paper, payable_interest, receivable_interest, deposit, other_debt, fund, un_distributed_income, minor_share_holder_profit, payable)
SELECT
    (year || CASE WHEN quarter = 1 THEN '0331' WHEN quarter = 2 THEN '0630' WHEN quarter = 3 THEN '0930' WHEN quarter IN (4,5) THEN '1231' END)::INT AS date_key,
    c.company_key,
    (CASE WHEN b.quarter = 5 THEN TRUE ELSE FALSE END) AS is_annual,
    b.cash, b.fixed_asset, b.asset, b.debt, b.equity, b.capital, b.central_bank_deposit, b.other_bank_deposit, b.other_bank_loan, b.stock_invest, b.customer_loan, b.bad_loan, b.provision, b.net_customer_loan, b.other_asset, b.other_bank_credit, b.owe_other_bank, b.owe_central_bank, b.valuable_paper, b.payable_interest, b.receivable_interest, b.deposit, b.other_debt, b.fund, b.un_distributed_income, b.minor_share_holder_profit, b.payable
FROM public.balance_sheet b
JOIN dwh.dim_company c ON b.ticker = c.ticker
WHERE b.quarter IN (1, 2, 3, 4, 5)
ON CONFLICT (date_key, company_key, is_annual) DO UPDATE SET
    cash = EXCLUDED.cash,
    fixed_asset = EXCLUDED.fixed_asset,
    asset = EXCLUDED.asset,
    debt = EXCLUDED.debt,
    equity = EXCLUDED.equity,
    capital = EXCLUDED.capital,
    central_bank_deposit = EXCLUDED.central_bank_deposit,
    other_bank_deposit = EXCLUDED.other_bank_deposit,
    other_bank_loan = EXCLUDED.other_bank_loan,
    stock_invest = EXCLUDED.stock_invest,
    customer_loan = EXCLUDED.customer_loan,
    bad_loan = EXCLUDED.bad_loan,
    provision = EXCLUDED.provision,
    net_customer_loan = EXCLUDED.net_customer_loan,
    other_asset = EXCLUDED.other_asset,
    other_bank_credit = EXCLUDED.other_bank_credit,
    owe_other_bank = EXCLUDED.owe_other_bank,
    owe_central_bank = EXCLUDED.owe_central_bank,
    valuable_paper = EXCLUDED.valuable_paper,
    payable_interest = EXCLUDED.payable_interest,
    receivable_interest = EXCLUDED.receivable_interest,
    deposit = EXCLUDED.deposit,
    other_debt = EXCLUDED.other_debt,
    fund = EXCLUDED.fund,
    un_distributed_income = EXCLUDED.un_distributed_income,
    minor_share_holder_profit = EXCLUDED.minor_share_holder_profit,
    payable = EXCLUDED.payable;

-- Cash flow: Bảng cash_flow bao gồm cả dữ liệu quý và cả năm. Dữ liệu cả năm được đánh dấu bằng quarter = 5 hoặc is_annual = TRUE
CREATE TABLE dwh.fact_cash_flow (
    date_key INT NOT NULL REFERENCES dwh.dim_date(date_key),
    company_key INT NOT NULL REFERENCES dwh.dim_company(company_key),
    is_annual BOOLEAN NOT NULL DEFAULT FALSE,
    invest_cost FLOAT, from_invest FLOAT, from_financial FLOAT,
    from_sale FLOAT, free_cash_flow FLOAT,
    PRIMARY KEY (date_key, company_key, is_annual)
);

-- Nạp dữ liệu từ bảng cash_flow, thực hiện JOIN để lấy company_key
INSERT INTO dwh.fact_cash_flow (date_key, company_key, is_annual, invest_cost, from_invest, from_financial, from_sale, free_cash_flow)
SELECT
    (year || CASE WHEN quarter = 1 THEN '0331' WHEN quarter = 2 THEN '0630' WHEN quarter = 3 THEN '0930' WHEN quarter IN (4,5) THEN '1231' END)::INT AS date_key,
    c.company_key,
    (CASE WHEN cf.quarter = 5 THEN TRUE ELSE FALSE END) AS is_annual,
    cf.invest_cost, cf.from_invest, cf.from_financial, cf.from_sale, cf.free_cash_flow
FROM public.cash_flow cf
JOIN dwh.dim_company c ON cf.ticker = c.ticker
WHERE cf.quarter IN (1, 2, 3, 4, 5)
ON CONFLICT (date_key, company_key, is_annual) DO UPDATE SET
    invest_cost = EXCLUDED.invest_cost,
    from_invest = EXCLUDED.from_invest,
    from_financial = EXCLUDED.from_financial,
    from_sale = EXCLUDED.from_sale,
    free_cash_flow = EXCLUDED.free_cash_flow;

-- Income statement: Bảng income_statement bao gồm cả dữ liệu quý và cả năm. Dữ liệu cả năm được đánh dấu bằng is_annual = TRUE
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

-- Nạp dữ liệu từ bảng income_statement, thực hiện JOIN để lấy company_key
INSERT INTO dwh.fact_income_statement (date_key, company_key, is_annual, revenue, year_revenue_growth, operation_expense, operation_profit, year_operation_profit_growth, pre_tax_profit, post_tax_profit, share_holder_income, year_share_holder_income_growth, invest_profit, service_profit, other_profit, provision_expense, operation_income, quarter_revenue_growth, quarter_operation_profit_growth, quarter_share_holder_income_growth)
SELECT
    (year || CASE WHEN quarter = 1 THEN '0331' WHEN quarter = 2 THEN '0630' WHEN quarter = 3 THEN '0930' WHEN quarter IN (4,5) THEN '1231' END)::INT AS date_key,
    c.company_key,
    (CASE WHEN i.quarter = 5 THEN TRUE ELSE FALSE END) AS is_annual,
    i.revenue, i.year_revenue_growth, i.operation_expense, i.operation_profit, i.year_operation_profit_growth, i.pre_tax_profit, i.post_tax_profit, i.share_holder_income, i.year_share_holder_income_growth, i.invest_profit, i.service_profit, i.other_profit, i.provision_expense, i.operation_income, i.quarter_revenue_growth, i.quarter_operation_profit_growth, i.quarter_share_holder_income_growth
FROM public.income_statement i
JOIN dwh.dim_company c ON i.ticker = c.ticker
WHERE i.quarter IN (1, 2, 3, 4, 5)
ON CONFLICT (date_key, company_key, is_annual) DO UPDATE SET
    revenue = EXCLUDED.revenue,
    year_revenue_growth = EXCLUDED.year_revenue_growth,
    operation_expense = EXCLUDED.operation_expense,
    operation_profit = EXCLUDED.operation_profit,
    year_operation_profit_growth = EXCLUDED.year_operation_profit_growth,
    pre_tax_profit = EXCLUDED.pre_tax_profit,
    post_tax_profit = EXCLUDED.post_tax_profit,
    share_holder_income = EXCLUDED.share_holder_income,
    year_share_holder_income_growth = EXCLUDED.year_share_holder_income_growth,
    invest_profit = EXCLUDED.invest_profit,
    service_profit = EXCLUDED.service_profit,
    other_profit = EXCLUDED.other_profit,
    provision_expense = EXCLUDED.provision_expense,
    operation_income = EXCLUDED.operation_income,
    quarter_revenue_growth = EXCLUDED.quarter_revenue_growth,
    quarter_operation_profit_growth = EXCLUDED.quarter_operation_profit_growth,
    quarter_share_holder_income_growth = EXCLUDED.quarter_share_holder_income_growth;

-- Bảng ratio: Bảng ratio bao gồm cả dữ liệu quý và cả năm. Dữ liệu cả năm được đánh dấu bằng is_annual = TRUE
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

-- Nạp dữ liệu từ bảng ratio, thực hiện JOIN để lấy company_key
INSERT INTO dwh.fact_ratio (date_key, company_key, is_annual, price_to_earning, price_to_book, dividend, roe, roa, earning_per_share, book_value_per_share, interest_margin, non_interest_on_toi, bad_debt_percentage, provision_on_bad_debt, cost_of_financing, equity_on_total_asset, equity_on_loan, cost_to_income, equity_on_liability, eps_change, asset_on_equity, pre_provision_on_toi, post_tax_on_toi, loan_on_earn_asset, loan_on_asset, loan_on_deposit, deposit_on_earn_asset, bad_debt_on_asset, liquidity_on_liability, payable_on_equity, cancel_debt, book_value_per_share_change, credit_growth)
SELECT
    (year || CASE WHEN quarter = 1 THEN '0331' WHEN quarter = 2 THEN '0630' WHEN quarter = 3 THEN '0930' WHEN quarter IN (4,5) THEN '1231' END)::INT AS date_key,
    c.company_key,
    (CASE WHEN r.quarter = 5 THEN TRUE ELSE FALSE END) AS is_annual,
    r.price_to_earning, r.price_to_book, r.dividend, r.roe, r.roa, r.earning_per_share, r.book_value_per_share, r.interest_margin, r.non_interest_on_toi, r.bad_debt_percentage, r.provision_on_bad_debt, r.cost_of_financing, r.equity_on_total_asset, r.equity_on_loan, r.cost_to_income, r.equity_on_liability, r.eps_change, r.asset_on_equity, r.pre_provision_on_toi, r.post_tax_on_toi, r.loan_on_earn_asset, r.loan_on_asset, r.loan_on_deposit, r.deposit_on_earn_asset, r.bad_debt_on_asset, r.liquidity_on_liability, r.payable_on_equity, r.cancel_debt, r.book_value_per_share_change, r.credit_growth
FROM public.ratio r
JOIN dwh.dim_company c ON r.ticker = c.ticker
WHERE r.quarter IN (1, 2, 3, 4, 5)
ON CONFLICT (date_key, company_key, is_annual) DO UPDATE SET
    price_to_earning = EXCLUDED.price_to_earning,
    price_to_book = EXCLUDED.price_to_book,
    dividend = EXCLUDED.dividend,
    roe = EXCLUDED.roe,
    roa = EXCLUDED.roa,
    earning_per_share = EXCLUDED.earning_per_share,
    book_value_per_share = EXCLUDED.book_value_per_share,
    interest_margin = EXCLUDED.interest_margin,
    non_interest_on_toi = EXCLUDED.non_interest_on_toi,
    bad_debt_percentage = EXCLUDED.bad_debt_percentage,
    provision_on_bad_debt = EXCLUDED.provision_on_bad_debt,
    cost_of_financing = EXCLUDED.cost_of_financing,
    equity_on_total_asset = EXCLUDED.equity_on_total_asset,
    equity_on_loan = EXCLUDED.equity_on_loan,
    cost_to_income = EXCLUDED.cost_to_income,
    equity_on_liability = EXCLUDED.equity_on_liability,
    eps_change = EXCLUDED.eps_change,
    asset_on_equity = EXCLUDED.asset_on_equity,
    pre_provision_on_toi = EXCLUDED.pre_provision_on_toi,
    post_tax_on_toi = EXCLUDED.post_tax_on_toi,
    loan_on_earn_asset = EXCLUDED.loan_on_earn_asset,
    loan_on_asset = EXCLUDED.loan_on_asset,
    loan_on_deposit = EXCLUDED.loan_on_deposit,
    deposit_on_earn_asset = EXCLUDED.deposit_on_earn_asset,
    bad_debt_on_asset = EXCLUDED.bad_debt_on_asset,
    liquidity_on_liability = EXCLUDED.liquidity_on_liability,
    payable_on_equity = EXCLUDED.payable_on_equity,
    cancel_debt = EXCLUDED.cancel_debt,
    book_value_per_share_change = EXCLUDED.book_value_per_share_change,
    credit_growth = EXCLUDED.credit_growth;

-- =========================
-- Bảng log cho ETL Trigger
-- =========================
CREATE TABLE IF NOT EXISTS dwh.etl_trigger_log (
    log_id SERIAL PRIMARY KEY,
    trigger_name VARCHAR(100),
    source_table VARCHAR(50),
    ticker VARCHAR(50),
    error_message TEXT,
    payload JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
