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
    CURRENT_DATE::timestamp,  -- Ngày kết thúc là HÔM NAY (tự động cập nhật khi chạy lệnh)
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

CREATE TABLE dwh.fact_daily_price (
    -- Khóa ngoại Dimension
    date_key INT NOT NULL REFERENCES dwh.dim_date(date_key),
    company_key INT NOT NULL REFERENCES dwh.dim_company(company_key),
    
    -- Các chỉ số (Metrics)
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    volume BIGINT,
    
    -- Khóa chính tổng hợp
    PRIMARY KEY (date_key, company_key)
);

-- Nạp dữ liệu từ bảng gốc
INSERT INTO dwh.fact_daily_price (date_key, company_key, open_price, high_price, low_price, close_price, volume)
SELECT 
    TO_CHAR(p.date, 'yyyymmdd')::INT AS date_key,
    c.company_key,
    p.open, p.high, p.low, p.close, p.volume
FROM public.daily_price p
JOIN dwh.dim_company c ON p.ticker = c.ticker;

CREATE TABLE dwh.fact_balance_sheet (
    date_key INT NOT NULL REFERENCES dwh.dim_date(date_key), -- Ngày cuối quý
    company_key INT NOT NULL REFERENCES dwh.dim_company(company_key),
    
    -- Các chỉ số (Copy toàn bộ cột số từ bảng gốc)
    cash FLOAT, fixed_asset FLOAT, asset FLOAT, debt FLOAT, equity FLOAT, capital FLOAT, 
    central_bank_deposit FLOAT, other_bank_deposit FLOAT, other_bank_loan FLOAT, 
    stock_invest FLOAT, customer_loan FLOAT, bad_loan FLOAT, provision FLOAT, 
    net_customer_loan FLOAT, other_asset FLOAT, other_bank_credit FLOAT, 
    owe_other_bank FLOAT, owe_central_bank FLOAT, valuable_paper FLOAT, 
    payable_interest FLOAT, receivable_interest FLOAT, deposit FLOAT, other_debt FLOAT, 
    fund FLOAT, un_distributed_income FLOAT, minor_share_holder_profit FLOAT, payable FLOAT,

    PRIMARY KEY (date_key, company_key)
);

-- Nạp dữ liệu (Tính toán date_key là ngày cuối quý)
INSERT INTO dwh.fact_balance_sheet
SELECT 
    (year || CASE WHEN quarter = 1 THEN '0331' WHEN quarter = 2 THEN '0630' WHEN quarter = 3 THEN '0930' WHEN quarter = 4 THEN '1231' END)::INT AS date_key,
    c.company_key,
    b.cash, b.fixed_asset, b.asset, b.debt, b.equity, b.capital, b.central_bank_deposit, b.other_bank_deposit, b.other_bank_loan, b.stock_invest, b.customer_loan, b.bad_loan, b.provision, b.net_customer_loan, b.other_asset, b.other_bank_credit, b.owe_other_bank, b.owe_central_bank, b.valuable_paper, b.payable_interest, b.receivable_interest, b.deposit, b.other_debt, b.fund, b.un_distributed_income, b.minor_share_holder_profit, b.payable
FROM public.balance_sheet b
JOIN dwh.dim_company c ON b.ticker = c.ticker
WHERE b.quarter IN (1, 2, 3, 4); -- Chỉ lấy dữ liệu quý

CREATE TABLE dwh.fact_cash_flow (
    date_key INT NOT NULL REFERENCES dwh.dim_date(date_key),
    company_key INT NOT NULL REFERENCES dwh.dim_company(company_key),
    
    invest_cost FLOAT, from_invest FLOAT, from_financial FLOAT, 
    from_sale FLOAT, free_cash_flow FLOAT,

    PRIMARY KEY (date_key, company_key)
);

-- Nạp dữ liệu
INSERT INTO dwh.fact_cash_flow
SELECT 
    (year || CASE WHEN quarter = 1 THEN '0331' WHEN quarter = 2 THEN '0630' WHEN quarter = 3 THEN '0930' WHEN quarter = 4 THEN '1231' END)::INT AS date_key,
    c.company_key,
    cf.invest_cost, cf.from_invest, cf.from_financial, cf.from_sale, cf.free_cash_flow
FROM public.cash_flow cf
JOIN dwh.dim_company c ON cf.ticker = c.ticker
WHERE cf.quarter IN (1, 2, 3, 4);

CREATE TABLE dwh.fact_income_statement (
    date_key INT NOT NULL REFERENCES dwh.dim_date(date_key),
    company_key INT NOT NULL REFERENCES dwh.dim_company(company_key),
    
    revenue FLOAT, year_revenue_growth FLOAT, operation_expense FLOAT, 
    operation_profit FLOAT, year_operation_profit_growth FLOAT, pre_tax_profit FLOAT, 
    post_tax_profit FLOAT, share_holder_income FLOAT, year_share_holder_income_growth FLOAT, 
    invest_profit FLOAT, service_profit FLOAT, other_profit FLOAT, provision_expense FLOAT, 
    operation_income FLOAT, quarter_revenue_growth FLOAT, 
    quarter_operation_profit_growth FLOAT, quarter_share_holder_income_growth FLOAT,

    PRIMARY KEY (date_key, company_key)
);

-- Nạp dữ liệu
INSERT INTO dwh.fact_income_statement
SELECT 
    (year || CASE WHEN quarter = 1 THEN '0331' WHEN quarter = 2 THEN '0630' WHEN quarter = 3 THEN '0930' WHEN quarter = 4 THEN '1231' END)::INT AS date_key,
    c.company_key,
    i.revenue, i.year_revenue_growth, i.operation_expense, i.operation_profit, i.year_operation_profit_growth, i.pre_tax_profit, i.post_tax_profit, i.share_holder_income, i.year_share_holder_income_growth, i.invest_profit, i.service_profit, i.other_profit, i.provision_expense, i.operation_income, i.quarter_revenue_growth, i.quarter_operation_profit_growth, i.quarter_share_holder_income_growth
FROM public.income_statement i
JOIN dwh.dim_company c ON i.ticker = c.ticker
WHERE i.quarter IN (1, 2, 3, 4);


CREATE TABLE dwh.fact_ratio (
    date_key INT NOT NULL REFERENCES dwh.dim_date(date_key),
    company_key INT NOT NULL REFERENCES dwh.dim_company(company_key),
    
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

    PRIMARY KEY (date_key, company_key)
);

-- Nạp dữ liệu
INSERT INTO dwh.fact_ratio
SELECT 
    (year || CASE WHEN quarter = 1 THEN '0331' WHEN quarter = 2 THEN '0630' WHEN quarter = 3 THEN '0930' WHEN quarter = 4 THEN '1231' END)::INT AS date_key,
    c.company_key,
    r.price_to_earning, r.price_to_book, r.dividend, r.roe, r.roa, r.earning_per_share, r.book_value_per_share, r.interest_margin, r.non_interest_on_toi, r.bad_debt_percentage, r.provision_on_bad_debt, r.cost_of_financing, r.equity_on_total_asset, r.equity_on_loan, r.cost_to_income, r.equity_on_liability, r.eps_change, r.asset_on_equity, r.pre_provision_on_toi, r.post_tax_on_toi, r.loan_on_earn_asset, r.loan_on_asset, r.loan_on_deposit, r.deposit_on_earn_asset, r.bad_debt_on_asset, r.liquidity_on_liability, r.payable_on_equity, r.cancel_debt, r.book_value_per_share_change, r.credit_growth
FROM public.ratio r
JOIN dwh.dim_company c ON r.ticker = c.ticker
WHERE r.quarter IN (1, 2, 3, 4);