-- ==================================================
-- PHẦN 1: CÁC HÀM TIỆN ÍCH VÀ HÀM XỬ LÝ (TRIGGER FUNCTIONS)
-- ==================================================

-- 1.0. Log function (Ghi log lỗi hoặc sự kiện ETL)
CREATE OR REPLACE FUNCTION dwh.log_etl_event(
    p_trigger_name VARCHAR,
    p_source_table VARCHAR,
    p_ticker VARCHAR DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL,
    p_payload JSONB DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    INSERT INTO dwh.etl_trigger_log(trigger_name, source_table, ticker, error_message, payload)
    VALUES (p_trigger_name, p_source_table, p_ticker, p_error_message, p_payload);
END;
$$ LANGUAGE plpgsql;

-- 1.1. Get quarter end date key (Tính toán ngày cuối quý để lấy DateKey)
CREATE OR REPLACE FUNCTION dwh.get_quarter_end_date_key(p_year INT, p_quarter INT)
RETURNS INT AS $$
DECLARE
    v_date_key INT;
    v_quarter_end_date DATE;
BEGIN
    IF p_quarter NOT IN (1, 2, 3, 4, 5) THEN RETURN NULL; END IF;
    
    -- Xác định ngày cuối quý
    v_quarter_end_date := CASE
        WHEN p_quarter = 1 THEN make_date(p_year, 3, 31)
        WHEN p_quarter = 2 THEN make_date(p_year, 6, 30)
        WHEN p_quarter = 3 THEN make_date(p_year, 9, 30)
        WHEN p_quarter IN (4, 5) THEN make_date(p_year, 12, 31)
    END;

    -- Lookup date_key từ bảng dim_date (đã gộp)
    SELECT date_key INTO v_date_key FROM dwh.dim_date WHERE full_date = v_quarter_end_date LIMIT 1;
    RETURN v_date_key;
END;
$$ LANGUAGE plpgsql;

-- 1.2. Daily Price Trigger
CREATE OR REPLACE FUNCTION dwh.etl_daily_price_trigger_func()
RETURNS TRIGGER AS $$
DECLARE
    v_date_key INT;
    v_company_key INT;
BEGIN
    IF NEW.ticker IS NULL OR NEW.date IS NULL THEN
        PERFORM dwh.log_etl_event('etl_daily_price_trigger_func', 'daily_price', NEW.ticker, 'NULL ticker/date', to_jsonb(NEW));
        RETURN NEW;
    END IF;

    SELECT date_key INTO v_date_key FROM dwh.dim_date WHERE full_date = NEW.date LIMIT 1;
    SELECT company_key INTO v_company_key FROM dwh.dim_company WHERE ticker = NEW.ticker LIMIT 1;

    IF v_date_key IS NULL OR v_company_key IS NULL THEN
        PERFORM dwh.log_etl_event('etl_daily_price_trigger_func', 'daily_price', NEW.ticker, 'Missing dim_date/dim_company', to_jsonb(NEW));
        RETURN NEW;
    END IF;

    INSERT INTO dwh.fact_daily_price(date_key, company_key, open_price, high_price, low_price, close_price, volume)
    VALUES (v_date_key, v_company_key, NEW.open, NEW.high, NEW.low, NEW.close, NEW.volume)
    ON CONFLICT (date_key, company_key) DO UPDATE SET
        open_price = EXCLUDED.open_price, high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price, close_price = EXCLUDED.close_price, volume = EXCLUDED.volume;
    RETURN NEW;
EXCEPTION WHEN OTHERS THEN
    PERFORM dwh.log_etl_event('etl_daily_price_trigger_func', 'daily_price', NEW.ticker, SQLERRM, to_jsonb(NEW));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 1.3. Ratio Trigger
CREATE OR REPLACE FUNCTION dwh.etl_ratio_trigger_func()
RETURNS TRIGGER AS $$
DECLARE
    v_date_key INT;
    v_company_key INT;
BEGIN
    IF NEW.ticker IS NULL OR NEW.year IS NULL OR NEW.quarter IS NULL THEN
        PERFORM dwh.log_etl_event('etl_ratio_trigger_func', 'ratio', NEW.ticker, 'NULL ticker/year/quarter', to_jsonb(NEW));
        RETURN NEW;
    END IF;
    
    v_date_key := dwh.get_quarter_end_date_key(NEW.year, NEW.quarter);
    SELECT company_key INTO v_company_key FROM dwh.dim_company WHERE ticker = NEW.ticker LIMIT 1;
    
    IF v_date_key IS NULL OR v_company_key IS NULL THEN
        PERFORM dwh.log_etl_event('etl_ratio_trigger_func', 'ratio', NEW.ticker, 'Missing mapping', to_jsonb(NEW));
        RETURN NEW;
    END IF;

    INSERT INTO dwh.fact_ratio (
        date_key, company_key, is_annual, price_to_earning, price_to_book, dividend, roe, roa, earning_per_share, book_value_per_share, interest_margin, non_interest_on_toi, bad_debt_percentage, provision_on_bad_debt, cost_of_financing, equity_on_total_asset, equity_on_loan, cost_to_income, equity_on_liability, eps_change, asset_on_equity, pre_provision_on_toi, post_tax_on_toi, loan_on_earn_asset, loan_on_asset, loan_on_deposit, deposit_on_earn_asset, bad_debt_on_asset, liquidity_on_liability, payable_on_equity, cancel_debt, book_value_per_share_change, credit_growth
    ) VALUES (
        v_date_key, v_company_key, (CASE WHEN NEW.quarter = 5 THEN TRUE ELSE FALSE END), NEW.price_to_earning, NEW.price_to_book, NEW.dividend, NEW.roe, NEW.roa, NEW.earning_per_share, NEW.book_value_per_share, NEW.interest_margin, NEW.non_interest_on_toi, NEW.bad_debt_percentage, NEW.provision_on_bad_debt, NEW.cost_of_financing, NEW.equity_on_total_asset, NEW.equity_on_loan, NEW.cost_to_income, NEW.equity_on_liability, NEW.eps_change, NEW.asset_on_equity, NEW.pre_provision_on_toi, NEW.post_tax_on_toi, NEW.loan_on_earn_asset, NEW.loan_on_asset, NEW.loan_on_deposit, NEW.deposit_on_earn_asset, NEW.bad_debt_on_asset, NEW.liquidity_on_liability, NEW.payable_on_equity, NEW.cancel_debt, NEW.book_value_per_share_change, NEW.credit_growth
    ) ON CONFLICT (date_key, company_key, is_annual) DO UPDATE SET
        price_to_earning = EXCLUDED.price_to_earning, price_to_book = EXCLUDED.price_to_book, dividend = EXCLUDED.dividend, roe = EXCLUDED.roe, roa = EXCLUDED.roa, earning_per_share = EXCLUDED.earning_per_share, book_value_per_share = EXCLUDED.book_value_per_share, interest_margin = EXCLUDED.interest_margin, non_interest_on_toi = EXCLUDED.non_interest_on_toi, bad_debt_percentage = EXCLUDED.bad_debt_percentage, provision_on_bad_debt = EXCLUDED.provision_on_bad_debt, cost_of_financing = EXCLUDED.cost_of_financing, equity_on_total_asset = EXCLUDED.equity_on_total_asset, equity_on_loan = EXCLUDED.equity_on_loan, cost_to_income = EXCLUDED.cost_to_income, equity_on_liability = EXCLUDED.equity_on_liability, eps_change = EXCLUDED.eps_change, asset_on_equity = EXCLUDED.asset_on_equity, pre_provision_on_toi = EXCLUDED.pre_provision_on_toi, post_tax_on_toi = EXCLUDED.post_tax_on_toi, loan_on_earn_asset = EXCLUDED.loan_on_earn_asset, loan_on_asset = EXCLUDED.loan_on_asset, loan_on_deposit = EXCLUDED.loan_on_deposit, deposit_on_earn_asset = EXCLUDED.deposit_on_earn_asset, bad_debt_on_asset = EXCLUDED.bad_debt_on_asset, liquidity_on_liability = EXCLUDED.liquidity_on_liability, payable_on_equity = EXCLUDED.payable_on_equity, cancel_debt = EXCLUDED.cancel_debt, book_value_per_share_change = EXCLUDED.book_value_per_share_change, credit_growth = EXCLUDED.credit_growth;
    RETURN NEW;
EXCEPTION WHEN OTHERS THEN
    PERFORM dwh.log_etl_event('etl_ratio_trigger_func', 'ratio', NEW.ticker, SQLERRM, to_jsonb(NEW));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 1.4. Balance Sheet Trigger
CREATE OR REPLACE FUNCTION dwh.etl_balance_sheet_trigger_func()
RETURNS TRIGGER AS $$
DECLARE
    v_date_key INT;
    v_company_key INT;
BEGIN
    IF NEW.ticker IS NULL OR NEW.year IS NULL OR NEW.quarter IS NULL THEN
        PERFORM dwh.log_etl_event('etl_balance_sheet_trigger_func', 'balance_sheet', NEW.ticker, 'NULL ticker/year/quarter', to_jsonb(NEW));
        RETURN NEW;
    END IF;
    v_date_key := dwh.get_quarter_end_date_key(NEW.year, NEW.quarter);
    SELECT company_key INTO v_company_key FROM dwh.dim_company WHERE ticker = NEW.ticker LIMIT 1;

    IF v_date_key IS NULL OR v_company_key IS NULL THEN
        PERFORM dwh.log_etl_event('etl_balance_sheet_trigger_func', 'balance_sheet', NEW.ticker, 'Missing mapping', to_jsonb(NEW));
        RETURN NEW;
    END IF;

    INSERT INTO dwh.fact_balance_sheet (
        date_key, company_key, is_annual, cash, fixed_asset, asset, debt, equity, capital, central_bank_deposit, other_bank_deposit, other_bank_loan, stock_invest, customer_loan, bad_loan, provision, net_customer_loan, other_asset, other_bank_credit, owe_other_bank, owe_central_bank, valuable_paper, payable_interest, receivable_interest, deposit, other_debt, fund, un_distributed_income, minor_share_holder_profit, payable
    ) VALUES (
        v_date_key, v_company_key, (CASE WHEN NEW.quarter = 5 THEN TRUE ELSE FALSE END), NEW.cash, NEW.fixed_asset, NEW.asset, NEW.debt, NEW.equity, NEW.capital, NEW.central_bank_deposit, NEW.other_bank_deposit, NEW.other_bank_loan, NEW.stock_invest, NEW.customer_loan, NEW.bad_loan, NEW.provision, NEW.net_customer_loan, NEW.other_asset, NEW.other_bank_credit, NEW.owe_other_bank, NEW.owe_central_bank, NEW.valuable_paper, NEW.payable_interest, NEW.receivable_interest, NEW.deposit, NEW.other_debt, NEW.fund, NEW.un_distributed_income, NEW.minor_share_holder_profit, NEW.payable
    ) ON CONFLICT (date_key, company_key, is_annual) DO UPDATE SET
        cash = EXCLUDED.cash, fixed_asset = EXCLUDED.fixed_asset, asset = EXCLUDED.asset, debt = EXCLUDED.debt, equity = EXCLUDED.equity, capital = EXCLUDED.capital, central_bank_deposit = EXCLUDED.central_bank_deposit, other_bank_deposit = EXCLUDED.other_bank_deposit, other_bank_loan = EXCLUDED.other_bank_loan, stock_invest = EXCLUDED.stock_invest, customer_loan = EXCLUDED.customer_loan, bad_loan = EXCLUDED.bad_loan, provision = EXCLUDED.provision, net_customer_loan = EXCLUDED.net_customer_loan, other_asset = EXCLUDED.other_asset, other_bank_credit = EXCLUDED.other_bank_credit, owe_other_bank = EXCLUDED.owe_other_bank, owe_central_bank = EXCLUDED.owe_central_bank, valuable_paper = EXCLUDED.valuable_paper, payable_interest = EXCLUDED.payable_interest, receivable_interest = EXCLUDED.receivable_interest, deposit = EXCLUDED.deposit, other_debt = EXCLUDED.other_debt, fund = EXCLUDED.fund, un_distributed_income = EXCLUDED.un_distributed_income, minor_share_holder_profit = EXCLUDED.minor_share_holder_profit, payable = EXCLUDED.payable;
    RETURN NEW;
EXCEPTION WHEN OTHERS THEN
    PERFORM dwh.log_etl_event('etl_balance_sheet_trigger_func', 'balance_sheet', NEW.ticker, SQLERRM, to_jsonb(NEW));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 1.5. Income Statement Trigger
CREATE OR REPLACE FUNCTION dwh.etl_income_statement_trigger_func()
RETURNS TRIGGER AS $$
DECLARE
    v_date_key INT;
    v_company_key INT;
BEGIN
    IF NEW.ticker IS NULL OR NEW.year IS NULL OR NEW.quarter IS NULL THEN
        PERFORM dwh.log_etl_event('etl_income_statement_trigger_func', 'income_statement', NEW.ticker, 'NULL ticker/year/quarter', to_jsonb(NEW));
        RETURN NEW;
    END IF;
    v_date_key := dwh.get_quarter_end_date_key(NEW.year, NEW.quarter);
    SELECT company_key INTO v_company_key FROM dwh.dim_company WHERE ticker = NEW.ticker LIMIT 1;

    IF v_date_key IS NULL OR v_company_key IS NULL THEN
        PERFORM dwh.log_etl_event('etl_income_statement_trigger_func', 'income_statement', NEW.ticker, 'Missing mapping', to_jsonb(NEW));
        RETURN NEW;
    END IF;

    INSERT INTO dwh.fact_income_statement (
        date_key, company_key, is_annual, revenue, year_revenue_growth, operation_expense, operation_profit, year_operation_profit_growth, pre_tax_profit, post_tax_profit, share_holder_income, year_share_holder_income_growth, invest_profit, service_profit, other_profit, provision_expense, operation_income, quarter_revenue_growth, quarter_operation_profit_growth, quarter_share_holder_income_growth
    ) VALUES (
        v_date_key, v_company_key, (CASE WHEN NEW.quarter = 5 THEN TRUE ELSE FALSE END), NEW.revenue, NEW.year_revenue_growth, NEW.operation_expense, NEW.operation_profit, NEW.year_operation_profit_growth, NEW.pre_tax_profit, NEW.post_tax_profit, NEW.share_holder_income, NEW.year_share_holder_income_growth, NEW.invest_profit, NEW.service_profit, NEW.other_profit, NEW.provision_expense, NEW.operation_income, NEW.quarter_revenue_growth, NEW.quarter_operation_profit_growth, NEW.quarter_share_holder_income_growth
    ) ON CONFLICT (date_key, company_key, is_annual) DO UPDATE SET
        revenue = EXCLUDED.revenue, year_revenue_growth = EXCLUDED.year_revenue_growth, operation_expense = EXCLUDED.operation_expense, operation_profit = EXCLUDED.operation_profit, year_operation_profit_growth = EXCLUDED.year_operation_profit_growth, pre_tax_profit = EXCLUDED.pre_tax_profit, post_tax_profit = EXCLUDED.post_tax_profit, share_holder_income = EXCLUDED.share_holder_income, year_share_holder_income_growth = EXCLUDED.year_share_holder_income_growth, invest_profit = EXCLUDED.invest_profit, service_profit = EXCLUDED.service_profit, other_profit = EXCLUDED.other_profit, provision_expense = EXCLUDED.provision_expense, operation_income = EXCLUDED.operation_income, quarter_revenue_growth = EXCLUDED.quarter_revenue_growth, quarter_operation_profit_growth = EXCLUDED.quarter_operation_profit_growth, quarter_share_holder_income_growth = EXCLUDED.quarter_share_holder_income_growth;
    RETURN NEW;
EXCEPTION WHEN OTHERS THEN
    PERFORM dwh.log_etl_event('etl_income_statement_trigger_func', 'income_statement', NEW.ticker, SQLERRM, to_jsonb(NEW));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 1.6. Cash Flow Trigger
CREATE OR REPLACE FUNCTION dwh.etl_cash_flow_trigger_func()
RETURNS TRIGGER AS $$
DECLARE
    v_date_key INT;
    v_company_key INT;
BEGIN
    IF NEW.ticker IS NULL OR NEW.year IS NULL OR NEW.quarter IS NULL THEN
        PERFORM dwh.log_etl_event('etl_cash_flow_trigger_func', 'cash_flow', NEW.ticker, 'NULL ticker/year/quarter', to_jsonb(NEW));
        RETURN NEW;
    END IF;
    v_date_key := dwh.get_quarter_end_date_key(NEW.year, NEW.quarter);
    SELECT company_key INTO v_company_key FROM dwh.dim_company WHERE ticker = NEW.ticker LIMIT 1;

    IF v_date_key IS NULL OR v_company_key IS NULL THEN
        PERFORM dwh.log_etl_event('etl_cash_flow_trigger_func', 'cash_flow', NEW.ticker, 'Missing mapping', to_jsonb(NEW));
        RETURN NEW;
    END IF;

    INSERT INTO dwh.fact_cash_flow (
        date_key, company_key, is_annual, invest_cost, from_invest, from_financial, from_sale, free_cash_flow
    ) VALUES (
        v_date_key, v_company_key, (CASE WHEN NEW.quarter = 5 THEN TRUE ELSE FALSE END), NEW.invest_cost, NEW.from_invest, NEW.from_financial, NEW.from_sale, NEW.free_cash_flow
    ) ON CONFLICT (date_key, company_key, is_annual) DO UPDATE SET
        invest_cost = EXCLUDED.invest_cost, from_invest = EXCLUDED.from_invest, from_financial = EXCLUDED.from_financial, from_sale = EXCLUDED.from_sale, free_cash_flow = EXCLUDED.free_cash_flow;
    RETURN NEW;
EXCEPTION WHEN OTHERS THEN
    PERFORM dwh.log_etl_event('etl_cash_flow_trigger_func', 'cash_flow', NEW.ticker, SQLERRM, to_jsonb(NEW));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 1.7. SYNC DIM COMPANY (Cập nhật thẳng vào dim_company đã gộp)
CREATE OR REPLACE FUNCTION dwh.sync_dim_company()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.ticker IS NULL THEN
        PERFORM dwh.log_etl_event('sync_dim_company', 'company', NULL, 'NULL ticker in source company', to_jsonb(NEW));
        RETURN NEW;
    END IF;

    -- Upsert dim_company với đầy đủ các cột mới và ánh xạ sàn/ngành
    INSERT INTO dwh.dim_company (
        ticker, short_name, company_type, established_year, 
        no_employees, no_shareholders, website, 
        foreign_percent, outstanding_share, issue_share, stock_rating,
        delta_in_week, delta_in_month, delta_in_year,
        exchange_code, industry_name, industry_id, industry_id_v2
    )
    VALUES (
        NEW.ticker, NEW.short_name, NEW.company_type, NEW.established_year, 
        NEW.no_employees, NEW.no_shareholders, NEW.website, 
        NEW.foreign_percent, NEW.outstanding_share, NEW.issue_share, NEW.stock_rating,
        NEW.delta_in_week, NEW.delta_in_month, NEW.delta_in_year,
        NEW.exchange, NEW.industry, NEW.industry_id, NEW.industry_id_v2
    )
    ON CONFLICT (ticker) DO UPDATE SET
        short_name = EXCLUDED.short_name,
        company_type = EXCLUDED.company_type,
        established_year = EXCLUDED.established_year,
        no_employees = EXCLUDED.no_employees,
        no_shareholders = EXCLUDED.no_shareholders,
        website = EXCLUDED.website,
        foreign_percent = EXCLUDED.foreign_percent,
        outstanding_share = EXCLUDED.outstanding_share,
        issue_share = EXCLUDED.issue_share,
        stock_rating = EXCLUDED.stock_rating,
        delta_in_week = EXCLUDED.delta_in_week,
        delta_in_month = EXCLUDED.delta_in_month,
        delta_in_year = EXCLUDED.delta_in_year,
        exchange_code = EXCLUDED.exchange_code,
        industry_name = EXCLUDED.industry_name,
        industry_id = EXCLUDED.industry_id,
        industry_id_v2 = EXCLUDED.industry_id_v2;

    RETURN NEW;
EXCEPTION WHEN OTHERS THEN
    PERFORM dwh.log_etl_event('sync_dim_company', 'company', NEW.ticker, SQLERRM, to_jsonb(NEW));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ==================================================
-- PHẦN 2: GÀI BẪY (TẠO TRIGGERS) TRÊN CÁC BẢNG THÔ
-- ==================================================

DROP TRIGGER IF EXISTS trg_daily_price_to_dwh ON public.daily_price;
CREATE TRIGGER trg_daily_price_to_dwh AFTER INSERT OR UPDATE ON public.daily_price FOR EACH ROW EXECUTE FUNCTION dwh.etl_daily_price_trigger_func();

DROP TRIGGER IF EXISTS trg_ratio_to_dwh ON public.ratio;
CREATE TRIGGER trg_ratio_to_dwh AFTER INSERT OR UPDATE ON public.ratio FOR EACH ROW EXECUTE FUNCTION dwh.etl_ratio_trigger_func();

DROP TRIGGER IF EXISTS trg_balance_sheet_to_dwh ON public.balance_sheet;
CREATE TRIGGER trg_balance_sheet_to_dwh AFTER INSERT OR UPDATE ON public.balance_sheet FOR EACH ROW EXECUTE FUNCTION dwh.etl_balance_sheet_trigger_func();

DROP TRIGGER IF EXISTS trg_income_statement_to_dwh ON public.income_statement;
CREATE TRIGGER trg_income_statement_to_dwh AFTER INSERT OR UPDATE ON public.income_statement FOR EACH ROW EXECUTE FUNCTION dwh.etl_income_statement_trigger_func();

DROP TRIGGER IF EXISTS trg_cash_flow_to_dwh ON public.cash_flow;
CREATE TRIGGER trg_cash_flow_to_dwh AFTER INSERT OR UPDATE ON public.cash_flow FOR EACH ROW EXECUTE FUNCTION dwh.etl_cash_flow_trigger_func();

DROP TRIGGER IF EXISTS trg_company_to_dwh ON public.company;
CREATE TRIGGER trg_company_to_dwh AFTER INSERT OR UPDATE ON public.company FOR EACH ROW EXECUTE FUNCTION dwh.sync_dim_company();