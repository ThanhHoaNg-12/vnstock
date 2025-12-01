-- ==================================================
-- PHẦN 1: CÁC HÀM TIỆN ÍCH VÀ HÀM XỬ LÝ (TRIGGER FUNCTIONS)
-- ==================================================

-- 1.1. Hàm tiện ích tính ngày cuối quý (Dùng chung cho các báo cáo tài chính)
CREATE OR REPLACE FUNCTION dwh.get_quarter_end_date_key(p_year INT, p_quarter INT)
RETURNS INT AS $$
BEGIN
    -- Chỉ xử lý dữ liệu quý (1, 2, 3, 4), bỏ qua dữ liệu năm (5)
    IF p_quarter NOT IN (1, 2, 3, 4) THEN
        RETURN NULL;
    END IF;

    -- Ghép năm với ngày tháng cố định của cuối quý để tạo date_key (YYYYMMDD)
    RETURN (p_year || CASE
        WHEN p_quarter = 1 THEN '0331'
        WHEN p_quarter = 2 THEN '0630'
        WHEN p_quarter = 3 THEN '0930'
        WHEN p_quarter = 4 THEN '1231'
    END)::INT;
END;
$$ LANGUAGE plpgsql;


-- 1.2. Hàm xử lý cho bảng Giá Hàng Ngày (DAILY_PRICE)
CREATE OR REPLACE FUNCTION dwh.etl_daily_price_trigger_func()
RETURNS TRIGGER AS $$
DECLARE
    v_date_key INT;
    v_company_key INT;
BEGIN
    -- Tìm khóa ngày dựa trên ngày giao dịch cụ thể
    SELECT date_key INTO v_date_key FROM dwh.dim_date WHERE full_date = NEW.date;
    -- Tìm khóa công ty
    SELECT company_key INTO v_company_key FROM dwh.dim_company WHERE ticker = NEW.ticker;

    IF v_date_key IS NOT NULL AND v_company_key IS NOT NULL THEN
        INSERT INTO dwh.fact_daily_price (
            date_key, company_key, open_price, high_price, low_price, close_price, volume
        ) VALUES (
            v_date_key, v_company_key, NEW.open, NEW.high, NEW.low, NEW.close, NEW.volume
        ) ON CONFLICT (date_key, company_key) DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- 1.3. Hàm xử lý cho bảng Chỉ Số Tài Chính (RATIO)
-- (Đây là đoạn bạn bị viết thiếu, mình đã bổ sung đầy đủ)
CREATE OR REPLACE FUNCTION dwh.etl_ratio_trigger_func()
RETURNS TRIGGER AS $$
DECLARE
    v_date_key INT;
    v_company_key INT;
BEGIN
    -- Tính date_key cuối quý bằng hàm tiện ích
    v_date_key := dwh.get_quarter_end_date_key(NEW.year, NEW.quarter);
    -- Tìm khóa công ty
    SELECT company_key INTO v_company_key FROM dwh.dim_company WHERE ticker = NEW.ticker;

    -- Chèn vào kho
    IF v_date_key IS NOT NULL AND v_company_key IS NOT NULL THEN
        INSERT INTO dwh.fact_ratio (
            date_key, company_key, price_to_earning, price_to_book, dividend, roe, roa, earning_per_share, book_value_per_share, interest_margin, non_interest_on_toi, bad_debt_percentage, provision_on_bad_debt, cost_of_financing, equity_on_total_asset, equity_on_loan, cost_to_income, equity_on_liability, eps_change, asset_on_equity, pre_provision_on_toi, post_tax_on_toi, loan_on_earn_asset, loan_on_asset, loan_on_deposit, deposit_on_earn_asset, bad_debt_on_asset, liquidity_on_liability, payable_on_equity, cancel_debt, book_value_per_share_change, credit_growth
        ) VALUES (
            v_date_key, v_company_key, NEW.price_to_earning, NEW.price_to_book, NEW.dividend, NEW.roe, NEW.roa, NEW.earning_per_share, NEW.book_value_per_share, NEW.interest_margin, NEW.non_interest_on_toi, NEW.bad_debt_percentage, NEW.provision_on_bad_debt, NEW.cost_of_financing, NEW.equity_on_total_asset, NEW.equity_on_loan, NEW.cost_to_income, NEW.equity_on_liability, NEW.eps_change, NEW.asset_on_equity, NEW.pre_provision_on_toi, NEW.post_tax_on_toi, NEW.loan_on_earn_asset, NEW.loan_on_asset, NEW.loan_on_deposit, NEW.deposit_on_earn_asset, NEW.bad_debt_on_asset, NEW.liquidity_on_liability, NEW.payable_on_equity, NEW.cancel_debt, NEW.book_value_per_share_change, NEW.credit_growth
        ) ON CONFLICT (date_key, company_key) DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- 1.4. Hàm xử lý cho bảng Cân Đối Kế Toán (BALANCE_SHEET)
CREATE OR REPLACE FUNCTION dwh.etl_balance_sheet_trigger_func()
RETURNS TRIGGER AS $$
DECLARE v_date_key INT; v_company_key INT;
BEGIN
    v_date_key := dwh.get_quarter_end_date_key(NEW.year, NEW.quarter);
    SELECT company_key INTO v_company_key FROM dwh.dim_company WHERE ticker = NEW.ticker;

    IF v_date_key IS NOT NULL AND v_company_key IS NOT NULL THEN
        INSERT INTO dwh.fact_balance_sheet (
            date_key, company_key, cash, fixed_asset, asset, debt, equity, capital, central_bank_deposit, other_bank_deposit, other_bank_loan, stock_invest, customer_loan, bad_loan, provision, net_customer_loan, other_asset, other_bank_credit, owe_other_bank, owe_central_bank, valuable_paper, payable_interest, receivable_interest, deposit, other_debt, fund, un_distributed_income, minor_share_holder_profit, payable
        ) VALUES (
            v_date_key, v_company_key, NEW.cash, NEW.fixed_asset, NEW.asset, NEW.debt, NEW.equity, NEW.capital, NEW.central_bank_deposit, NEW.other_bank_deposit, NEW.other_bank_loan, NEW.stock_invest, NEW.customer_loan, NEW.bad_loan, NEW.provision, NEW.net_customer_loan, NEW.other_asset, NEW.other_bank_credit, NEW.owe_other_bank, NEW.owe_central_bank, NEW.valuable_paper, NEW.payable_interest, NEW.receivable_interest, NEW.deposit, NEW.other_debt, NEW.fund, NEW.un_distributed_income, NEW.minor_share_holder_profit, NEW.payable
        ) ON CONFLICT (date_key, company_key) DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- 1.5. Hàm xử lý cho bảng Kết Quả Kinh Doanh (INCOME_STATEMENT)
CREATE OR REPLACE FUNCTION dwh.etl_income_statement_trigger_func()
RETURNS TRIGGER AS $$
DECLARE v_date_key INT; v_company_key INT;
BEGIN
    v_date_key := dwh.get_quarter_end_date_key(NEW.year, NEW.quarter);
    SELECT company_key INTO v_company_key FROM dwh.dim_company WHERE ticker = NEW.ticker;

    IF v_date_key IS NOT NULL AND v_company_key IS NOT NULL THEN
        INSERT INTO dwh.fact_income_statement (
            date_key, company_key, revenue, year_revenue_growth, operation_expense, operation_profit, year_operation_profit_growth, pre_tax_profit, post_tax_profit, share_holder_income, year_share_holder_income_growth, invest_profit, service_profit, other_profit, provision_expense, operation_income, quarter_revenue_growth, quarter_operation_profit_growth, quarter_share_holder_income_growth
        ) VALUES (
            v_date_key, v_company_key, NEW.revenue, NEW.year_revenue_growth, NEW.operation_expense, NEW.operation_profit, NEW.year_operation_profit_growth, NEW.pre_tax_profit, NEW.post_tax_profit, NEW.share_holder_income, NEW.year_share_holder_income_growth, NEW.invest_profit, NEW.service_profit, NEW.other_profit, NEW.provision_expense, NEW.operation_income, NEW.quarter_revenue_growth, NEW.quarter_operation_profit_growth, NEW.quarter_share_holder_income_growth
        ) ON CONFLICT (date_key, company_key) DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- 1.6. Hàm xử lý cho bảng Lưu Chuyển Tiền Tệ (CASH_FLOW)
CREATE OR REPLACE FUNCTION dwh.etl_cash_flow_trigger_func()
RETURNS TRIGGER AS $$
DECLARE v_date_key INT; v_company_key INT;
BEGIN
    v_date_key := dwh.get_quarter_end_date_key(NEW.year, NEW.quarter);
    SELECT company_key INTO v_company_key FROM dwh.dim_company WHERE ticker = NEW.ticker;

    IF v_date_key IS NOT NULL AND v_company_key IS NOT NULL THEN
        INSERT INTO dwh.fact_cash_flow (
            date_key, company_key, invest_cost, from_invest, from_financial, from_sale, free_cash_flow
        ) VALUES (
            v_date_key, v_company_key, NEW.invest_cost, NEW.from_invest, NEW.from_financial, NEW.from_sale, NEW.free_cash_flow
        ) ON CONFLICT (date_key, company_key) DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ==================================================
-- PHẦN 2: GÀI BẪY (TẠO TRIGGERS) TRÊN CÁC BẢNG THÔ
-- ==================================================

-- Trigger cho bảng daily_price
DROP TRIGGER IF EXISTS trg_daily_price_to_dwh ON public.daily_price;
CREATE TRIGGER trg_daily_price_to_dwh
AFTER INSERT ON public.daily_price
FOR EACH ROW EXECUTE FUNCTION dwh.etl_daily_price_trigger_func();

-- Trigger cho bảng ratio
DROP TRIGGER IF EXISTS trg_ratio_to_dwh ON public.ratio;
CREATE TRIGGER trg_ratio_to_dwh
AFTER INSERT ON public.ratio
FOR EACH ROW EXECUTE FUNCTION dwh.etl_ratio_trigger_func();

-- Trigger cho bảng balance_sheet
DROP TRIGGER IF EXISTS trg_balance_sheet_to_dwh ON public.balance_sheet;
CREATE TRIGGER trg_balance_sheet_to_dwh
AFTER INSERT ON public.balance_sheet
FOR EACH ROW EXECUTE FUNCTION dwh.etl_balance_sheet_trigger_func();

-- Trigger cho bảng income_statement
DROP TRIGGER IF EXISTS trg_income_statement_to_dwh ON public.income_statement;
CREATE TRIGGER trg_income_statement_to_dwh
AFTER INSERT ON public.income_statement
FOR EACH ROW EXECUTE FUNCTION dwh.etl_income_statement_trigger_func();

-- Trigger cho bảng cash_flow
DROP TRIGGER IF EXISTS trg_cash_flow_to_dwh ON public.cash_flow;
CREATE TRIGGER trg_cash_flow_to_dwh
AFTER INSERT ON public.cash_flow
FOR EACH ROW EXECUTE FUNCTION dwh.etl_cash_flow_trigger_func();