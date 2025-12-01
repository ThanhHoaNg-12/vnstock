-- ==================================================
-- BƯỚC 1: XÓA BẢNG CŨ (LÀM SẠCH)
-- ==================================================
-- Xóa bảng cũ và các ràng buộc khóa ngoại liên quan
DROP TABLE IF EXISTS dwh.dim_date CASCADE;
-- Xóa các bảng mới nếu đã lỡ tạo thử trước đó
DROP TABLE IF EXISTS dwh.dim_quarter CASCADE;
DROP TABLE IF EXISTS dwh.dim_year CASCADE;


-- ==================================================
-- BƯỚC 2: TẠO VÀ NẠP DỮ LIỆU CHO DIM_YEAR
-- ==================================================
-- 2.1. Tạo bảng
CREATE TABLE dwh.dim_year (
    year_key INT PRIMARY KEY,   -- Ví dụ: 2023
    year_number INT NOT NULL,   -- 2023
    is_leap_year BOOLEAN NOT NULL -- Năm nhuận hay không
);

-- 2.2. Nạp dữ liệu tự động (Từ 2000 đến 2035)
INSERT INTO dwh.dim_year (year_key, year_number, is_leap_year)
SELECT 
    y AS year_key,
    y AS year_number,
    (y % 4 = 0 AND y % 100 <> 0) OR (y % 400 = 0) AS is_leap_year
FROM generate_series(2000, 2035) AS y;


-- ==================================================
-- BƯỚC 3: TẠO VÀ NẠP DỮ LIỆU CHO DIM_QUARTER (ĐÃ SỬA LỖI)
-- ==================================================
-- 3.1. Tạo bảng
CREATE TABLE dwh.dim_quarter (
    quarter_key INT PRIMARY KEY,    -- Ví dụ: 20231, 20232
    year_key INT NOT NULL REFERENCES dwh.dim_year(year_key), -- Khóa ngoại
    quarter_number INT NOT NULL,    -- 1, 2, 3, 4
    quarter_name VARCHAR(20) NOT NULL -- 'Quý 1 2023', ...
);

-- 3.2. Nạp dữ liệu tự động (SỬA LỖI Ở ĐÂY)
INSERT INTO dwh.dim_quarter (quarter_key, year_key, quarter_number, quarter_name)
SELECT 
    -- Ép kiểu sang TEXT trước khi nối, sau đó ép ngược lại về INT
    (y.year_number::TEXT || q::TEXT)::INT AS quarter_key, 
    y.year_key,
    q AS quarter_number,
    'Quý ' || q || ' ' || y.year_number AS quarter_name
FROM dwh.dim_year y
CROSS JOIN generate_series(1, 4) AS q -- Tạo tổ hợp mỗi năm có 4 quý
ORDER BY 1;


-- ==================================================
-- BƯỚC 4: TẠO VÀ NẠP DỮ LIỆU CHO DIM_DATE MỚI (ĐÃ SỬA LỖI TƯƠNG TỰ)
-- ==================================================
-- 4.1. Tạo bảng
CREATE TABLE dwh.dim_date (
    date_key INT PRIMARY KEY,         -- YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    -- Khóa ngoại trỏ lên bảng Quý
    quarter_key INT NOT NULL REFERENCES dwh.dim_quarter(quarter_key),
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    day_of_month INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_trading_day BOOLEAN NOT NULL DEFAULT TRUE
);

-- 4.2. Nạp dữ liệu tự động (SỬA LỖI Ở ĐÂY)
INSERT INTO dwh.dim_date
SELECT
    TO_CHAR(datum, 'yyyymmdd')::INT AS date_key,
    datum::DATE AS full_date,
    -- Tính toán khóa ngoại quarter_key (YYYYQ) - Cần ép kiểu TEXT
    (EXTRACT(YEAR FROM datum)::TEXT || EXTRACT(QUARTER FROM datum)::TEXT)::INT AS quarter_key,
    
    EXTRACT(ISODOW FROM datum) AS day_of_week,
    TO_CHAR(datum, 'Day') AS day_name,
    EXTRACT(DAY FROM datum) AS day_of_month,
    EXTRACT(MONTH FROM datum) AS month,
    TO_CHAR(datum, 'Month') AS month_name,
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series(
    '2000-01-01'::timestamp, 
    CURRENT_DATE::timestamp, 
    '1 day'::interval
) AS datum
ORDER BY 1;


-- ==================================================
-- BƯỚC 5: TÁI TẠO LẠI CÁC RÀNG BUỘC KHÓA NGOẠI CHO BẢNG FACT
-- ==================================================
-- Nối lại cho bảng giá
ALTER TABLE dwh.fact_daily_price 
ADD CONSTRAINT fk_fact_price_date FOREIGN KEY (date_key) REFERENCES dwh.dim_date(date_key);

-- Nối lại cho các báo cáo tài chính
ALTER TABLE dwh.fact_ratio 
ADD CONSTRAINT fk_fact_ratio_date FOREIGN KEY (date_key) REFERENCES dwh.dim_date(date_key);

ALTER TABLE dwh.fact_balance_sheet 
ADD CONSTRAINT fk_fact_balance_date FOREIGN KEY (date_key) REFERENCES dwh.dim_date(date_key);

ALTER TABLE dwh.fact_income_statement 
ADD CONSTRAINT fk_fact_income_date FOREIGN KEY (date_key) REFERENCES dwh.dim_date(date_key);

ALTER TABLE dwh.fact_cash_flow 
ADD CONSTRAINT fk_fact_cash_date FOREIGN KEY (date_key) REFERENCES dwh.dim_date(date_key);
