# Nhập thư viện vnstock để lấy danh sách mã chứng khoán
from vnstock import Listing
from pathlib import Path
import pandas as pd
# Nhập thư viện Regular Expression (biểu thức chính quy) để xử lý văn bản mạnh mẽ
import re
import logging

# Cấu hình logging cơ bản cho các tiện ích này
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

def make_folder(stock_data_path: Path) -> None:
    """
    Hàm tiện ích: Tạo thư mục nếu nó chưa tồn tại.
    Giúp tránh lỗi khi cố gắng lưu file vào một thư mục không có thật.
    :param stock_data_path: Đường dẫn thư mục cần tạo.
    """
    if not stock_data_path.exists():
        logger.info(f"Creating folder {stock_data_path=}")
        # mkdir(parents=True, exist_ok=True) là cách an toàn nhất để tạo thư mục đa cấp
        stock_data_path.mkdir(parents=True, exist_ok=True)

def write_data_to_file(stock_data_file: Path, stock_data: pd.DataFrame) -> None:
    """
    Hàm tiện ích: Ghi một DataFrame ra file CSV.
    :param stock_data_file: Đường dẫn file đích.
    :param stock_data: Dữ liệu cần ghi.
    """
    logger.info(f"Writing data to {stock_data_file=}")
    # Sử dụng encoding='utf-8-sig' để hỗ trợ tốt tiếng Việt khi mở bằng Excel
    stock_data.to_csv(stock_data_file, index=False, encoding='utf-8-sig')


def get_banks_listings(testing: bool = False) -> pd.DataFrame:
    """
    Hàm lấy danh sách các ngân hàng cần tải dữ liệu.
    Sử dụng vnstock để lấy toàn bộ danh sách niêm yết, sau đó lọc theo ngành.
    :param testing: Nếu True, chỉ trả về 2 mã (ACB, MBB) để test nhanh.
    :return: DataFrame chứa danh sách các ngân hàng.
    """
    listing = Listing()

    # Lấy danh sách tất cả các mã chứng khoán theo ngành
    listings_df = listing.symbols_by_industries()

    if testing:
        # Chế độ test: chỉ lấy 2 mã mẫu
        listings_df = listings_df[listings_df['symbol'].isin(['ACB', 'MBB'])]
    else:
        # Chế độ thật: Lọc lấy tất cả các mã thuộc ngành 'Ngân hàng'
        listings_df = listings_df[listings_df['icb_name3'] == 'Ngân hàng']

    return listings_df

def parse_boolean(value: str) -> bool:
    """
    Hàm tiện ích: Chuyển đổi giá trị chuỗi từ file cấu hình (.env) thành kiểu boolean thật.
    Ví dụ: chuỗi "True", "true", hoặc "1" sẽ thành True.
    """
    return value in ['True', 'true', '1']

def transform_df(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Hàm xử lý quan trọng: Chuẩn hóa cột thời gian (Năm/Quý) cho các báo cáo tài chính.
    Dữ liệu từ vnstock có thể có index dạng '2022-Q1' (quý) hoặc '2022' (năm).
    Hàm này tách chúng ra thành 2 cột rõ ràng: 'year' và 'quarter'.
    """
    df = dataframe.copy()
    # Xóa các cột cũ nếu có để tránh xung đột
    drop_columns = ['report_period', 'year', 'quarter']
    for col in drop_columns:
        if col in df.columns:
            df = df.drop(col, axis=1)

    # Nếu đã có sẵn year và quarter thì không cần làm gì thêm (đề phòng)
    if 'year' in df.columns and 'quarter' in df.columns:
        return df

    # Hàm con hỗ trợ xử lý từng dòng index
    def extract_year_quarter(index_val):
        index_str = str(index_val)
        if '-' in index_str:
            # Đây là báo cáo quý, ví dụ: '2022-Q1' -> Tách thành year=2022, quarter=1
            year_str, quarter_str = index_str.split('-')
            year = int(year_str)
            quarter = int(quarter_str.replace('Q', ''))
            return pd.Series([year, quarter])
        else:
            # Đây là báo cáo năm, ví dụ: '2022' -> year=2022.
            # Quy ước quarter = 5 để biểu thị dữ liệu cả năm (annual).
            year = int(index_str)
            quarter = 5
            return pd.Series([year, quarter])

    # Áp dụng hàm con vào index để tạo ra 2 cột mới
    df[['year', 'quarter']] = df.index.to_series().apply(extract_year_quarter)

    return df

def clean_dataframe(df: pd.DataFrame, table_schema: list[str], primary_key_cols: list[str]) -> pd.DataFrame:
    """
    Hàm làm sạch dữ liệu quan trọng trước khi lưu vào DB hoặc file.
    Nhiệm vụ:
    1. Đảm bảo DataFrame có đủ các cột theo thiết kế của bảng (schema). Nếu thiếu thì thêm vào và để trống (None).
    2. Sắp xếp lại thứ tự cột cho đúng chuẩn.
    3. Loại bỏ các dòng trùng lặp dựa trên khóa chính (primary keys) để tránh lỗi khi insert vào DB.
    """
    # 1. Xử lý các cột bị thiếu
    # Tìm các cột có trong thiết kế (schema) nhưng chưa có trong dữ liệu tải về
    missing_columns = [col for col in table_schema if col not in df.columns]

    # Thêm các cột thiếu đó vào và điền giá trị None
    for col in missing_columns:
        df[col] = None
        logger.info(f"Added missing column: '{col}'")

    # Sắp xếp lại thứ tự các cột cho khớp y hệt với thiết kế trong schema.sql
    df = df[table_schema]

    # 2. Xử lý dòng trùng lặp
    # Kiểm tra xem các cột khóa chính có tồn tại trong file không
    if all(col in df.columns for col in primary_key_cols):
        # Dùng pandas để xóa các dòng trùng khóa chính, chỉ giữ lại dòng đầu tiên tìm thấy.
        initial_row_count = len(df)
        df.drop_duplicates(subset=primary_key_cols, keep='first', inplace=True)
        dropped_rows = initial_row_count - len(df)
        if dropped_rows > 0:
            logger.info(f"Removed {dropped_rows} duplicate rows based on the primary key.")
    else:
        # Nếu không đủ cột khóa chính thì cảnh báo và bỏ qua bước lọc trùng (hiếm khi xảy ra nếu code đúng)
        logger.info(f"Warning: Primary key columns {tuple(primary_key_cols)} not found in the DataFrame. Skipping duplicate removal.")

    return df

# --- Các hàm hỗ trợ tạo bảng chiều (Dimension tables) ---
# (Có thể dùng để tạo dữ liệu mẫu cho các bảng danh mục Quý, Năm, Ngày)

def prepare_quarter_table() -> pd.DataFrame:
    # Tạo bảng danh mục Quý (1, 2, 3, 4, và 5 cho cả năm)
    quarter = pd.DataFrame({'quarter': [1, 2, 3, 4, 5]})
    return quarter

def prepare_year_table(years: list[int]) -> pd.DataFrame:
    # Tạo bảng danh mục Năm dựa trên danh sách năm đầu vào
    year = pd.DataFrame({'year': years})
    return year

def prepare_dates_table(dates_list: list[pd.Timestamp]) -> pd.DataFrame:
    # Tạo bảng danh mục Ngày, có thêm cột Năm
    dates = pd.DataFrame({'date': dates_list})
    dates['year'] = dates['date'].dt.year
    return dates

# ---------------------------------------------------------

def get_table_schemas_from_sql(filepath):
    """
    Hàm phân tích file .sql để "hiểu" cấu trúc database mà không cần kết nối DB.
    Nó đọc file schema.sql và dùng biểu thức chính quy (Regex) để trích xuất:
    - Tên các bảng
    - Danh sách các cột trong mỗi bảng
    - Khóa chính (Primary Keys)
    - Khóa ngoại (Foreign Keys)

    Kết quả trả về là một dictionary mô tả cấu trúc DB, giúp DataOrchestrator biết cách lọc trùng dữ liệu.
    """
    table_schemas = {}
    current_table = None
    in_table_definition = False

    try:
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()

                # Bỏ qua dòng trống và dòng chú thích (--)
                if not line or line.startswith('--'):
                    continue

                # Tìm điểm bắt đầu định nghĩa một bảng (CREATE TABLE)
                if line.upper().startswith('CREATE TABLE'):
                    # Sử dụng Regex để bắt tên bảng một cách linh hoạt (chấp nhận có hoặc không có dấu `backticks`)
                    match = re.search(r'CREATE TABLE(?:\s+IF NOT EXISTS)?\s+`?(\w+)`?', line, re.IGNORECASE)
                    if match:
                        current_table = match.group(1)
                        # Khởi tạo cấu trúc lưu trữ cho bảng mới tìm thấy
                        table_schemas[current_table] = {
                            'columns': [],
                            'primary_keys': [],
                            'foreign_keys': []
                        }
                        in_table_definition = True
                    continue

                # Xử lý các dòng bên trong khối định nghĩa bảng
                if in_table_definition:
                    # Kiểm tra dấu hiệu kết thúc bảng );
                    if line.startswith(');'):
                        in_table_definition = False
                        current_table = None
                        continue

                    # Kiểm tra định nghĩa KHÓA CHÍNH (PRIMARY KEY)
                    if 'PRIMARY KEY' in line.upper():
                        # Dùng regex trích xuất các cột trong dấu ngoặc: PRIMARY KEY (col1, col2)
                        pk_match = re.search(r'PRIMARY KEY\s*\((.*?)\)', line, re.IGNORECASE)
                        if pk_match:
                            # Tách chuỗi bằng dấu phẩy và làm sạch tên cột
                            keys = [k.strip().replace('`', '') for k in pk_match.group(1).split(',')]
                            table_schemas[current_table]['primary_keys'].extend(keys)
                        continue # Chuyển sang dòng tiếp theo

                    # Kiểm tra định nghĩa KHÓA NGOẠI (FOREIGN KEY)
                    if 'FOREIGN KEY' in line.upper():
                        fk_match = re.search(r'FOREIGN KEY\s*\((.*?)\)', line, re.IGNORECASE)
                        if fk_match:
                            keys = [k.strip().replace('`', '') for k in fk_match.group(1).split(',')]
                            table_schemas[current_table]['foreign_keys'].extend(keys)
                        continue # Chuyển sang dòng tiếp theo

                    # Nếu không phải là định nghĩa khóa, thì nó là một dòng định nghĩa CỘT.
                    # Regex đơn giản này giả định từ đầu tiên trong dòng là tên cột.
                    col_match = re.match(r'`?(\w+)`?', line)
                    if col_match:
                        column_name = col_match.group(1)
                        # Tránh nhầm lẫn các từ khóa ràng buộc (CONSTRAINT, KEY...) là tên cột
                        if column_name.upper() not in ['CONSTRAINT', 'PRIMARY', 'FOREIGN', 'KEY']:
                             table_schemas[current_table]['columns'].append(column_name)

    except FileNotFoundError:
        print(f"Error: The file '{filepath}' was not found.")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    return table_schemas