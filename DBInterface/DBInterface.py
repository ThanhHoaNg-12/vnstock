# Nhập các thư viện cần thiết để làm việc với PostgreSQL
import psycopg
from psycopg.connection import Connection
# sql: Mô-đun giúp tạo các câu truy vấn SQL động một cách an toàn (tránh SQL Injection)
from psycopg import sql
from psycopg.rows import Row

# Nhập thư viện để xử lý bộ nhớ đệm trong RAM (quan trọng cho việc nạp dữ liệu nhanh)
from io import StringIO
import pandas as pd
from pathlib import Path
import logging

# Cấu hình hệ thống ghi nhật ký (logging) cơ bản
# Định dạng: Thời gian - Mức độ (INFO/DEBUG/ERROR) - Thông báo
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

# --- LỚP GIAO TIẾP VỚI CƠ SỞ DỮ LIỆU ---
class DBInterface:
    """
    Lớp này chịu trách nhiệm quản lý tất cả các tương tác với cơ sở dữ liệu PostgreSQL:
    - Kết nối
    - Tạo bảng
    - Nạp dữ liệu (bulk insert)
    - Truy vấn dữ liệu
    """
    def __init__(self, db_url: str, db_schema_file: Path = Path.cwd() / "schema.sql"):
        # Lấy một logger riêng cho thư viện 'psycopg' để theo dõi sát sao các hoạt động của DB
        self._logger = logging.getLogger("psycopg")
        # Đặt mức độ DEBUG để nhìn thấy chi tiết mọi câu lệnh SQL được thực thi (hữu ích khi phát triển)
        self._logger.setLevel("DEBUG")

        self._db_url = db_url
        # Thiết lập kết nối thực sự đến cơ sở dữ liệu
        self._conn: Connection = psycopg.connect(self._db_url)

        # Ngay khi khởi tạo, đảm bảo các bảng cần thiết được tạo ra dựa trên file schema
        self._create_table(db_schema_file)

    def _create_table(self, db_schema_file: Path):
        """Hàm nội bộ: Đọc file SQL và thực thi để tạo cấu trúc bảng."""
        self._logger.info(f"Creating tables from {db_schema_file=}")
        # Mở file chứa định nghĩa bảng (VD: schema.sql)
        with open(db_schema_file, "r") as f:
            # Đọc nội dung file, chuyển sang dạng bytes (utf-8) và thực thi lệnh SQL.
            # Việc dùng bytes giúp xử lý an toàn các ký tự đặc biệt nếu có.
            self._conn.execute(f.read().encode("utf-8"))
            # Commit: Xác nhận lưu các thay đổi (tạo bảng) vào database vĩnh viễn.
            self._conn.commit()

    def dump_data_to_db(self, table_name: str, df: pd.DataFrame):
        """
        Hàm quan trọng: Đổ dữ liệu từ Pandas DataFrame vào bảng SQL một cách hiệu quả nhất.
        Hàm này sử dụng kỹ thuật "Bulk Insert" thông qua lệnh COPY của PostgreSQL,
        nhanh hơn rất nhiều so với việc dùng lệnh INSERT từng dòng.
        """
        """
        Dump a pandas DataFrame from a csv file to a PostgreSQL table.
        (Phần docstring tiếng Anh gốc giữ nguyên để tham khảo)
        ...
        """
        self._logger.info(f"Dumping data to {table_name=}")
        # Tạo con trỏ (cursor) để thực thi lệnh trong một giao dịch (transaction)
        with self._conn.cursor() as cur:
            # Bắt đầu lệnh COPY.
            # Sử dụng sql.SQL và sql.Identifier để chèn tên bảng một cách an toàn,
            # tránh nguy cơ bảo mật SQL Injection.
            with cur.copy(sql.SQL("COPY {} FROM STDIN WITH CSV HEADER DELIMITER ','").format(
                sql.Identifier(table_name)
            )) as copy:
                # KỸ THUẬT TỐI ƯU TỐC ĐỘ:
                # 1. Tạo một bộ nhớ đệm trong RAM (StringIO) thay vì ghi file ra ổ cứng.
                csv_buffer = StringIO()
                # 2. Ghi dữ liệu từ DataFrame vào bộ nhớ đệm này dưới dạng CSV.
                df.to_csv(csv_buffer, sep=",", index=False)
                # 3. Đưa con trỏ đọc về đầu bộ nhớ đệm.
                csv_buffer.seek(0)
                # 4. Đọc dữ liệu từ bộ nhớ đệm và "bơm" thẳng vào luồng COPY của database.
                copy.write(csv_buffer.read())

            # Sau khi copy xong hết dữ liệu, commit giao dịch để lưu lại.
            self._conn.commit()

    def get_records_with_primary_keys(self, table_name: str, ticker: str) -> list[Row]:
        """
        Lấy các bản ghi đã tồn tại trong DB dựa trên mã chứng khoán (ticker).
        Thường dùng để kiểm tra xem dữ liệu đã có chưa trước khi nạp mới (tránh trùng lặp).
        """
        """
        Fetch records from a PostgreSQL table based on the given table name, ticker, and columns.
        (Phần docstring tiếng Anh gốc giữ nguyên)
        ...
        """
        with self._conn.cursor() as cur:
            # Thực thi truy vấn SELECT.
            # - sql.Identifier: Đảm bảo tên bảng và tên cột an toàn.
            # - %s và (ticker,): Sử dụng placeholder để truyền giá trị ticker vào an toàn.
            cur.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE {} = %s",
                ).format(sql.Identifier(table_name), sql.Identifier('ticker')),
                (ticker,),
            )
            # Trả về tất cả các dòng kết quả tìm được.
            return cur.fetchall()

    def close_connection(self):
        """Đóng kết nối database khi không dùng nữa để giải phóng tài nguyên."""
        self._conn.close()