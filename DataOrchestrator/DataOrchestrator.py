import pandas as pd
from typing import Any
from datetime import datetime, timedelta
from pathlib import Path
from FinanceApi.FinanceApi import FinanceAPI
from util.utility import make_folder, write_data_to_file, get_table_schemas_from_sql
from DBInterface.DBInterface import DBInterface
import logging

# Cấu hình hệ thống ghi nhật ký (logging)
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)
# Tạo logger riêng cho DataOrchestrator
logger = logging.getLogger(__name__)
logger.setLevel("INFO")

# Hàm gọi API để lấy dữ liệu cổ phiếu
def call_api(api_client: FinanceAPI, stock: str, start_date: str, end_date: str) -> dict[str, pd.DataFrame]:
    """
    :param api_client: A FinanceAPI object
    :param stock: A str representing the stock symbol
    :param start_date: Start date in the format YYYY-MM-DD
    :param end_date: End date in the format YYYY-MM-DD
    :return: The stock data
    """

    try:
        stock_data = api_client.build_dict(stock, start_date, end_date)
    # Xử lý ngoại lệ nếu có lỗi xảy ra trong quá trình gọi API
    except Exception as e:
        logger.error(e)
        return {}
    else:
        return stock_data

# Lớp DataOrchestrator để điều phối việc lấy và lưu trữ dữ liệu cổ phiếu
class DataOrchestrator:
    # Khởi tạo đối tượng DataOrchestrator
    def __init__(self, listing_df: pd.DataFrame, data_path: Path, db_url: str, db_schema_file: Path,
                 load_from_file: bool = False):
        """
        Initialize a DataOrchestrator instance.

        :param listing_df: A DataFrame containing the list of stocks to fetch data for
        :param data_path: The path to store the fetched financial data
        :param db_url: The URL of the PostgreSQL database
        :param db_schema_file: The path to the SQL file containing the database schema
        """
        #Lấy danh sách 27 ngân hàng (listing_df) mà main.py đưa cho, và gán vào biến self.listings_df
        self.listings_df = listing_df
        # Lưu trữ đường dẫn hiện tại để lưu dữ liệu
        self._cur_path = data_path
        # Lưu trữ ngày hiện tại
        self._today = datetime.now()
        # Khởi tạo giao diện cơ sở dữ liệu
        self._db_interface = DBInterface(db_url, db_schema_file)
        self._db_schema = get_table_schemas_from_sql(str(db_schema_file))
        self._load_from_file = load_from_file

    # Làm việc với từng cổ phiếu để lấy dữ liệu
    def _fetch_data_worker(self, start_date: str, end_date: str, finance_api: FinanceAPI, ticker: str) -> dict[
        str, pd.DataFrame]:
        """
        Worker function for fetching financial data for a given ticker.

        This function fetches financial data from the FinanceAPI instance for the given ticker
        and date range. It logs the progress and any errors that may occur during the fetch
        process, and puts the fetched data or None (to signal an error) into the specified
        results queue.

        Parameters
        ----------
        ticker: str
            The ticker symbol for the stock
        start_date: str
            The start date for the fetch operation
        end_date: str
            The end date for the fetch operation
        finance_api: FinanceAPI
            The FinanceAPI instance for making API calls
        ticker: str
            The ticker symbol for the stock
        """
        #Logger thông báo bắt đầu tải dữ liệu cho mã cổ phiếu cụ thể
        try:
            logger.info(f"Downloading data for {ticker}")
        # tạo thư mục lưu trữ dữ liệu nếu chưa tồn tại
            make_folder(self._cur_path / ticker)
            make_folder(self._cur_path / ticker / end_date)
            # Gọi API để lấy dữ liệu cổ phiếu
            stock_data = call_api(finance_api, ticker, start_date, end_date)
            return stock_data
        except Exception as e:
            logger.error(f"Failed to fetch data for {ticker}: {e}")
            return {}
# Xóa các bản ghi không cần thiết khỏi dataframe trước khi lưu vào cơ sở dữ liệu
    def _delete_unnecessary_records_from_df(self, df: pd.DataFrame, table_name: str, ticker: str, primary_keys: list[str]) -> pd.DataFrame:
        """
        Given these parameters, this function deletes unnecessary records from the dataframe
        It has to get a list of records that exist in the database based on the ticker, table_name, and the primary keys of the table
        It will then drop these records from the dataframe
        Args:
            df: Df to clean
            table_name: table_name
            ticker: ticker
            primary_keys: primary keys

        Returns: A cleaned dataframe

        """
        # Lấy các bản ghi từ cơ sở dữ liệu dựa trên khóa chính
        primary_keys_records = self._db_interface.get_records_with_primary_keys(table_name, ticker)
        if not primary_keys_records:
            return df
        # Tạo dataframe từ các bản ghi khóa chính
        db_df = pd.DataFrame(primary_keys_records, columns=df.columns)

        #Định nghĩa các cột khóa chính để nối
        # Cho dữ liệu tài chính, thường là sự kết hợp của ticker, year và quarter.

        #chuẩn hóa cột 'date' nếu có trong khóa chính
        if 'date' in primary_keys:
            df['date'] = pd.to_datetime(df['date'])
            db_df['date'] = pd.to_datetime(db_df['date'])
        # Use a left merge with an indicator to identify rows from `df` that are not in `db_df`.
        # This is how you perform a left anti-join in pandas.

        # 1. Trộn bảng df và db_df dựa trên các khóa chính với phương pháp nối 'left' và chỉ định cột chỉ báo
        merged_df = df.merge(db_df[primary_keys], on=primary_keys, how='left', indicator=True)

        # Lọc các bản ghi chỉ tồn tại trong df (không có trong db_df)
        cleaned_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])
        #Trả về bảng cleaned_df chỉ chứa dữ liệu thực sự mới.
        return cleaned_df

    # Chạy quá trình điều phối dữ liệu
    def run(self):
        """
        Execute the data orchestration process for fetching and storing financial data.

        This method initializes a FinanceAPI instance and a results queue, calculates date ranges,
        and uses a ThreadPoolExecutor to fetch financial data for each ticker in the listings.
        The fetched data is processed and written to CSV files in the specified data path.

        The process involves:
        - Calculating the date range from eleven years ago to today.
        - Submitting fetch tasks for each ticker using a worker function.
        - Collecting results from the queue and saving data to files.

        The method logs the progress and completion of data fetching and writing operations.
        """
        # Khởi tạo FinanceAPI để gọi API lấy dữ liệu
        finance_api = FinanceAPI(schema_dict=self._db_schema)
        # Tính toán khoảng thời gian từ 11 năm trước đến ngày hiện tại
        eleven_years_ago = self._today - timedelta(days=365 * 11)
        start_date = eleven_years_ago.strftime('%Y-%m-%d')
        end_date = self._today.strftime('%Y-%m-%d')

        stock_data_list = []
        # VÒNG LẶP: Đi từng ngân hàng một
        for _, row in self.listings_df.iterrows():
            ticker = row['symbol']
            #1. Kiểm tra có dùng được file cũ không (nếu có thì load từ file cũ)
            if self._load_from_file:
                # Loop through the files in the folder
                cur_date_path = self._cur_path / ticker / end_date
                stock_data_dictionary: dict[str, Any] = {'ticker': ticker}
                # If the folder exists and has 6 files
                # kiểm tra thư mục hiện tại có tồn tại không
                if cur_date_path.exists():
                    files_in_folder = list(cur_date_path.iterdir())
                    #Kiểm tra xem có đủ 6 file không
                    if len(files_in_folder) == 6:
                        # Nếu có đủ 6 file, đọc từng file và lưu vào dictionary
                        for file in files_in_folder:
                            stock_data = pd.read_csv(file, sep=',', encoding='utf-8-sig')
                            stem = file.stem
                            table_name = stem.split('_', maxsplit=1)[1]
                            stock_data_dictionary[table_name] = stock_data
                    # Nếu không đủ 6 file, gọi API để lấy dữ liệu mới
                    else:
                        stock_data_dictionary = self._fetch_data_worker(start_date, end_date, finance_api, ticker)
                # Nếu thư mục không tồn tại, gọi API để lấy dữ liệu mới           
                else:
                    stock_data_dictionary = self._fetch_data_worker(start_date, end_date, finance_api, ticker)
                stock_data_list.append(stock_data_dictionary)
            #2. Nếu không dùng file cũ thì gọi API để lấy dữ liệu mới
            else:
                stock_data = self._fetch_data_worker(start_date, end_date, finance_api, ticker)
            # Thêm dữ liệu cổ phiếu vào danh sách  
                stock_data_list.append(stock_data)

        # Chuẩn bị ghi dữ liệu vào tệp và cơ sở dữ liệu
        logger.info("Data writing process started.")
        table_names = (k for k in stock_data_list[0].keys() if k != 'ticker')
        tables_to_dump = {k: pd.DataFrame() for k in table_names}
        # VÒNG LẶP: Ghi dữ liệu từng ngân hàng một
        for data in stock_data_list:
            ticker = data['ticker']
            symbol_folder_path = self._cur_path / ticker
            # Sort the file_paths by this criteria: The file names that have "company_profile" in the name will be dumped first
            #sắp xếp các bảng để ghi vào tệp, ưu tiên bảng company_profile trước
            for k in sorted(data.keys(), key=lambda x: 'company' in x, reverse=True):
                df = data[k]
                # Kiểm tra nếu df là một DataFrame hợp lệ
                if isinstance(df, pd.DataFrame):
                    #Lưu file ra ổ cứng 
                    file_path = symbol_folder_path / end_date / f"{ticker}_{k}.csv"
                    write_data_to_file(file_path, df)
                    #lọc trùng và gom vào bảng chung 
                    try:
                        df = self._delete_unnecessary_records_from_df(df, k, ticker, self._db_schema[k]['primary_keys']) 
                        tables_to_dump[k] = pd.concat([tables_to_dump[k], df], ignore_index=True)
                    except Exception as e:
                        logger.error(f"Failed with exception: {e}")
                        continue
        # Since all tables refer to the ticker in company_profile, we have to dump company_profile first
        # Sắp xếp một lần nữa để đảm bảo bảng company_profile được ghi vào cơ sở dữ liệu trước
        for table_name, table in sorted(tables_to_dump.items(), key=lambda x: 'company' in x[0], reverse=True):
            self._db_interface.dump_data_to_db(table_name, table) 
        # Hoàn tất quá trình ghi dữ liệu
        logger.info("Data writing process complete.")


        self._db_interface.close_connection()
