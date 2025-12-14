import vnstock
import vnstock.explorer.tcbs.company as tcbs_company
import vnstock.explorer.tcbs.financial as tcbs_financial
from util.utility import fixed_delay_api_call, remove_optimize_execution_decorator, transform_df, clean_dataframe
import logging
import pandas as pd
from typing import Any

tcbs_company._BASE_URL = 'https://apiextaws.tcbs.com.vn'
tcbs_financial._BASE_URL = 'https://apiextaws.tcbs.com.vn'

# Thiết lập logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)
# Tạo logger cho module FinanceApi 
logger = logging.getLogger(__name__)
# Chỉ cho phép ghi log ở mức INFO trở lên hoặc lỗi
logger.setLevel("INFO")

# Định nghĩa lớp FinanceAPI
class FinanceAPI:
    # Khởi tạo lớp với schema_dict (cấu trúc 6 bảng dữ liệu)
    def __init__(self, schema_dict: dict[str, Any], bearer_key: str):
        # Thiết lập ngôn ngữ và nguồn dữ liệu
        remove_optimize_execution_decorator()
        self._finance = None
        self._company = None
        self._language = 'en'
        self._source = 'TCBS'
        # Lưu trữ schema_dict
        self._schema_dict = schema_dict
        self._bearer_key  = bearer_key

# Định nghĩa phương thức để lấy thông tin hồ sơ công ty
    def _get_company_profile(self, symbol: str, table_name: str) -> pd.DataFrame:
        """
        Get company profile data

        :param symbol: Ticker symbol of the company
        :return: Company profile data as a DataFrame
        """
        # Lấy dữ liệu tổng quan về công ty
        company_df = self._company.overview()
        # Đổi tên cột 'symbol' thành 'ticker'
        company_df = company_df.rename(columns={'symbol': 'ticker'})
        # Hàm clean_dataframe để kiểm tra cột và xóa hàng trùng lặp theo khóa chính 
        final_df = clean_dataframe(company_df, self._schema_dict[table_name]['columns'],
                                     self._schema_dict[table_name]['primary_keys'])
        return final_df

    def _get_company_cash_flow(self, symbol: str, table_name: str) -> pd.DataFrame:
        """
        Retrieve and merge the quarterly and annual cash flow data for a given company symbol.

        :param symbol: Ticker symbol of the company
        :return: Merged cash flow data as a DataFrame
        """
        # Lấy báo cáo lưu chuyển tiền tệ hàng năm và hàng quý
        # Tạo đối tượng Finance với symbol: mã cổ phiếu và source : nguồn dữ liệu
        annual_data = self._finance.cash_flow(period="year")
        quarterly_data = self._finance.cash_flow(period="quarter")
        # Thêm cột ticker vào dữ liệu 
        annual_data['ticker'] = symbol
        quarterly_data['ticker'] = symbol
        # hàm transform xử lý cột thời gian : Với báo cáo quý (ví dụ "Q1-2024"): Nó tách thành cột quarter = 1 và year = 2024.
        annual_data = transform_df(annual_data)
        quarterly_data = transform_df(quarterly_data)
        # Kết hợp dữ liệu hàng năm và hàng quý, sau đó làm sạch dữ liệu
        # clean_dataframe để sắp xếp đúng và xóa hàng trùng lặp theo khóa chính
        final_df = clean_dataframe(pd.concat([annual_data, quarterly_data]), self._schema_dict[table_name]['columns'],
                                   self._schema_dict[table_name]['primary_keys'])
        return final_df
# Bảng cân đối kế toán
    def _get_company_balance_sheet(self, symbol: str, table_name: str) -> pd.DataFrame:
        """
        Retrieve and merge the quarterly and annual balance sheet data for a given company symbol.

        :param symbol: Ticker symbol of the company
        :return: Merged balance sheet data as a DataFrame
        """
        annual_data = self._finance.balance_sheet(period="year")
        annual_data['ticker'] = symbol
        annual_data = transform_df(annual_data)

        quarterly_data = self._finance.balance_sheet(period="quarter")
        quarterly_data['ticker'] = symbol
        quarterly_data = transform_df(quarterly_data)
        final_df = clean_dataframe(pd.concat([annual_data, quarterly_data]), self._schema_dict[table_name]['columns'],
                                   self._schema_dict[table_name]['primary_keys'])
        return final_df
# Báo cáo kết quả hoạt động kinh doanh
    def _get_company_income_statement(self, symbol: str, table_name: str) -> pd.DataFrame:
        """
        Retrieve and merge the quarterly and annual income statement data for a given company symbol.

        :param symbol: Ticker symbol of the company
        :return: Merged income statement data as a DataFrame
        """
        annual_data = self._finance.income_statement(period="year")
        annual_data['ticker'] = symbol
        annual_data = transform_df(annual_data)

        quarterly_data = self._finance.income_statement(period="quarter")
        quarterly_data['ticker'] = symbol
        quarterly_data = transform_df(quarterly_data)
        final_df = clean_dataframe(pd.concat([annual_data, quarterly_data]), self._schema_dict[table_name]['columns'],
                                   self._schema_dict[table_name]['primary_keys'])
        return final_df
# Chỉ số tài chính
    def _get_company_ratio(self, symbol: str, table_name: str) -> pd.DataFrame:
        """
        Retrieve and merge the quarterly and annual ratio data for a given company symbol.

        :param symbol: Ticker symbol of the company
        :return: Merged ratio data as a DataFrame
        """
        annual_data = self._finance.ratio(period="year")
        quarterly_data = self._finance.ratio(period="quarter")
        annual_data['ticker'] = symbol
        quarterly_data['ticker'] = symbol
        annual_data = transform_df(annual_data)
        quarterly_data = transform_df(quarterly_data)
        final_df = clean_dataframe(pd.concat([annual_data, quarterly_data]), self._schema_dict[table_name]['columns'],
                                   self._schema_dict[table_name]['primary_keys'])
        return final_df

    #staticmethod là phương thức tĩnh không phụ thuộc vào trạng thái của đối tượng lớp
    @staticmethod
    def _get_company_price_history_data(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get price history data
        :param symbol: Ticker symbol
        :param start_date: Start date
        :param end_date: End date
        :return: Price history data as a DataFrame
        """
        #quote dùng vnstock.Quote để lấy lịch sử giá cổ phiếu
        quote = vnstock.Quote(symbol=symbol)
        # Lấy lịch sử giá cổ phiếu từ start_date đến end_date
        price_history = quote.history(start=start_date, end=end_date)
        price_history['ticker'] = symbol
        # Rename time column to date
        #đổi tên cột 'time' thành 'date'
        price_history = price_history.rename(columns={'time': 'date'})
        return price_history
    
    def build_dict(self, ticker: str, start_date: str, end_date: str) -> dict[str, Any]:
        """
        Build a dictionary of dataframes by calling the API from vnstock
        :param ticker: Ticker symbol
        :param start_date: Start date of the price history
        :param end_date: End date of the price history
        :return: A dictionary of dataframes
        """
        # Thay vì phải viết code thủ công để gọi 6 lần cho 6 loại dữ liệu khác nhau, hàm này sẽ gom tất cả vào một chỗ để xử lý tự động và gọn gàng.
        self._company = vnstock.Company(symbol=ticker, source=self._source)
        self._company.headers["Authorization"] = f"Bearer {self._bearer_key}"
        self._finance = self._company.finance
        self._finance.headers["Authorization"] = f"Bearer {self._bearer_key}"
        functions_to_call = {
            "company": (self._get_company_profile, {"symbol": ticker, "table_name": "company"}),
            "cash_flow": (self._get_company_cash_flow, {"symbol": ticker, "table_name": "cash_flow"}),
            "balance_sheet": (self._get_company_balance_sheet, {"symbol": ticker, "table_name": "balance_sheet"}),
            "income_statement": (self._get_company_income_statement,
                                 {"symbol": ticker, "table_name": "income_statement"}),
            "ratio": (self._get_company_ratio, {"symbol": ticker, "table_name": "ratio"}),
            "daily_price": (self._get_company_price_history_data,
                            {"symbol": ticker, "start_date": start_date, "end_date": end_date})
        }
# tạo dictionary response để lưu trữ kết quả trả về
        response: dict[str, Any] = {"ticker": ticker}
# lặp qua từng mục trong functions_to_call và gọi hàm tương ứng với tham số đã cho
        for key, (func, kwargs) in functions_to_call.items():
            response[key] = fixed_delay_api_call(func, **kwargs)
        return response

