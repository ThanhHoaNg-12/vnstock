from concurrent.futures import ThreadPoolExecutor, as_completed
from vnstock import Finance, Quote, Company
import time
import logging
import pandas as pd
from typing import Any, Optional

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def add_year_and_quarter_to_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Check if the dataframe has a 'year' and 'quarter' column. If not, add them.
    :param dataframe: The dataframe to check
    :return: New dataframe with 'year' and 'quarter' columns
    """
    df = dataframe.copy()
    if 'Year' in df.columns and 'Quarter' in df.columns:
        return df

    # Helper function to process a single index label
    def extract_year_quarter(index_val):
        index_str = str(index_val)
        if '-' in index_str:
            # It's a quarterly report, e.g., '2022-Q1'
            year_str, quarter_str = index_str.split('-')
            year = int(year_str)
            quarter = int(quarter_str.replace('Q', ''))
            return pd.Series([year, quarter])
        else:
            # It's an annual report, e.g., '2022'
            year = int(index_str)
            quarter = 5  # Using 5 to denote annual
            return pd.Series([year, quarter])

    # Apply the helper function to the index
    # This creates a new DataFrame with 'Year' and 'Quarter' columns
    df[['Year', 'Quarter']] = df.index.to_series().apply(extract_year_quarter)
    # Remove Report Type column
    if 'report_period' in df.columns:
        df = df.drop('report_period', axis=1)
    # Rename Year and Quarter columns
    return df

class FinanceAPI:
    def __init__(self):
        self._language = 'vi'
        self._source = 'TCBS'

    @staticmethod
    def _get_company_profile(symbol: str) -> pd.DataFrame:
        """
        Get company profile data

        :param symbol: Ticker symbol of the company
        :return: Company profile data as a DataFrame
        """
        company = Company(symbol=symbol, source='vci')
        return company.overview()

    def _get_company_cash_flow(self, symbol: str) -> pd.DataFrame:
        """
        Retrieve and merge the quarterly and annual cash flow data for a given company symbol.

        :param symbol: Ticker symbol of the company
        :return: Merged cash flow data as a DataFrame
        """
        finance = Finance(symbol=symbol, source=self._source)
        annual_data = finance.cash_flow(period="year", lang=self._language)
        return add_year_and_quarter_to_dataframe(annual_data)

    def _get_company_balance_sheet(self, symbol: str) -> pd.DataFrame:
        """
        Retrieve and merge the quarterly and annual balance sheet data for a given company symbol.

        :param symbol: Ticker symbol of the company
        :return: Merged balance sheet data as a DataFrame
        """
        finance = Finance(symbol=symbol, source=self._source)
        annual_data = finance.balance_sheet(period="year", lang=self._language)
        # Remove all columns that have 0 in all rows
        return add_year_and_quarter_to_dataframe(annual_data)

    def _get_company_income_statement(self, symbol: str) -> pd.DataFrame:
        """
        Retrieve and merge the quarterly and annual income statement data for a given company symbol.

        :param symbol: Ticker symbol of the company
        :return: Merged income statement data as a DataFrame
        """
        finance = Finance(symbol=symbol, source=self._source)
        annual_data = finance.income_statement(period="year", lang=self._language)
        # Remove all columns that have 0 in all rows
        return add_year_and_quarter_to_dataframe(annual_data)

    def _get_company_ratio(self, symbol: str) -> pd.DataFrame:
        """
        Retrieve and merge the quarterly and annual ratio data for a given company symbol.

        :param symbol: Ticker symbol of the company
        :return: Merged ratio data as a DataFrame
        """
        finance = Finance(symbol=symbol, source="vci")
        annual_data = finance.ratio(period="year", lang=self._language)
        annual_data['ticker'] = symbol
        return annual_data

    @staticmethod
    def _get_company_price_history_data(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get price history data
        :param symbol: Ticker symbol
        :param start_date: Start date
        :param end_date: End date
        :return: Price history data as a DataFrame
        """
        quote = Quote(symbol=symbol)
        price_history =quote.history(start=start_date, end=end_date)
        price_history['ticker'] = symbol
        return price_history

    def build_dict(self, ticker: str, start_date: str, end_date: str) -> dict[str, pd.DataFrame | str]:
        """
        Build a dictionary of dataframes by calling the API from vnstock
        :param ticker: Ticker symbol
        :param start_date: Start date of the price history
        :param end_date: End date of the price history
        :return: A dictionary of dataframes
        """
        function_objects = [
            {"name": "company_profile", "function": self._get_company_profile, "kwargs": {"symbol": ticker}},
            {"name": "cash_flow", "function": self._get_company_cash_flow, "kwargs": {"symbol": ticker}},
            {"name": "balance_sheet", "function": self._get_company_balance_sheet, "kwargs": {"symbol": ticker}},
            {"name": "income_statement", "function": self._get_company_income_statement, "kwargs": {"symbol": ticker}},
            {"name": "ratio", "function": self._get_company_ratio, "kwargs": {"symbol": ticker}},
            {"name": "daily_chart", "function": self._get_company_price_history_data,
             "kwargs": {"symbol": ticker, "start_date": start_date, "end_date": end_date}},
        ]

        raw_data = {"ticker": ticker}

        # for func_obj in function_objects:
        #     result_df = _fixed_delay_api_call(func_obj)
        #     if result_df is not None:
        #         raw_data[func_obj["name"]] = result_df

        with ThreadPoolExecutor(max_workers=len(function_objects)) as executor:
            future_to_name = {executor.submit(_fixed_delay_api_call, func_obj): func_obj["name"] for func_obj in function_objects}
            for future in as_completed(future_to_name):
                name = future_to_name[future]
                try:
                    result_df = future.result()
                    if result_df is not None:
                        raw_data[name] = result_df
                except Exception as exc:
                    logger.error(f'{name} generated an exception: {exc}')

        return raw_data

def _fixed_delay_api_call(function_object: dict[str, Any]) -> Optional[pd.DataFrame]:
    """Make API calls and pause the program
    when API limits has been reached.
    :param function_object: A dictionary consisting of function and kwargs
    :returns: A pandas DataFrame or None if an error occurs
    """
    start = time.perf_counter() + 1
    function = function_object['function']
    kwargs = function_object['kwargs']
    try:
        return function(**kwargs)
    except Exception as e:
        logger.error(f"Error calling API with {function=}, {kwargs=}: {e}")
        time.sleep(60)
        try:
            return function(**kwargs)
        except Exception as e2:
            logger.error(f"Error on retry for {function=}, {kwargs=}: {e2}")
            return None
    finally:
        diff = start - time.perf_counter()
        if diff > 0:
            time.sleep(diff)
