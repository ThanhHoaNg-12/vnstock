from concurrent.futures import ThreadPoolExecutor, as_completed
from vnstock import Finance, Quote, Company
from vnai.beam.quota import RateLimitExceeded
import time
import logging
import pandas as pd
from typing import Any

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def transform_df(dataframe: pd.DataFrame) -> pd.DataFrame:
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

    drop_columns = ['report_period', 'year', 'quarter']
    for col in drop_columns:
        if col in df.columns:
            df = df.drop(col, axis=1)
    # Rename Year and Quarter columns
    return df

class FinanceAPI:
    def __init__(self):
        self._language = 'en'
        self._source = 'TCBS'

    @staticmethod
    def _get_company_profile(symbol: str) -> pd.DataFrame:
        """
        Get company profile data

        :param symbol: Ticker symbol of the company
        :return: Company profile data as a DataFrame
        """
        company = Company(symbol=symbol, source="vci")
        return company.overview()

    def _get_company_cash_flow(self, symbol: str) -> pd.DataFrame:
        """
        Retrieve and merge the quarterly and annual cash flow data for a given company symbol.

        :param symbol: Ticker symbol of the company
        :return: Merged cash flow data as a DataFrame
        """
        finance = Finance(symbol=symbol, source=self._source)
        annual_data = finance.cash_flow(period="year", lang=self._language)
        quarterly_data = finance.cash_flow(period="quarter", lang=self._language)
        annual_data['ticker'] = symbol
        quarterly_data['ticker'] = symbol
        annual_data = transform_df(annual_data)
        quarterly_data = transform_df(quarterly_data)
        return pd.concat([annual_data, quarterly_data])

    def _get_company_balance_sheet(self, symbol: str) -> pd.DataFrame:
        """
        Retrieve and merge the quarterly and annual balance sheet data for a given company symbol.

        :param symbol: Ticker symbol of the company
        :return: Merged balance sheet data as a DataFrame
        """
        finance = Finance(symbol=symbol, source=self._source)
        annual_data = finance.balance_sheet(period="year", lang=self._language)
        annual_data['ticker'] = symbol
        annual_data = transform_df(annual_data)

        quarterly_data = finance.balance_sheet(period="quarter", lang=self._language)
        quarterly_data['ticker'] = symbol
        quarterly_data = transform_df(quarterly_data)
        return pd.concat([annual_data, quarterly_data])

    def _get_company_income_statement(self, symbol: str) -> pd.DataFrame:
        """
        Retrieve and merge the quarterly and annual income statement data for a given company symbol.

        :param symbol: Ticker symbol of the company
        :return: Merged income statement data as a DataFrame
        """
        finance = Finance(symbol=symbol, source=self._source)
        annual_data = finance.income_statement(period="year", lang=self._language)
        annual_data['ticker'] = symbol
        annual_data = transform_df(annual_data)

        quarterly_data = finance.income_statement(period="quarter", lang=self._language)
        quarterly_data['ticker'] = symbol
        quarterly_data = transform_df(quarterly_data)
        return pd.concat([annual_data, quarterly_data])

    def _get_company_ratio(self, symbol: str) -> pd.DataFrame:
        """
        Retrieve and merge the quarterly and annual ratio data for a given company symbol.

        :param symbol: Ticker symbol of the company
        :return: Merged ratio data as a DataFrame
        """
        finance = Finance(symbol=symbol, source=self._source)
        annual_data = finance.ratio(period="year", lang=self._language)
        quarterly_data = finance.ratio(period="quarter", lang=self._language)
        annual_data = transform_df(annual_data)
        quarterly_data = transform_df(quarterly_data)
        return pd.concat([annual_data, quarterly_data])


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

    def build_dict(self, ticker: str, start_date: str, end_date: str) -> dict[str, Any]:
        """
        Build a dictionary of dataframes by calling the API from vnstock
        :param ticker: Ticker symbol
        :param start_date: Start date of the price history
        :param end_date: End date of the price history
        :return: A dictionary of dataframes
        """
        functions_to_call = {
            "company_profile": (self._get_company_profile, {"symbol": ticker}),
            "cash_flow": (self._get_company_cash_flow, {"symbol": ticker}),
            "balance_sheet": (self._get_company_balance_sheet, {"symbol": ticker}),
            "income_statement": (self._get_company_income_statement, {"symbol": ticker}),
            "ratio": (self._get_company_ratio, {"symbol": ticker}),
            "daily_chart": (self._get_company_price_history_data, {"symbol": ticker, "start_date": start_date, "end_date": end_date})
        }

        response = {"ticker": ticker}


        for key, (func, kwargs) in functions_to_call.items():
            response[key] = _fixed_delay_api_call(func, **kwargs)
        return response

def _fixed_delay_api_call(function, **kwargs) -> pd.DataFrame:
    """Make API calls and pause the program
    when API limits has been reached.
    :param function: The function to call
    :param kwargs: Keyword arguments for the function
    :returns: A pandas DataFrame or None if an error occurs
    """
    start = time.perf_counter() + 1
    try:
        result = function(**kwargs)
    except RateLimitExceeded as e:
        logger.error(f"Error calling API with {function=}, {kwargs=}: {e}")
        time.sleep(e.retry_after)
        result = function(**kwargs)
    except Exception as e:
        logger.error(f"Error calling API with {function=}, {kwargs=}: {e}")
        time.sleep(60)
        result = function(**kwargs)
    else:
        diff = start - time.perf_counter()
        if diff > 0:
            time.sleep(diff + 1)
        else:
            time.sleep(1)
    return result
