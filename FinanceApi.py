from concurrent.futures import ThreadPoolExecutor
from vnstock_data import Finance, Quote, Company
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


def merge_quarterly_and_annual_data(quarterly_data: pd.DataFrame, annual_data: pd.DataFrame) -> pd.DataFrame:
    """
    Merge quarterly and annual statement data
    :param quarterly_data: Quarterly data
    :param annual_data: Annual data
    :return: Merged data
    """
    # Add lengthReport = 5 to annual data
    annual_data["lengthReport"] = 5
    merged_data = pd.concat([quarterly_data, annual_data], ignore_index=True)
    return merged_data

class FinanceAPI:
    def __init__(self):
        self._daily_chart: Optional[pd.DataFrame] = None
        self._company_profile: Optional[pd.DataFrame]  = None
        self._cash_flow: Optional[pd.DataFrame]  = None
        self._balance_sheet: Optional[pd.DataFrame]  = None
        self._income_statement: Optional[pd.DataFrame]  = None
        self._ratio: Optional[pd.DataFrame]  = None
    def _get_company_profile(self, symbol: str) -> None:
        """
        Get company profile data

        :param symbol: Ticker symbol of the company
        :return: None, assigns the retrieved data to self._company_profile
        """
        company = Company(symbol=symbol, source="vci")
        self._company_profile = company.overview()

    def _get_company_cash_flow(self, symbol: str) -> None:
        """
        Retrieve and merge the quarterly and annual cash flow data for a given company symbol.

        :param symbol: Ticker symbol of the company
        :return: None, assigns the merged data to self._cash_flow
        """
        finance = Finance(symbol=symbol, source="vci")
        quarterly_data = finance.cash_flow(period="quarter", lang="en")
        annual_data = finance.cash_flow(period="annual", lang="en")
        self._cash_flow = merge_quarterly_and_annual_data(quarterly_data, annual_data)

    def _get_company_balance_sheet(self, symbol: str) -> None:
        """
        Retrieve and merge the quarterly and annual balance sheet data for a given company symbol.

        :param symbol: Ticker symbol of the company
        :return: None, assigns the merged data to self._balance_sheet
        """
        finance = Finance(symbol=symbol, source="vci")
        quarterly_data = finance.balance_sheet(period="quarter", lang="en")
        annual_data = finance.balance_sheet(period="annual", lang="en")
        self._balance_sheet = merge_quarterly_and_annual_data(quarterly_data, annual_data)

    def _get_company_income_statement(self, symbol: str) -> None:
        """
        Retrieve and merge the quarterly and annual income statement data for a given company symbol.

        :param symbol: Ticker symbol of the company
        :return: None, assigns the merged data to self._income_statement
        """
        finance = Finance(symbol=symbol, source="vci")
        quarterly_data = finance.income_statement(period="quarter", lang="en")
        annual_data = finance.income_statement(period="annual", lang="en")
        self._income_statement = merge_quarterly_and_annual_data(quarterly_data, annual_data)

    def _get_company_ratio(self, symbol: str) -> None:
        """
        Retrieve and merge the quarterly and annual ratio data for a given company symbol.

        :param symbol: Ticker symbol of the company
        :return: None, assigns the merged data to self._ratio
        """
        finance = Finance(symbol=symbol, source="vci")
        quarterly_data = finance.ratio(period="quarter", lang="en")
        annual_data = finance.ratio(period="annual", lang="en")
        self._ratio = merge_quarterly_and_annual_data(quarterly_data, annual_data)
    def _get_company_price_history_data(self, symbol: str, start_date: str, end_date: str) -> None:
        """
        Get price history data
        :param symbol: Ticker symbol
        :param start_date: Start date
        :param end_date: End date
        :return: None,  ssigns to self._daily_chart
        """
        quote = Quote(symbol=symbol, source="vci")
        self._daily_chart = quote.history(start=start_date, end=end_date)

    def build_dict(self, ticker: str, start_date: str, end_date: str) -> dict[str, pd.DataFrame]:
        """
        Build a dictionary of dataframes by calling the API from vnstock
        :param ticker: Ticker symbol
        :param start_date: Start date of the price history
        :param end_date: End date of the price history
        :return: A dictionary of dataframes
        """
        raw_data: dict[str, str | pd.DataFrame] = {"ticker": ticker}
        function_objects = [
            {"function": self._get_company_profile, "kwargs": {"symbol": ticker}},
            {"function": self._get_company_cash_flow, "kwargs": {"symbol": ticker}},
            {"function": self._get_company_balance_sheet, "kwargs": {"symbol": ticker}},
            {"function": self._get_company_income_statement, "kwargs": {"symbol": ticker}},
            {"function": self._get_company_ratio, "kwargs": {"symbol": ticker}},
            {"function": self._get_company_price_history_data,
             "kwargs": {"symbol": ticker, "start_date": start_date, "end_date": end_date}},
        ]

        with ThreadPoolExecutor(max_workers=len(function_objects)) as executor:
            for function_object in function_objects:
                executor.submit(_fixed_delay_api_call, function_object)

        # For testing
        # for function_object in function_objects:
        #     _fixed_delay_api_call(function_object)

        if self._daily_chart is not None:
            raw_data['daily_chart'] = self._daily_chart
        if self._company_profile is not None:
            raw_data['company_profile'] = self._company_profile
        if self._cash_flow is not None:
            raw_data['cash_flow'] = self._cash_flow
        if self._balance_sheet is not None:
            raw_data['balance_sheet'] = self._balance_sheet
        if self._income_statement is not None:
            raw_data['income_statement'] = self._income_statement
        if self._ratio is not None:
            raw_data['ratio'] = self._ratio

        return raw_data

def _fixed_delay_api_call(function_object: dict[str, Any]) -> None:
    """Make API calls and pause the program
    when API limits has been reached.
    :param function_object: A dictionary consisting of function and kwargs
    :returns: None, modifies the FinanceAPI object instead
    """
    start = time.perf_counter() + 1
    function = function_object['function']
    kwargs = function_object['kwargs']
    try:
        function(**kwargs)
    except Exception as e:
        logger.error(f"Error calling API with {function=}, {kwargs=}: {e}")
        time.sleep(60)
        function(**kwargs)
    diff = start - time.perf_counter()
    if diff > 0:
        time.sleep(diff)

