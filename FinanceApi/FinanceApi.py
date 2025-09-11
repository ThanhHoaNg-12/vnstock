from vnstock import Finance, Quote, Company
from vnai.beam.quota import RateLimitExceeded
import time
import logging
import pandas as pd
from typing import Any
from util.utility import transform_df, clean_dataframe

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


class FinanceAPI:
    def __init__(self, schema_dict: dict[str, Any]):
        self._language = 'en'
        self._source = 'TCBS'
        self._schema_dict = schema_dict

    def _get_company_profile(self, symbol: str, table_name: str) -> pd.DataFrame:
        """
        Get company profile data

        :param symbol: Ticker symbol of the company
        :return: Company profile data as a DataFrame
        """
        company = Company(symbol=symbol, source=self._source)
        # Rename symbol to ticker
        company_df = company.overview()
        company_df = company_df.rename(columns={'symbol': 'ticker'})
        final_df = clean_dataframe(company_df, self._schema_dict[table_name]['columns'],
                                     self._schema_dict[table_name]['primary_keys'])
        return final_df

    def _get_company_cash_flow(self, symbol: str, table_name: str) -> pd.DataFrame:
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
        final_df = clean_dataframe(pd.concat([annual_data, quarterly_data]), self._schema_dict[table_name]['columns'],
                                   self._schema_dict[table_name]['primary_keys'])
        return final_df

    def _get_company_balance_sheet(self, symbol: str, table_name: str) -> pd.DataFrame:
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
        final_df = clean_dataframe(pd.concat([annual_data, quarterly_data]), self._schema_dict[table_name]['columns'],
                                   self._schema_dict[table_name]['primary_keys'])
        return final_df

    def _get_company_income_statement(self, symbol: str, table_name: str) -> pd.DataFrame:
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
        final_df = clean_dataframe(pd.concat([annual_data, quarterly_data]), self._schema_dict[table_name]['columns'],
                                   self._schema_dict[table_name]['primary_keys'])
        return final_df

    def _get_company_ratio(self, symbol: str, table_name: str) -> pd.DataFrame:
        """
        Retrieve and merge the quarterly and annual ratio data for a given company symbol.

        :param symbol: Ticker symbol of the company
        :return: Merged ratio data as a DataFrame
        """
        finance = Finance(symbol=symbol, source=self._source)
        annual_data = finance.ratio(period="year", lang=self._language)
        quarterly_data = finance.ratio(period="quarter", lang=self._language)
        annual_data['ticker'] = symbol
        quarterly_data['ticker'] = symbol
        annual_data = transform_df(annual_data)
        quarterly_data = transform_df(quarterly_data)
        final_df = clean_dataframe(pd.concat([annual_data, quarterly_data]), self._schema_dict[table_name]['columns'],
                                   self._schema_dict[table_name]['primary_keys'])
        return final_df

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
        price_history = quote.history(start=start_date, end=end_date)
        price_history['ticker'] = symbol
        # Rename time column to date
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

        response: dict[str, Any] = {"ticker": ticker}

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
