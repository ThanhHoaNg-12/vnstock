import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from FinanceApi import FinanceAPI
from utility import make_folder, write_data_to_file, call_api
import logging
from concurrent.futures import ThreadPoolExecutor
import queue

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

class DataOrchestrator:
    def __init__(self, listing_df: pd.DataFrame, data_path: Path, max_workers: int = 10):
        self.listings_df = listing_df
        self._cur_path = data_path
        self._today = datetime.now()
        self._max_workers = max_workers

    def _fetch_data_worker(self, start_date: str, end_date: str, finance_api: FinanceAPI, ticker: str) -> dict[str, pd.DataFrame]:
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
        try:
            logger.info(f"Downloading data for {ticker}")
            make_folder(self._cur_path / ticker)
            stock_data = call_api(finance_api, ticker, start_date, end_date)
            return stock_data
        except Exception as e:
            logger.error(f"Failed to fetch data for {ticker}: {e}")
            return {}
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
        finance_api = FinanceAPI()
        eleven_years_ago = self._today - timedelta(days=365 * 11)
        start_date = eleven_years_ago.strftime('%Y-%m-%d')
        end_date = self._today.strftime('%Y-%m-%d')

        stock_data_list = []
        for _, row in self.listings_df.iterrows():
            ticker = row['symbol']
            stock_data = self._fetch_data_worker(start_date, end_date, finance_api,  ticker)
            stock_data_list.append(stock_data)

        for data in stock_data_list:
            ticker = data['ticker']
            symbol_folder_path = self._cur_path / ticker
            for k, v in data.items():
                if isinstance(v, pd.DataFrame):
                    write_data_to_file(symbol_folder_path / f"{ticker}_{k}.csv", v)
        logger.info("Data writing process complete.")
