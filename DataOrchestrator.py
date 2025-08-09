import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from FinanceApi import FinanceAPI
from utility import make_folder, write_data_to_file, call_api
import logging

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

logger = logging.getLogger(__name__)
logger.setLevel("INFO")
class DataOrchestrator:
    def __init__(self, listing_df: pd.DataFrame, data_path: Path):
        self.listings_df = listing_df
        self._tickers_data = {}
        self._cur_path = data_path
        self._stock_data_list: list[dict[str, pd.DataFrame]] = []
        self._today = datetime.now()


    def run(self):
        finance_api = FinanceAPI()
        for index, row in self.listings_df.iterrows():
            ticker = row['symbol']

            make_folder(self._cur_path / ticker)
            eleven_years_ago = self._today - timedelta(days=365 * 11)
            start_date = eleven_years_ago.strftime('%Y-%m-%d')
            end_date = self._today.strftime('%Y-%m-%d')
            logger.info(f"Downloading data for {ticker}")
            stock_data = call_api(finance_api, ticker, start_date, end_date)
            self._stock_data_list.append(stock_data)


        for data in self._stock_data_list:
            ticker = data['ticker']
            symbol_folder_path = self._cur_path / ticker
            for k, v in data.items():
                if type(v) is pd.DataFrame:
                    write_data_to_file(symbol_folder_path / f"{ticker}_{k}.csv", v)
