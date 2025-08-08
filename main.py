from vnstock import  Listing
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from queue import SimpleQueue, Empty
import pandas as pd
import logging
from FinanceApi import FinanceAPI
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

logger = logging.getLogger(__name__)
logger.setLevel("INFO")



def make_folder(stock_data_path: Path) -> None:
    """

    :param stock_data_path: The path to create the folder if it does not exist
    :return: None, mutates the path
    """
    if not stock_data_path.exists():
        logger.info(f"Creating folder {stock_data_path=}")
        stock_data_path.mkdir()

def write_data_to_file(stock_data_file: Path, stock_data: pd.DataFrame) -> None:
    """

    :param stock_data_file:
    :param stock_data:
    :return:  None
    """
    logger.info(f"Writing data to {stock_data_file=}")
    stock_data.to_csv(stock_data_file, index=False)

def call_api(api_client: FinanceAPI, stock: str, start_date: str, end_date: str) -> dict[str, pd.DataFrame]:
    """
    :param api_client: A FinanceAPI object
    :param stock: A str representing the stock symbol
    :param stock_data_file: a file containing the stock data
    :param today: The current date
    :return: The stock data
    """
    try:
        stock_data = api_client.build_dict(stock, start_date, end_date)
    except Exception as e:
        logger.error(e)
    else:
        return stock_data
def get_banks_listings() -> pd.DataFrame:
    """
    Get all listing from vnstock with source 'vci
    :return: pd.DataFrame
    """
    listing = Listing()

    listings_df = listing.symbols_by_industries()

    listings_df = listings_df[listings_df['icb_name3'] == 'Ngân hàng']

    return listings_df




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


def main():
    logger.info("Starting...")
    # Get all listing from vnstock with source 'vci'
    listings_df = get_banks_listings()
    logger.info(f"Found {len(listings_df)} banks")

    stock_data_folder = Path.cwd() / "StockData"
    logger.info(f"Creating folders {stock_data_folder=}")
    if not stock_data_folder.exists():
        stock_data_folder.mkdir()
    logger.info("Downloading data...")
    data_orchestrator = DataOrchestrator(listing_df=listings_df, data_path=stock_data_folder)
    data_orchestrator.run()

    logger.info("Done")

if __name__ == '__main__':
    main()