from vnstock import Listing
from pathlib import Path
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
    stock_data.to_csv(stock_data_file, index=False, encoding='utf-8-sig')

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
    except Exception as e:
        logger.error(e)
        return {}
    else:
        return stock_data
def get_banks_listings() -> pd.DataFrame:
    """
    Get all listing from vnstock with source 'vci
    :return: pd.DataFrame
    """
    listing = Listing()

    listings_df = listing.symbols_by_industries()

    listings_df = listings_df[listings_df['symbol'] == 'ACB']

    return listings_df
