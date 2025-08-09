from pathlib import Path
import logging
from DataOrchestrator import DataOrchestrator
from utility import get_banks_listings
import os
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

logger = logging.getLogger(__name__)
logger.setLevel("INFO")




def main():
    logger.info("Starting...")
    # Get all listing from vnstock with source 'vci'
    listings_df = get_banks_listings(os.getenv("TESTING") == "True")
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