from pathlib import Path
import logging
from dotenv import load_dotenv, find_dotenv
from DataOrchestrator.DataOrchestrator import DataOrchestrator
from util.utility import get_banks_listings, parse_boolean
import os
from datetime import datetime

# Cấu hình hệ thống ghi nhật ký (logging) và nạp biến môi trường
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)
# Tạo logger
logger = logging.getLogger(__name__)
logger.setLevel("INFO")

load_dotenv(find_dotenv())


def main():
    logger.info("Starting...")
    # Lấy danh sách các ngân hàng từ nguồn dữ liệu
    listings_df = get_banks_listings(parse_boolean(os.getenv("TESTING")))
    logger.info(f"Found {len(listings_df)} banks")
    # Tạo thư mục lưu trữ dữ liệu nếu chưa tồn tại
    stock_data_folder = Path.cwd() / "StockData"
    logger.info(f"Creating folders {stock_data_folder=}")
    today = os.getenv("DATE")
    if today == "TODAY":
        today = None
    else:
        today = datetime.strptime(today, "%Y-%m-%d")
    if not stock_data_folder.exists():
        stock_data_folder.mkdir()
    # Tạo và chạy DataOrchestrator để tải dữ liệu
    logger.info("Downloading data...")
    data_orchestrator = DataOrchestrator(listing_df=listings_df, data_path=stock_data_folder,
                                         db_url=os.getenv("DATABASE_URL"), db_schema_file=Path.cwd() / "schema.sql",
                                         load_from_file=parse_boolean(os.getenv("LOAD_FROM_FILE")), today=today)
    data_orchestrator.run()
    # Hoàn tất
    logger.info("Done")

# Chạy hàm main khi tập tin được thực thi trực tiếp
if __name__ == '__main__':
    main()
