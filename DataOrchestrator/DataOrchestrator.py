import pandas as pd
from typing import Any
from datetime import datetime, timedelta
from pathlib import Path
from FinanceApi.FinanceApi import FinanceAPI
from util.utility import make_folder, write_data_to_file, get_table_schemas_from_sql, prepare_year_table, \
    prepare_dates_table, prepare_quarter_table
from DBInterface.DBInterface import DBInterface
import logging

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


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


class DataOrchestrator:
    def __init__(self, listing_df: pd.DataFrame, data_path: Path, db_url: str, db_schema_file: Path,
                 load_from_file: bool = False):
        """
        Initialize a DataOrchestrator instance.

        :param listing_df: A DataFrame containing the list of stocks to fetch data for
        :param data_path: The path to store the fetched financial data
        :param db_url: The URL of the PostgreSQL database
        :param db_schema_file: The path to the SQL file containing the database schema
        """
        self.listings_df = listing_df
        self._cur_path = data_path
        self._today = datetime.now()
        self._db_interface = DBInterface(db_url, db_schema_file)
        self._db_schema = get_table_schemas_from_sql(str(db_schema_file))
        self._load_from_file = load_from_file

    def _fetch_data_worker(self, start_date: str, end_date: str, finance_api: FinanceAPI, ticker: str) -> dict[
        str, pd.DataFrame]:
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
            make_folder(self._cur_path / ticker / end_date)
            stock_data = call_api(finance_api, ticker, start_date, end_date)
            return stock_data
        except Exception as e:
            logger.error(f"Failed to fetch data for {ticker}: {e}")
            return {}

    def _delete_unnecessary_records_from_df(self, df: pd.DataFrame, table_name: str, ticker: str, primary_keys: list[str]) -> pd.DataFrame:
        """
        Given these parameters, this function deletes unnecessary records from the dataframe
        It has to get a list of records that exist in the database based on the ticker, table_name, and the primary keys of the table
        It will then drop these records from the dataframe
        Args:
            df: Df to clean
            table_name: table_name
            ticker: ticker
            primary_keys: primary keys

        Returns: A cleaned dataframe

        """
        primary_keys_records = self._db_interface.get_records_with_primary_keys(table_name, ticker)
        if not primary_keys_records:
            return df
        db_df = pd.DataFrame(primary_keys_records, columns=df.columns)

        # Define the primary key columns for joining.
        # For financial data, this is typically a combination of ticker, year, and quarter.

        if 'date' in primary_keys:
            df['date'] = pd.to_datetime(df['date'])
            db_df['date'] = pd.to_datetime(db_df['date'])
        # Use a left merge with an indicator to identify rows from `df` that are not in `db_df`.
        # This is how you perform a left anti-join in pandas.
        merged_df = df.merge(db_df[primary_keys], on=primary_keys, how='left', indicator=True)

        # Filter to keep only the rows that are unique to the left DataFrame (`df`).
        cleaned_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])

        return cleaned_df

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
        finance_api = FinanceAPI(schema_dict=self._db_schema)
        eleven_years_ago = self._today - timedelta(days=365 * 11)
        start_date = eleven_years_ago.strftime('%Y-%m-%d')
        end_date = self._today.strftime('%Y-%m-%d')

        stock_data_list = []
        for _, row in self.listings_df.iterrows():
            ticker = row['symbol']
            if self._load_from_file:
                # Loop through the files in the folder
                cur_date_path = self._cur_path / ticker / end_date
                stock_data_dictionary: dict[str, Any] = {'ticker': ticker}
                # If the folder exists and has 6 files
                if cur_date_path.exists():
                    files_in_folder = list(cur_date_path.iterdir())
                    if len(files_in_folder) == 6:
                        for file in files_in_folder:
                            stock_data = pd.read_csv(file, sep=',', encoding='utf-8-sig')
                            stem = file.stem
                            table_name = stem.split('_', maxsplit=1)[1]
                            stock_data_dictionary[table_name] = stock_data
                    else:
                        stock_data_dictionary = self._fetch_data_worker(start_date, end_date, finance_api, ticker)
                else:
                    stock_data_dictionary = self._fetch_data_worker(start_date, end_date, finance_api, ticker)
                stock_data_list.append(stock_data_dictionary)
            else:
                stock_data = self._fetch_data_worker(start_date, end_date, finance_api, ticker)
                stock_data_list.append(stock_data)
        quarter_table = prepare_quarter_table()
        year_table = prepare_year_table(list(range(2000, 2101)))
        dates_table = prepare_dates_table(list(
            pd.date_range(datetime(year=2000, month=1, day=1), datetime(year=2100, month=1, day=1), freq='D')))
        for table, table_name, expected_rows in zip([quarter_table, year_table, dates_table], ['quarters', 'years', 'dates'], [5, 101, 36526]):
            records_number = self._db_interface.count_records(table_name)
            if records_number != expected_rows:
                self._db_interface.dump_data_to_db(table_name, table)
        logger.info("Data writing process started.")
        table_names = (k for k in stock_data_list[0].keys() if k != 'ticker')
        tables_to_dump = {k: pd.DataFrame() for k in table_names}
        for data in stock_data_list:
            ticker = data['ticker']
            symbol_folder_path = self._cur_path / ticker
            # Sort the file_paths by this criteria: The file names that have "company_profile" in the name will be dumped first
            for k in sorted(data.keys(), key=lambda x: 'company_profile' in x, reverse=True):
                df = data[k]
                if isinstance(df, pd.DataFrame):
                    file_path = symbol_folder_path / end_date / f"{ticker}_{k}.csv"
                    write_data_to_file(file_path, df)
                    try:
                        df = self._delete_unnecessary_records_from_df(df, k, ticker, self._db_schema[k]['primary_keys'])
                        tables_to_dump[k] = pd.concat([tables_to_dump[k], df], ignore_index=True)
                    except Exception as e:
                        logger.error(f"Failed with exception: {e}")
                        continue
        # Since all tables refer to the ticker in company_profile, we have to dump company_profile first

        for table_name, table in tables_to_dump.items():
            self._db_interface.dump_data_to_db(table_name, table) 
        logger.info("Data writing process complete.")


        self._db_interface.close_connection()
