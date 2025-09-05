from vnstock import Listing
from pathlib import Path
import pandas as pd
import re
import logging
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
    if not stock_data_file.exists():
        logger.info(f"Writing data to {stock_data_file=}")
        stock_data.to_csv(stock_data_file, index=False, encoding='utf-8-sig')



def get_banks_listings(testing: bool = False) -> pd.DataFrame:
    """
    Get all listing from vnstock with source 'vci
    :param testing: If True, only return 2 stocks
    :return: pd.DataFrame
    """
    listing = Listing()

    listings_df = listing.symbols_by_industries()

    if testing:
        listings_df = listings_df[listings_df['symbol'].isin(['ACB', 'MBB'])]
    else:
        listings_df = listings_df[listings_df['icb_name3'] == 'Ngân hàng']

    return listings_df
def parse_boolean(value: str) -> bool:
    return value in ['True', 'true', '1']

def transform_df(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Check if the dataframe has a 'year' and 'quarter' column. If not, add them.
    :param dataframe: The dataframe to check
    :return: New dataframe with 'year' and 'quarter' columns
    """
    df = dataframe.copy()
    drop_columns = ['report_period', 'year', 'quarter']
    for col in drop_columns:
        if col in df.columns:
            df = df.drop(col, axis=1)

    if 'year' in df.columns and 'quarter' in df.columns:
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
    df[['year', 'quarter']] = df.index.to_series().apply(extract_year_quarter)
    # Remove Report Type column

    # Rename Year and Quarter columns
    return df

def clean_dataframe(df: pd.DataFrame, table_schema: list[str], primary_key_cols: list[str]) -> pd.DataFrame:
    """
    Cleans a DataFrame by:
    1. Ensuring it has all columns defined in the table schema, adding missing ones with None values.
    2. Removing duplicate rows based on the primary key columns (ticker, year, quarter).

    Args:
        df (pd.DataFrame): The input DataFrame from the brokerage API.
        table_schema (list): A list of column names for the target database table.
        primary_key_cols (list): A list of column names that form the primary key for the table.

    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    # 1. Handle missing columns
    # Find columns in the schema that are not in the DataFrame
    missing_columns = [col for col in table_schema if col not in df.columns]

    # Add the missing columns to the DataFrame and fill with None
    for col in missing_columns:
        df[col] = None
        logger.info(f"Added missing column: '{col}'")

    # Reorder the columns to match the schema
    df = df[table_schema]

    # 2. Handle duplicate rows
    # Check if the primary key columns exist in the DataFrame
    if all(col in df.columns for col in primary_key_cols):
        # Drop duplicates, keeping the first occurrence
        initial_row_count = len(df)
        df.drop_duplicates(subset=primary_key_cols, keep='first', inplace=True)
        dropped_rows = initial_row_count - len(df)
        if dropped_rows > 0:
            logger.info(f"Removed {dropped_rows} duplicate rows based on the primary key.")
    else:
        logger.info(f"Warning: Primary key columns {tuple(primary_key_cols)} not found in the DataFrame. Skipping duplicate removal.")

    return df

def prepare_quarter_table() -> pd.DataFrame:
    # Prepare dim tables
    # Quarter table has 1 column quarter and 5 values from 1 to 5
    quarter = pd.DataFrame({'quarter': [1, 2, 3, 4, 5]})
    return quarter

def prepare_year_table(years: list[int]) -> pd.DataFrame:
    # Prepare dim tables
    # Year table has 1 column year and
    year = pd.DataFrame({'year': years})
    return year

def prepare_dates_table(dates_list: list[pd.Timestamp]) -> pd.DataFrame:
    # Prepare dates table
    dates = pd.DataFrame({'date': dates_list})
    # add the year column
    dates['year'] = dates['date'].dt.year
    return dates

def get_table_schemas_from_sql(filepath):
    """
    Parses a .sql file to extract table names, column names, primary keys, and foreign keys.

    Args:
        filepath (str): The path to the SQL schema file.

    Returns:
        dict: A dictionary where keys are table names and values are another
              dictionary containing lists of 'columns', 'primary_keys', and 'foreign_keys'.
    """
    table_schemas = {}
    current_table = None
    in_table_definition = False

    try:
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()

                # Skip empty lines and comments
                if not line or line.startswith('--'):
                    continue

                # Find the start of a CREATE TABLE statement
                if line.upper().startswith('CREATE TABLE'):
                    # Use regex to robustly find the table name, handling backticks and "IF NOT EXISTS"
                    match = re.search(r'CREATE TABLE(?:\s+IF NOT EXISTS)?\s+`?(\w+)`?', line, re.IGNORECASE)
                    if match:
                        current_table = match.group(1)
                        table_schemas[current_table] = {
                            'columns': [],
                            'primary_keys': [],
                            'foreign_keys': []
                        }
                        in_table_definition = True
                    continue

                # Process lines within a table definition
                if in_table_definition:
                    # Check for the end of the table definition
                    if line.startswith(');'):
                        in_table_definition = False
                        current_table = None
                        continue

                    # Check for PRIMARY KEY constraint
                    if 'PRIMARY KEY' in line.upper():
                        # Extract column names from PRIMARY KEY (col1, col2)
                        pk_match = re.search(r'PRIMARY KEY\s*\((.*?)\)', line, re.IGNORECASE)
                        if pk_match:
                            # Split by comma and strip quotes/spaces
                            keys = [k.strip().replace('`', '') for k in pk_match.group(1).split(',')]
                            table_schemas[current_table]['primary_keys'].extend(keys)
                        continue # Move to the next line

                    # Check for FOREIGN KEY constraint
                    if 'FOREIGN KEY' in line.upper():
                        # Extract column name from FOREIGN KEY (col_name) REFERENCES ...
                        fk_match = re.search(r'FOREIGN KEY\s*\((.*?)\)', line, re.IGNORECASE)
                        if fk_match:
                            keys = [k.strip().replace('`', '') for k in fk_match.group(1).split(',')]
                            table_schemas[current_table]['foreign_keys'].extend(keys)
                        continue # Move to the next line

                    # If it's not a key, it's a column definition.
                    # This regex is simple and assumes the column name is the first word.
                    # It handles backticks and ignores constraints on the same line.
                    col_match = re.match(r'`?(\w+)`?', line)
                    if col_match:
                        column_name = col_match.group(1)
                        # Avoid adding constraint keywords as columns
                        if column_name.upper() not in ['CONSTRAINT', 'PRIMARY', 'FOREIGN', 'KEY']:
                             table_schemas[current_table]['columns'].append(column_name)

    except FileNotFoundError:
        print(f"Error: The file '{filepath}' was not found.")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    return table_schemas

