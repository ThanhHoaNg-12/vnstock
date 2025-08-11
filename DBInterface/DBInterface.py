import psycopg
from psycopg.connection import Connection
from psycopg import sql
from io import StringIO
import pandas as pd
from pathlib import Path
import logging


logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)


class DBInterface:
    def __init__(self, db_url: str, db_schema_file: Path = Path.cwd() / "schema.sql"):
        self._logger = logging.getLogger("psycopg")
        self._logger.setLevel("DEBUG")
        self._db_url = db_url
        self._conn: Connection = psycopg.connect(self._db_url)
        self._create_table(db_schema_file)

    def _create_table(self, db_schema_file: Path):
        self._logger.info(f"Creating tables from {db_schema_file=}")
        with open(db_schema_file, "r") as f:
            # Pass in bytes of the file
            self._conn.execute(f.read().encode("utf-8"))
            self._conn.commit()

    def dump_data_to_db(self, table_name: str, df: pd.DataFrame):
        """
        Dump a pandas DataFrame from a csv file to a PostgreSQL table.

        Parameters
        ----------
        table_name: str
            The name of the PostgreSQL table to which the data should be dumped.
        df: DataFrame
            the pandas DataFrame to be dumped to the PostgreSQL table.
        Notes
        -----
        The csv file must have the same column order as the table. The csv file must have a header row.
        This function commits the transaction after dumping the data.
        """
        with self._conn.cursor() as cur:
            with cur.copy(sql.SQL("COPY {} FROM STDIN WITH CSV HEADER DELIMITER ','").format(
                sql.Identifier(table_name)
            )) as copy:
                # Convert DataFrame to StringIO
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, sep=",", index=False)
                csv_buffer.seek(0)
                copy.write(csv_buffer.read())
            self._conn.commit()


    def close_connection(self):
        self._conn.close()
