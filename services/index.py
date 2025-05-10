"""
Services for database indexing operations.

This module provides the IndexService singleton class that handles writing
Polars DataFrames to a database with built-in retry functionality for
handling transient database errors.
"""

from typing import Any
import polars as pl
from attrs import define
from tenacity import retry, wait_fixed, stop_after_attempt
from config import settings as global_settings
from services.utlis import SingletonMetaNoArgs


@define
class IndexService(metaclass=SingletonMetaNoArgs):
    """
    A singleton service for writing data to a database index.

    This service handles the indexing of dataframes to a configured database table
    using the Polars library. Implements automatic retry functionality for resilience
    against transient database errors.

    Attributes:
        index_engine (str): Database engine to use (e.g., 'adbc').
        index_table (str): Name of the database table to write to.
        index_connection (str): Database connection string or URI.
    """
    index_engine: str = global_settings.index_engine
    index_table: str = global_settings.index_table
    # index_connection: str = global_settings.pg_url.unicode_string()
    index_connection: str = global_settings.SQLITE_DB

    def __call__(self) -> "IndexService":
        """
        Returns the singleton instance of this service.

        Returns:
            IndexService: The singleton instance.
        """
        return self

    @retry(wait=wait_fixed(1), stop=stop_after_attempt(7))
    def write_index(self, dataframe: pl.DataFrame, parquet_path_id: int) -> Any:
        """
        Write selected columns from a DataFrame to the configured database table.

        Selects specific columns from the input DataFrame, adds a parquet_id column,
        and writes the result to the database. Will automatically retry up to 7 times
        with a 1 second delay between attempts if the operation fails.

        Args:
            dataframe (pl.DataFrame): Source DataFrame to extract data from.
            parquet_path_id (int): ID to associate with all records in this batch.

        Returns:
            Any: Result of the database write operation.

        Raises:
            Exception: If writing to the database fails after all retry attempts.
        """
        dataframe = dataframe.select(
            ["isbn", "pages", "author", "pub_date", "pid", "hash"]
        ).with_columns(pl.lit(parquet_path_id).alias("parquet_id"))
        try:
            _res = dataframe.write_database(
                table_name=self.index_table,
                connection=self.index_connection,
                engine=self.index_engine,
                if_table_exists="append",
            )
            return _res
        except Exception as e:
            print(f"Error writing to database: {e}")
            raise