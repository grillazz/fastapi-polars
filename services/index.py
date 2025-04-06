"""
Service module for database index operations.
Provides functionality to write Polars DataFrames to a database index table.
"""
import polars as pl
from attrs import define
from config import settings as global_settings

@define
class IndexService:
    """
    Service for writing dataframes to a database index table.

    Attributes:
        index_engine (str): Database engine type.
        index_table (str): Name of the index table.
        index_connection (str): Database connection string.
    """
    index_engine: str = global_settings.index_engine
    index_table: str = global_settings.index_table
    index_connection: str = global_settings.pg_url.unicode_string()

    def write_index(self, dataframe: pl.DataFrame, parquet_path_id: int):
        """
        Writes a Polars DataFrame to the database index table.

        Args:
            dataframe (pl.DataFrame): DataFrame to write.
            parquet_path_id (int): ID of the parquet path.

        Returns:
            Any: Result of the database write operation.

        Raises:
            Exception: If there is an error writing to the database.
        """
        dataframe = dataframe.select([
            "isbn", "pages", "author", "pid", "hash"
        ]).with_columns(
            pl.lit(parquet_path_id).alias("parquet_id")
        )
        # TODO: add logger for dataframe head for debugging
        try:
            _res = dataframe.write_database(
                table_name=self.index_table,
                connection=self.index_connection,
                engine=self.index_engine,
                if_table_exists="append"
            )
            return _res
        except Exception as e:
            print(f"Error writing to database: {e}")