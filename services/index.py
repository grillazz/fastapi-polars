from typing import Any
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

    def __call__(self) -> "IndexService":
        """
        Makes the service callable, allowing it to be used as a dependency.

        Returns:
            IndexService: The current instance of the service.
        """
        return self

    def write_index(self, dataframe: pl.DataFrame, parquet_path_id: int) -> Any:
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
