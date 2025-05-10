from typing import Any
import time
import polars as pl
from attrs import define
from tenacity import retry, wait_fixed, wait_random_exponential, stop_after_attempt
from config import settings as global_settings
from services.utlis import SingletonMetaNoArgs


@define
class IndexService(metaclass=SingletonMetaNoArgs):
    index_engine: str = global_settings.index_engine
    index_table: str = global_settings.index_table
    # index_connection: str = global_settings.pg_url.unicode_string()
    index_connection: str = global_settings.SQLITE_DB

    def __call__(self) -> "IndexService":
        return self

    @retry(wait=wait_fixed(1), stop=stop_after_attempt(7))
    def write_index(self, dataframe: pl.DataFrame, parquet_path_id: int) -> Any:
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
