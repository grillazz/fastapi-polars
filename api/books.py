import hashlib
import logging
import os

from fastapi import Request, APIRouter, Depends, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse

from models.parquet import ParquetIndex
from schemas.pydantic import BookSchema
from schemas.polars import pl_book_schema
import polars as pl
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from services.files import FilenameGeneratorService, get_filename_generator_service
from services.s3 import S3Service
from services.index import IndexService
from services.database import DatabaseService
from config import settings as global_settings

router = APIRouter()

logger = logging.getLogger(__name__)


async def append_to_parquet_file(dataframe: pl.DataFrame, file_path: str):
    """
    Appends a Polars DataFrame to an existing Parquet file on the file system.

    Args:
        dataframe (pl.DataFrame): The DataFrame to append.
        file_path (str): The path to the Parquet file.
    """
    try:
        result_df = dataframe
        if os.path.exists(file_path):
            result_df = pl.concat([pl.read_parquet(file_path), dataframe])
        result_df.write_parquet(file_path)
        return True
    except Exception as e:
        logger.error(f"Error appending to Parquet file: {e}")
        return False


def remove_daily_parquet_file(file_path: str) -> bool:
    """
    Removes a daily Parquet file from the filesystem.

    Args:
        file_path (str): Path to the Parquet file to be removed.

    Returns:
        bool: True if the file was successfully removed or didn't exist, False if removal failed.
    """
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Successfully removed Parquet file: {file_path}")
            return True
        else:
            logger.info(f"Parquet file not found for removal: {file_path}")
            return True
    except Exception as e:
        logger.error(f"Error removing Parquet file {file_path}: {e}")
        return False


@router.get(
    "/v1/current_stats",
    summary="Get current statistics about the DataFrame in the application state.",
)
async def get_statistics_about_frame(request: Request):
    """
    Root endpoint to display a welcome message and information about the current DataFrame.

    This endpoint checks if a DataFrame is stored in the application state under the name specified
    in the global settings. If the DataFrame exists, it returns its estimated size and the count of
    the 'key' column. If the DataFrame does not exist, it returns a message indicating that no
    DataFrame is defined yet.

    Args:
        request (Request): The FastAPI request object.

    Returns:
        dict: A dictionary containing a welcome message and DataFrame information if available.
    """
    try:
        dataframe = getattr(request.app, global_settings.dataframe_name)
        _s = dataframe.estimated_size(unit="mb")
        _c = dataframe.get_column("isbn").count()
        return {"message": f"Welcome to Grizzly Rest API. {_s=} {_c=}"}
    except AttributeError:
        return {"message": "Welcome to Grizzly Rest API. No dataframe defined yet."}


@router.get("/v1/filter_parquets")
async def filter_parquets(
    bucket: str,
    file_name: str,
    value: int,
    s3: S3Service = Depends(),
):
    """
    Endpoint to filter Parquet files in S3 based on a specific column and value.

    Args:
        bucket (str): The S3 bucket name.
        file_name (str): The Parquet file name to filter.
        column (str): The column to filter on.
        value (str): The value to filter for.
        s3 (S3Service): The S3 service dependency.

    Returns:
        dict: Filtered data and metadata about the scan operation.
    """
    path = "s3://daily/*.parquet"
    # if not s3.parquet_file_exists(path):
    #     return {"error": f"Parquet file '{file_name}' not found in bucket '{bucket}'"}

    # Create a lazy query with filtering
    lazy_df = pl.scan_parquet(
        path,
        storage_options=dict(
            endpoint_url=s3.s3_url,
            aws_access_key_id=s3.s3_key,
            aws_secret_access_key=s3.s3_secret,
        ),
        allow_missing_columns=True,
    )
    filtered_df = (
        lazy_df.select("isbn", "pages")
        .filter(pl.col("pages") < value)
        .collect(streaming=True)
    )

    row_count = filtered_df.height

    return {
        "data": filtered_df.to_dicts(),
        "metadata": {
            "row_count": row_count,
            "columns": filtered_df.columns,
        },
    }


@router.post("/v1/ingest_data")
async def ingest_data_into_frame(
    data: list[BookSchema],
    request: Request,
    index: IndexService = Depends(),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    s3: S3Service = Depends(),
    filename_generator: FilenameGeneratorService = Depends(
        get_filename_generator_service
    ),
):
    _pl_data_frame = pl.DataFrame(
        [
            {
                "isbn": _d.isbn,
                "description": _d.description,
                "pages": _d.pages,
                "author": _d.author,
                "pub_date": _d.pub_date,
                "pid": os.getpid(),
                "hash": hash(_d.isbn + str(_d.pages) + _d.author),
                # TODO: will be more deterministic ? "hash": hashlib.sha256((_d.isbn + str(_d.pages) + _d.author).encode()).hexdigest()
            }
            for _d in data
        ]
    )  # Convert input data to a Polars DataFrame
    if not hasattr(request.app, global_settings.dataframe_name):
        setattr(
            request.app,
            global_settings.dataframe_name,
            pl.DataFrame(schema=pl_book_schema),
        )  # Initialize DataFrame in app state if not present
    getattr(request.app, global_settings.dataframe_name).extend(
        _pl_data_frame
    )  # Extend the existing DataFrame with new data

    # TODO: using IndexService write _pl_data_frame as increment to observability table in relational database
    # write index should catch dupes before writing to database
    background_tasks.add_task(
        index.swap_dataframe_to_sqlite,
        dataframe=_pl_data_frame,
    )

    # TODO: do write parquet if this is last chunk of data for day
    if (
        getattr(request.app, global_settings.dataframe_name).estimated_size(unit="mb")
        > global_settings.dataframe_dump_size
    ):
        _file = (
            await filename_generator.generate_filename()
        )  # Generate a filename for the dump

        _res = s3.materialize_dataframe(
            getattr(request.app, global_settings.dataframe_name), _file
        )  # Materialize the DataFrame to S3, Make this function synchronous and blocking on IO.
        # Other coroutines will naturally wait for it to complete.
        if _res:
            delattr(request.app, global_settings.dataframe_name)  # Clear the DataFrame
            remove_daily_parquet_file(
                f"daily_{str(os.getpid())}.parquet"
            )  # delete the persistence file from the local filesystem
            index.swap_dataframe_to_sqlite(pl.DataFrame(schema=pl_book_schema), if_table_exists="replace")
            return {"message": _res}

    return {"message": "Data frozen in ice cube"}  # Return a success message


@router.post("/v1/save_parquet")
async def materialize_data_in_parquet_file(
    request: Request,
    s3: S3Service = Depends(),
    filename_generator: FilenameGeneratorService = Depends(
        get_filename_generator_service
    ),
    db_session: AsyncSession = Depends(DatabaseService().get_db),
):
    """
    Endpoint to materialize the iced data stored in the application state to S3.

    Args:
        request (Request): The FastAPI request object.
        s3 (S3Service): The S3 service dependency.
        filename_generator (FilenameGeneratorService): The filename generator service dependency.
        db_session (DatabaseService): The database service dependency.

    Returns:
        dict: A message indicating the result of the materialization process.
    """
    _file = (
        await filename_generator.generate_filename()
    )  # Generate a filename for the dump
    _df: pl.DataFrame = getattr(request.app, global_settings.dataframe_name)

    _df_to_parquet = _df.select(["description", "hash"])

    _res = s3.materialize_dataframe(
        _df_to_parquet, _file
    )  # Materialize the DataFrame to S3

    _parquet_path_id = hash(_res["path"])

    # _parquet_index = ParquetIndex(id=_parquet_path_id, s3_url=_res["path"])
    # _res_db = await _parquet_index.save(db_session)
    # TODO: check how it looks in memory and if address alloc by _df is the same as request.app.your_books_data
    # TODO: drop all dfs which are already saved in s3 and sql

    _index: IndexService = IndexService()
    _index_res = _index.write_index(dataframe=_df, parquet_path_id=_parquet_path_id)

    return {
        "message": _res,
        # "database": _res_db,
        "books_added_to_index": _index_res,
    }  # Return the result message


@router.post("/v1/merge_parquet_files")
def merge_parquet_files(
    s3: S3Service = Depends(),
):
    """
    Endpoint to merge Parquet files stored in the specified S3 bucket.

    Args:
        s3 (S3Service): The S3 service dependency.

    Returns:
        dict: A message indicating the result of the merge operation.
    """
    # try to remove daily.parquet if exists
    if s3.parquet_file_exists("daily/daily.parquet"):
        s3.delete_parquet_file("daily/daily.parquet")
    _df = s3.merge_parquet_files(
        "daily"
    )  # Merge Parquet files in the "daily" bucket into a single DataFrame
    _res = s3.materialize_dataframe(
        _df, "daily.parquet"
    )  # Materialize the DataFrame to S3
    return {"message": _res}  # Return the result message


@router.get("/v1/list_buckets")
def list_buckets(s3: S3Service = Depends()):
    """
    Endpoint to list all available buckets in the S3 storage.

    Args:
        s3 (S3Service): The S3 service dependency.

    Returns:
        dict: A dictionary containing the list of bucket names.
    """
    buckets = s3.list_buckets()
    return {"buckets": buckets}


@router.post("/v1/create_bucket/{bucket_name}")
def create_bucket(bucket_name: str, s3: S3Service = Depends()):
    """
    Endpoint to create a new bucket in the S3 storage.

    Args:
        bucket_name (str): The name of the bucket to be created.
        s3 (S3Service): The S3 service dependency.

    Returns:
        dict: A dictionary containing the status and bucket name.
    """
    result = s3.create_bucket(bucket_name)
    return result


@router.get("/v1/list_files/{bucket_name}")
def list_files(bucket_name: str, s3: S3Service = Depends()):
    """
    Endpoint to list all files in a specific S3 bucket.

    Args:
        bucket_name (str): The name of the bucket.
        s3 (S3Service): The S3 service dependency.

    Returns:
        dict: A dictionary containing the list of file paths.
    """
    files = s3.list_files(bucket_name)
    return {"files": files}
