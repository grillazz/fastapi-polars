import os
from fastapi import Request, APIRouter, Depends


from models.parquet import ParquetIndex
from schemas.pydantic import PolarsIcedSchema
from schemas.polars import pl_iced_schema
import polars as pl
from sqlalchemy.ext.asyncio import AsyncSession

from services.files import FilenameGeneratorService, get_filename_generator_service
from services.s3 import S3Service
from services.database import DatabaseService
from config import settings as global_settings

router = APIRouter()


@router.get("/")
async def root(request: Request):
    """
    Root endpoint to display a welcome message and information about the current DataFrame.

    This endpoint checks if a DataFrame is stored in the application state under the name specified
    in the global settings. If the DataFrame exists, it returns its estimated size and the count of
    the 'ingest' column. If the DataFrame does not exist, it returns a message indicating that no
    DataFrame is defined yet.

    Args:
        request (Request): The FastAPI request object.

    Returns:
        dict: A dictionary containing a welcome message and DataFrame information if available.
    """
    try:
        dataframe = request.app.__getattribute__(global_settings.dataframe_name)
        _s = dataframe.estimated_size(unit="mb")
        _c = dataframe.get_column("ingest").count()
        return {"message": f"Welcome to Grizzly Rest API. {_s=} {_c=}"}
    except AttributeError:
        return {"message": "Welcome to Grizzly Rest API. No dataframe defined yet."}


@router.post("/v1/froze_data_in_frame")
async def froze_data_in_frame(
    data: list[PolarsIcedSchema],
    request: Request,
    s3: S3Service = Depends(),
    filename_generator: FilenameGeneratorService = Depends(
        get_filename_generator_service
    ),
):
    """
    Endpoint to freeze data into a Polars DataFrame and store it in the application state.
    If the DataFrame exceeds a specified size, it is materialized to S3 and cleared.

    Args:
        data (list[PolarsIcedSchema]): List of data items to be added to the DataFrame.
        request (Request): The FastAPI request object.
        s3 (S3Service): The S3 service dependency.
        filename_generator (FilenameGeneratorService): The filename generator service dependency.

    Returns:
        dict: A message indicating the data has been frozen.
    """
    _ice_cube = pl.DataFrame(
        [{**_d.__dict__, "pid": str(os.getpid())} for _d in data]
    )  # Convert input data to a Polars DataFrame
    if not hasattr(request.app, global_settings.dataframe_name):
        request.app.__setattr__(
            global_settings.dataframe_name, pl.DataFrame(schema=pl_iced_schema)
        )  # Initialize DataFrame in app state if not present
    request.app.__getattribute__(global_settings.dataframe_name).extend(
        _ice_cube
    )  # Extend the existing DataFrame with new data
    if (
        request.app.__getattribute__(global_settings.dataframe_name).estimated_size(
            unit="mb"
        )
        > global_settings.dataframe_dump_size
    ):
        _file = (
            await filename_generator.generate_filename()
        )  # Generate a filename for the dump

        _res = s3.materialize_dataframe(
            request.app.__getattribute__(global_settings.dataframe_name), _file
        )  # Materialize the DataFrame to S3, Make this function synchronous and blocking on IO.
        # Other coroutines will naturally wait for it to complete.
        if _res:
            delattr(request.app, global_settings.dataframe_name)  # Clear the DataFrame

    return {"message": "Data frozen in ice cube"}  # Return a success message


@router.post("/v1/materialize_iced_data")
async def materialize_iced_data(
    request: Request,
    s3: S3Service = Depends(),
    filename_generator: FilenameGeneratorService = Depends(
        get_filename_generator_service
    ),
    db_session: AsyncSession  = Depends(DatabaseService().get_db),
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
    _df: pl.DataFrame = request.app.your_iced_data
    _res = s3.materialize_dataframe(_df, _file)  # Materialize the DataFrame to S3
    _parquet_index = ParquetIndex(s3_url=_res["path"])
    _res_db = await _parquet_index.save(db_session)

    _df.write_database(
        table_name="books_index",
        connection="postgresql://metabase:secret@localhost/metabase",
        if_table_exists="append",
        engine="adbc"
    )
    return {"message": _res, "database": _res_db}  # Return the result message



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