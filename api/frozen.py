import itertools
from fastapi import Request, APIRouter, Depends
from whenever import Instant

from schemas.pydantic import PolarsIcedSchema
from schemas.polars import pl_iced_schema
import polars as pl
from services.s3 import S3Service
from config import settings as global_settings

router = APIRouter()


async def filename_generator(base_name: str):
    """
    Asynchronous generator function that yields file names with a base name,
    current date, and a sequence number.
    The sequence number resets to 1 every new day.

    Args:
        base_name (str): The base name for the file.

    Yields:
        str: The generated file name in the format '{base_name}_{current_date}_{sequence:03}.parquet'.
    """
    sequence = itertools.count(1)  # Initialize the sequence counter starting from 1
    current_date = (
        Instant.now().py_datetime().strftime("%Y%m%d")
    )  # Get the current date in 'YYYYMMDD' format
    while True:
        new_date = (
            Instant.now().py_datetime().strftime("%Y%m%d")
        )  # Get the new current date in 'YYYYMMDD' format
        if new_date != current_date:
            current_date = new_date  # Update the current date
            sequence = itertools.count(1)  # Reset the sequence counter
        yield f"{base_name}_{current_date}_{next(sequence):03}.parquet"  # Yield the generated file name


_gen = filename_generator("polars_iced_data")


@router.post("/v1/polars/froze_data_in_frame")
async def froze_data_in_frame(
    data: list[PolarsIcedSchema], request: Request, s3: S3Service = Depends()
):
    """
    Endpoint to freeze data into a Polars DataFrame and store it in the application state.
    If the DataFrame exceeds a specified size, it is materialized to S3 and cleared.

    Args:
        data (list[PolarsIcedSchema]): List of data items to be added to the DataFrame.
        request (Request): The FastAPI request object.
        s3 (S3Service): The S3 service dependency.

    Returns:
        dict: A message indicating the data has been frozen.
    """
    _ice_cube = pl.DataFrame([_d.__dict__ for _d in data])  # Convert input data to a Polars DataFrame
    if not hasattr(request.app, "polars_iced_data"):
        request.app.__setattr__("polars_iced_data", pl.DataFrame(schema=pl_iced_schema))  # Initialize DataFrame in app state if not present
    request.app.__getattribute__("polars_iced_data").extend(_ice_cube)  # Extend the existing DataFrame with new data
    if (
        request.app.__getattribute__("polars_iced_data").estimated_size(unit="mb")
        > global_settings.dataframe_dump_size
    ):
        dataframe_dump = request.app.__getattribute__("polars_iced_data").clone()  # Clone the DataFrame for dumping
        _file = await filename_generator("polars_iced_data").__anext__()  # Generate a filename for the dump
        _res = await s3.materialize_dataframe(dataframe_dump, _file)  # Materialize the DataFrame to S3
        if _res:
            dataframe_dump.clear()  # Clear the cloned DataFrame
            request.app.__getattribute__("polars_iced_data").clear()  # Clear the DataFrame in app state

    return {"message": "Data frozen in ice cube"}  # Return a success message


@router.post("/v1/materialize_iced_data")
async def materialize_iced_data(request: Request, s3: S3Service = Depends()):
    _file = await _gen.__anext__()
    _res = await s3.materialize_dataframe(request.app.polars_iced_data, _file)
    return {"message": _res}
