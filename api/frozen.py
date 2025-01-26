from fastapi import Request, APIRouter, Depends

from schemas.pydantic import PolarsIcedSchema
from schemas.polars import pl_iced_schema
import polars as pl

from services.files import FilenameGeneratorService, get_filename_generator_service
from services.s3 import S3Service
from config import settings as global_settings

router = APIRouter()


@router.post("/v1/polars/froze_data_in_frame")
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
        [_d.__dict__ for _d in data]
    )  # Convert input data to a Polars DataFrame
    if not hasattr(request.app, "polars_iced_data"):
        request.app.__setattr__(
            "polars_iced_data", pl.DataFrame(schema=pl_iced_schema)
        )  # Initialize DataFrame in app state if not present
    request.app.__getattribute__("polars_iced_data").extend(
        _ice_cube
    )  # Extend the existing DataFrame with new data
    if (
        request.app.__getattribute__("polars_iced_data").estimated_size(unit="mb")
        > global_settings.dataframe_dump_size
    ):
        dataframe_dump = request.app.__getattribute__(
            "polars_iced_data"
        ).clone()  # Clone the DataFrame for dumping
        _file = (
            await filename_generator.generate_filename()
        )  # Generate a filename for the dump
        _res = await s3.materialize_dataframe(
            dataframe_dump, _file
        )  # Materialize the DataFrame to S3
        if _res:
            dataframe_dump.clear()  # Clear the cloned DataFrame
            request.app.__getattribute__(
                "polars_iced_data"
            ).clear()  # Clear the DataFrame in app state

    return {"message": "Data frozen in ice cube"}  # Return a success message

@router.post("/v1/materialize_iced_data")
async def materialize_iced_data(
    request: Request,
    s3: S3Service = Depends(),
    filename_generator: FilenameGeneratorService = Depends(
        get_filename_generator_service
    ),
):
    """
    Endpoint to materialize the iced data stored in the application state to S3.

    Args:
        request (Request): The FastAPI request object.
        s3 (S3Service): The S3 service dependency.
        filename_generator (FilenameGeneratorService): The filename generator service dependency.

    Returns:
        dict: A message indicating the result of the materialization process.
    """
    _file = (
        await filename_generator.generate_filename()
    )  # Generate a filename for the dump
    _res = await s3.materialize_dataframe(request.app.polars_iced_data, _file)  # Materialize the DataFrame to S3
    return {"message": _res}  # Return the result message
