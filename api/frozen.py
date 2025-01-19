from fastapi import Request, APIRouter, Depends
from schemas.pydantic import PolarsIcedSchema
from schemas.polars import pl_iced_schema
import polars as pl
from services.s3 import S3Service

router = APIRouter()

@router.post("/v1/polars/froze_data_in_frame")
async def froze_data_in_frame(data: list[PolarsIcedSchema], request: Request):
    _ice_cube = pl.DataFrame([_d.__dict__ for _d in data])
    if not hasattr(request.app, "polars_iced_data"):
        request.app.__setattr__("polars_iced_data", pl.DataFrame(schema=pl_iced_schema))
    request.app.__getattribute__("polars_iced_data").extend(_ice_cube)
    return {"message": "Data frozen in ice cube"}


@router.post("/v1/materialize_iced_data")
async def materialize_iced_data(request: Request, s3: S3Service = Depends()):
    _res = await s3.materialize_dataframe(request.app.polars_iced_data, "polars_iced_data_2.parquet")
    return {"message": _res}