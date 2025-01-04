import io

from fastapi import FastAPI, Request, Depends
from contextlib import asynccontextmanager
import polars as pl
import os
from schemas.pydantic import PolarsIcedSchema
from schemas.polars import pl_iced_schema
from services.s3 import S3Service
from io import BytesIO

@asynccontextmanager
async def lifespan(_app: FastAPI):
    try:
        # TODO: try open daily /hourly parquet for schemas > ready only max datetime for current date
        # _app.polars_iced_data = pl.read_parquet("polars_iced_data_1.parquet")
        _app.s3 = S3Service()
        _app.s3.s3fs_client.session = await _app.s3.s3fs_client.set_session()
        _app.polars_iced_data = pl.DataFrame(schema=pl_iced_schema)
        _app.polars_iced_data_dump = pl.DataFrame(schema=pl_iced_schema)
        # print(f"{_app.polars_iced_data.count()=}")
        yield
    except FileNotFoundError:
        _app.polars_iced_data = pl.DataFrame(schema=pl_iced_schema)
        # _app.polars_iced_data = UberDataFrame().df
        yield
    finally:
        print(f"{_app.polars_iced_data.count()=}")
        _parquet_as_bytes = io.BytesIO()
        _app.polars_iced_data.write_parquet(_parquet_as_bytes)
        await _app.s3.s3fs_client.session.put_object(Bucket="daily", Key=f"polars_iced_data_{str(os.getpid())}.parquet", Body=_parquet_as_bytes.getvalue())
        await _app.s3.s3fs_client.session.close()




app = FastAPI(title="Polars Iced API", version="0.0.1", lifespan=lifespan)
@app.get("/")
async def root(request: Request):
    _c = request.app.polars_iced_data.count()
    return {"message": f"Welcome to Polars Iced API {_c=}"}


@app.post("/v1/polars_ice_data")
async def polars_iced_data(data: list[PolarsIcedSchema], request: Request):
    # TODO: cProfile below line for best performance
    _ice_cube = pl.DataFrame([_d.__dict__ for _d in data])
    request.app.polars_iced_data.extend(_ice_cube)
    return {"message": "Data frozen in ice cube"}


@app.post("/v1/dump_iced_data")
async def dump_iced_data(request: Request, s3: S3Service = Depends()):
    _b = BytesIO()
    request.app.polars_iced_data.write_parquet(_b)
    _obj = await s3.s3fs_client._touch(
        path="daily/polars_iced_data_1.parquet",
        Body=_b.getvalue()
    )
    return {"message": _obj}


@app.post("/v1/materialize_iced_data")
async def materialize_iced_data(request: Request, s3: S3Service = Depends()):
    _res = await s3.materialize_dataframe(request.app.polars_iced_data, "polars_iced_data_2.parquet")
    return {"message": _res}

@app.post("/v2/materialize_iced_data")
async def materialize_iced_data_v2(request: Request):
    _parquet_as_bytes = io.BytesIO()
    # for consitenct we materialize cloned object as at the same time new records can be added to
    request.app.polars_iced_data_dump = request.app.polars_iced_data.clone()
    request.app.polars_iced_data_dump.write_parquet(_parquet_as_bytes)
    _res = await request.app.s3.s3fs_client.session.put_object(
        Bucket="daily", Key="polars_iced_data_3.parquet", Body=_parquet_as_bytes.getvalue()
    )
    request.app.polars_iced_data_dump.clear()
    return {"message": _res}

# TODO: add middleware which will be adding new df per router if they not exists

# TODO: dataframes can be remove from memory to dist if number of records hit the limit > scheduler ?

# TODO: endpoint to post dataframe to s3

# TODO: scheduler only sent POST to this endpoint in defined time

# TODO: endpoint to read / scan parquet from s3

# TODO: validate dataframe before save with great expectations

