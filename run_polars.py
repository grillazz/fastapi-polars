from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
from pydantic import BaseModel
import polars as pl
import os
from polyfactory.factories.pydantic_factory import ModelFactory


@asynccontextmanager
async def lifespan(_app: FastAPI):
    try:
        # TODO: try open daily /houerly parquet for schema
        # _app.polars_iced_data = pl.read_parquet("polars_iced_data_1.parquet")
        _app.polars_iced_data = pl.DataFrame(schema={"ingest": pl.Int64, "saffire": pl.String})
        # print(f"{_app.polars_iced_data.count()=}")
        yield
    except FileNotFoundError:
        _app.polars_iced_data = pl.DataFrame(schema={"ingest": pl.Int64, "saffire": pl.String})
        # _app.polars_iced_data = UberDataFrame().df
        yield
    finally:
        print(f"{_app.polars_iced_data.count()=}")
        _app.polars_iced_data.write_parquet(f"polars_iced_data_{str(os.getpid())}.parquet")

app = FastAPI(title="Polars Iced API", version="0.0.1", lifespan=lifespan)
@app.get("/")
async def root(request: Request):
    _c = request.app.polars_iced_data.count()
    return {"message": f"Welcome to Polars Iced API {_c=}"}


class PolarsIcedSchema(BaseModel):
    ingest: int
    saffire: str

class PolarsIcedFactory(ModelFactory[PolarsIcedSchema]):
    __model__ = PolarsIcedSchema

@app.post("/v1/polars_ice_data")
async def polars_iced_data(data: list[PolarsIcedSchema], request: Request):
    # TODO: cProfile below line for best performance
    # _ice_cube = pl.DataFrame(_d.model_dump() for _d in data)
    _ice_cube = pl.DataFrame([_d.__dict__ for _d in data])

    request.app.polars_iced_data.extend(_ice_cube)

    # request.app.polars_iced_data.write_parquet("polars_iced_data_1.parquet")

    # cProfile.run("pl.DataFrame(_d.model_dump() for _d in data)")

    return {"message": "Data frozen in ice cube"}



# TODO: endpoint to post dataframe to s3

# TODO: scheduler only sent POST to this endpoint in defined time

# TODO: endpoint to read / scan parquet from s3
