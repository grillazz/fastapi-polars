import polars as pl

from fastapi import FastAPI, Request

from api.frozen import router as frozen_router

import itertools
from whenever import Instant


async def filename_generator(base_name: str):
    sequence = itertools.count(1)
    current_date = Instant.now().py_datetime().strftime("%Y%m%d")
    while True:
        new_date = Instant.now().py_datetime().strftime("%Y%m%d")
        if new_date != current_date:
            current_date = new_date
            sequence = itertools.count(1)
        yield f"{base_name}_{current_date}_{next(sequence):03}.parquet"


app = FastAPI(title="Polars Iced API", version="0.0.1")


@app.get("/")
async def root(request: Request):
    try:
        _s = request.app.polars_iced_data.estimated_size(unit="mb")
        _c = request.app.polars_iced_data.get_column("ingest").count()
        return {"message": f"Welcome to Polars Iced API {_s=} {_c=}"}
    except AttributeError:
        # TODO: inform user if dataframe not yet exists
        return {"message": "Welcome to Polars Iced API"}


app.include_router(frozen_router, prefix="/frozen", tags=["frozen"])
