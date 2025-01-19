import polars as pl

from fastapi import FastAPI, Request

from api.frozen import router as frozen_router
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