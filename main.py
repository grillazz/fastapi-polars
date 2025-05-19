import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from whenever import Instant
import pyarrow.parquet as pq
import polars as pl
from api.books import router as grizzly_router
from schemas.polars import pl_book_schema
from services.utlis import AppLogger

logger = AppLogger().get_logger()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    pid = os.getpid()
    try:
        logger.info(f">>> Process ID {pid} saved to file.")
        _app.pqwriter = pq.ParquetWriter(f"books_swap_{str(os.getpid())}.parquet", pl.DataFrame(schema=pl_book_schema).to_arrow().schema)
        logger.info(f">>> Parquet Writer created - {_app.pqwriter.is_open}")
        # TODO: daily task on to trigger on every worker to push dataframe tail to s3 parquet
        _app.now = Instant.now().py_datetime().date()
        logger.info(f">>> Date is set to {_app.now}")
        yield
    except Exception as e:
        logger.error(f"Failed to save process ID to file: {e}")
        raise
    finally:
        # Close any resources here if needed
        _app.pqwriter.close()
        pass


app = FastAPI(
    title="Grizzly Rest API",
    version="0.5.0",
    lifespan=lifespan,
    debug=True,
    contact={
        "name": "Jakub Miazek",
        "email": "the@grillazz.com",
    },
)

app.include_router(grizzly_router, prefix="/grizzly")
