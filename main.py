import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from whenever import Instant

from api.books import router as grizzly_router

from services.utlis import AppLogger

logger = AppLogger().get_logger()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    pid = os.getpid()
    try:
        logger.info(f">>> Process ID {pid} saved to file.")
        # TODO: add global which will be holding the DDMMYYYY and every call to the
        # TODO: endpoint will be checking if the date is the same if not it will
        # TODO: setting new date and it will destroy dataframe and create new one to hol dnew days logs
        _app.now = Instant.now().py_datetime().strftime("%Y%m%d")
        logger.info(f">>> Date is set to {_app.now}")
        yield
    except Exception as e:
        logger.error(f"Failed to save process ID to file: {e}")
        raise
    finally:
        # Close any resources here if needed
        pass


app = FastAPI(
    title="Grizzly Rest API",
    version="0.4.0",
    lifespan=lifespan,
    debug=True,
    contact={
        "name": "Jakub Miazek",
        "email": "the@grillazz.com",
    },
)

app.include_router(grizzly_router, prefix="/grizzly")
