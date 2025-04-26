import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.books import router as grizzly_router

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(_app: FastAPI):
    pid = os.getpid()
    try:
        logger.info(f">>> Process ID {pid} saved to file.")
        yield
    except Exception as e:
        logger.error(f"Failed to save process ID to file: {e}")
        raise
    finally:
        # Close any resources here if needed
        pass
app = FastAPI(
    title="Grizzly Rest API",
    version="0.3.0",
    lifespan=lifespan,
    debug=True,
    contact={
    "name": "Jakub Miazek",
    "email": "the@grillazz.com",
}
)

app.include_router(grizzly_router, prefix="/grizzly")
