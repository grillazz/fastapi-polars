from fastapi import FastAPI

from api.books import router as grizzly_router

app = FastAPI(title="Grizzly Rest API", version="0.3.0")

app.include_router(grizzly_router, prefix="/grizzly")
