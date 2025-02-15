from fastapi import FastAPI, Request

from api.frozen import router as ursa_router

app = FastAPI(title="Ursa Rest API", version="0.1.0")

app.include_router(ursa_router, prefix="/ursa")
