from polyfactory.factories.pydantic_factory import ModelFactory
from pydantic import BaseModel


class PolarsIcedSchema(BaseModel):
    ingest: int
    saffire: str


class PolarsIcedFactory(ModelFactory[PolarsIcedSchema]):
    __model__ = PolarsIcedSchema
