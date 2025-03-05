from polyfactory.factories.pydantic_factory import ModelFactory
from pydantic import BaseModel


class PolarsIcedSchema(BaseModel):
    ingest: int
    saffire: str
    # TODO: add date field > when testing in locust allow for range i.e. month of random dates


class PolarsIcedFactory(ModelFactory[PolarsIcedSchema]):
    __model__ = PolarsIcedSchema
