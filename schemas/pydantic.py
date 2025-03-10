from polyfactory.factories.pydantic_factory import ModelFactory
from pydantic import BaseModel
from pydantic_extra_types.isbn import ISBN

class PolarsIcedSchema(BaseModel):
    ingest: int
    saffire: str
    isbn: ISBN

    # TODO: add date field > when testing in locust allow for range i.e. month of random dates


class PolarsIcedFactory(ModelFactory[PolarsIcedSchema]):
    __model__ = PolarsIcedSchema
