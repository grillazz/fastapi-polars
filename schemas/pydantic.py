from polyfactory.factories.pydantic_factory import ModelFactory
from pydantic import BaseModel, Field, ConfigDict, field_validator
from pydantic_extra_types.isbn import ISBN

class PolarsIcedSchema(BaseModel):
    ingest: int = Field(description="The ingestion number or ID")
    saffire: str = Field(description="Saffire identifier")
    isbn: ISBN = Field(description="Book ISBN-10 or ISBN-13 number")

    # TODO: add date field > when testing in locust allow for range i.e. month of random dates

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "ingest": 12345,
                    "saffire": "SF-789-XYZ",
                    "isbn": "978-0-306-40615-7"
                }
            ]
        }
    )

    @field_validator('isbn', mode='before')
    @classmethod
    def clean_isbn(cls, value):
        if isinstance(value, str):
            return value.replace("-", "")
        return value

class PolarsIcedFactory(ModelFactory[PolarsIcedSchema]):
    __model__ = PolarsIcedSchema
