from datetime import  date
from polyfactory.factories.pydantic_factory import ModelFactory
from pydantic import BaseModel, Field, ConfigDict, field_validator
from pydantic_extra_types.isbn import ISBN


class BookSchema(BaseModel):
    isbn: ISBN = Field(description="Book ISBN-10 or ISBN-13 number")
    description: str = Field(description="Book description text")
    author: str = Field(description="Author of the book")
    pages: int = Field(description="Number of pages in the book")
    pub_date: date = Field(description="Date of publication")

    # TODO: add date field > when testing in locust allow for range i.e. month of random dates

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "pages": 432,
                    "description": "An illustrated guide to programming",
                    "isbn": "978-0-306-40615-7",
                    "author": "Jane Doe",
                    "pub_date": "2023-10-01"
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

class BookFactory(ModelFactory[BookSchema]):
    __model__ = BookSchema