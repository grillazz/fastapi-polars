from typing import Dict, Type, Any

from faker import Faker

from locust import HttpUser, task, between
from polyfactory.factories.pydantic_factory import ModelFactory
from schemas.pydantic import BookSchema
from pydantic_extra_types.isbn import ISBN

faker = Faker()


class ISBNGen(str):
    def __new__(cls, *args, **kwargs):
        # Create a string instance with a valid ISBN
        return super().__new__(cls, faker.isbn13())


class BookFactory(ModelFactory[BookSchema]):
    __model__ = BookSchema

    @classmethod
    def get_provider_map(cls) -> Dict[Type, Any]:
        providers_map = super().get_provider_map()

        return {
            ISBN: lambda: ISBNGen(),
            **providers_map,
        }


class PerformanceTests(HttpUser):
    wait_time = between(1, 3)

    @task(1)
    def test_your_books_data(self):
        payload = [
            BookFactory.build(factory_use_constructors=True).model_dump(mode="json")
            for _ in range(64)
        ]
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        self.client.post("/grizzly/v1/ingest_data", json=payload, headers=headers)
