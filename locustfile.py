from locust import HttpUser, task, between
from schemas.pydantic import PolarsIcedSchema

from polyfactory.factories.pydantic_factory import ModelFactory


class PolarsIcedFactory(ModelFactory[PolarsIcedSchema]):
    __model__ = PolarsIcedSchema


class PerformanceTests(HttpUser):
    wait_time = between(1, 3)

    @task(1)
    def test_your_books_data(self):
        payload = [
            PolarsIcedFactory.build(factory_use_constructors=True).model_dump(
                mode="json"
            )
            for _ in range(600)
        ]
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        self.client.post(
            "/frozen/v1/froze_data_in_frame", json=payload, headers=headers
        )
