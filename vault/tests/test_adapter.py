from dataclasses import dataclass
from typing import Dict, List

import pymongo
from ..adapter import MongoAdapter
import pytest 
from pymongo import ASCENDING
from ...models.database import MongoModels


@dataclass
class MongoAdapterTestModel(MongoModels):
    __collection_name__ = "test_collection"
    __database__ = "digger"
    name: str
    age: int
    primary_key = 'age'
    indexes = [{
        "key": primary_key,
        "dir": pymongo.ASCENDING,
        "unique": True
    }]

    @classmethod
    def _adapter(cls) -> MongoAdapter:
        return MongoAdapter(cls)

    def adapter() -> MongoAdapter:
        return MongoAdapterTestModel._adapter()

@pytest.fixture
def adapter_models() -> List[Dict]:
    return [
        {
            "name": "Piyush",
            "age": 19
        },
        {
            "name": "Vatsalya",
            "age": 16
        },
        {
            "name": "Tanmay",
            "age": 14
        }
    ]


def test_mongo_model_creation(adapter_models):
    try:
        for model in adapter_models:
            cls = MongoAdapterTestModel.adapter().create(**model)
            assert isinstance(cls, MongoAdapterTestModel), "Create model failing to create instances"
    except Exception as e:
        raise e

def test_mongo_model_aggregation(adapter_models):
    pass
