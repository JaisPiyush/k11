from dataclasses import dataclass
from typing import Any, Dict, List
from sqlalchemy.sql.expression import distinct, select

from sqlalchemy.sql.functions import func
from ..app import register_digger

import pymongo
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Integer, String
from ..adapter import MongoAdapter, TableAdapter
import pytest 
from pymongo import ASCENDING
@dataclass
class MongoAdapterTestModel:
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
    _adapter = MongoAdapter()

    @classmethod
    def adapter(cls) -> MongoAdapter:
        if cls._adapter.model_cls is None:
            cls._adapter.contribute_to_class(cls)
        return cls._adapter

    
    

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
            print(cls)
            assert isinstance(cls, MongoAdapterTestModel), "Create model failing to create instances"
    except Exception as e:
        print(e)

Base = declarative_base()

class PostgresTestModel(Base):
    __tablename__ = "test_table"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(70))
    age = Column(Integer)
    primary_key = "id"

    indexes = ["id"]

    @classmethod
    def get_primary_key(cls) -> Column:
        return getattr(cls, cls.primary_key, "id")

    @classmethod
    def adapter(cls) -> TableAdapter:
        return TableAdapter(cls)

def test_postgres_model_create(adapter_models):
    connection = register_digger()
    PostgresTestModel.metadata.create_all(connection.postgres_engine)
    model = PostgresTestModel.adapter().create(name="jaiswal", age=85)
    assert PostgresTestModel.adapter().count() > 0, "Fishy"
    assert isinstance(model.adapter, TableAdapter), "adapter injection failed"


def test_postgres_model_exists(adapter_models):
    print(PostgresTestModel.adapter().exists(PostgresTestModel.age == 16))
    assert PostgresTestModel.adapter().exists(PostgresTestModel.age == 16) == True, "Query failure"
   
    



