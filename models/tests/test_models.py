from dataclasses import dataclass
from typing import Optional
import pytest
from ..database import MongoModels


@dataclass
class MongoTestModel(MongoModels):
    name: str
    __collection_name__ = "test_model"
    primary_key = "name"
    faulcy: str
    non_dictables = ['faulcy']
       

"""
The model inheritance test, the inheriting model must contain all the methods,
and should work properly
"""

def test_model_inheritance():
    model_class = MongoTestModel("Piyush", "dsf")
    assert model_class.to_dict() == {"name": model_class.name}, "to_dict implementation fialed"
    class_model = MongoTestModel.from_dict(name="Rajesh", faulcy="sdf")
    assert class_model.name == "Rajesh", "class method is failing"

