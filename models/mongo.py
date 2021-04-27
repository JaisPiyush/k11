from dataclasses import dataclass

from pymongo.operations import IndexModel
from vault.adapter import MongoAdapter
from typing import List, Dict
from pymongo import IndexModel



@dataclass
class MongoModels:
    __collection_name__ = None
    __database__ = None
    _id = None
    indexes = []
    primary_key = "_id"
    non_dictables = []
    _non_dictables = ["__service__","__database__","__collection_name__", "indexes", "primary_key", "non_dictables"]
    _adapter = MongoAdapter()

    
    def get_non_dictables(self) -> List[str]:
        return self._non_dictables + self.non_dictables
    
    def _to_dict(self) -> Dict:
        return {key: self.__getattribute__(key) for key in self.__dict__ if key not in self.get_non_dictables()}

    def to_dict(self) -> Dict:
        return self._to_dict()
    
    def get_collection_name(self) -> str:
        return self.__collection_name__
    
    def get_database_name(self) -> str:
        return self.__database__
    
    @classmethod
    def adapter(cls):
        # print(cls)
        if cls._adapter.model_cls is None or cls._adapter.model_cls != cls:
            cls._adapter.contribute_to_class(cls)
        # print(cls, cls._adapter, cls._adapter.collection_name)
        return cls._adapter
    
    @staticmethod
    def gen_index_name(key: str, dirc) -> str:
        if isinstance(dirc, int):
            return key + "_"+ "ASC" if dirc == 1 else "DSC"
        return key + "_" + dirc

    """
    >>> create_index_model(["age",1,"aging"])
        IndexModel([("age", 1)], name="aging")
    
    >>> create_index_model(["age",-1])
        IndexModel([("age",1)], name="age_DSC")
    >>> create_index_model(["age"])
        IndexModel([("age",1)],name="age_ASC")
    """
    
    @staticmethod
    def create_index_model(dics: Dict) -> IndexModel:
        unique = False
        if "dir" not in dics:
            dics['dir'] = 1
        if "name" not in dics:
           dics['name'] = MongoModels.gen_index_name(dics['key'], dics['dir'])
        if "unique" not in dics:
            dics['unique'] = False
        return IndexModel([[dics['key'], dics['dir']]], name=dics['name'],unique=int(dics['unique']))
    
    def create_indexes(self) -> List[IndexModel]:
        return [self.create_index_model(index) for index in self.indexes]
    
    def create_primary_key_index(self) -> IndexModel:
        return self.create_index_model({"key": self.primary_key})
    
    @staticmethod
    def process_kwargs(**kwargs) -> Dict:
        return kwargs
    
    def set_id(self, id_):
        self._id = id_
        return self
    
    @classmethod
    def from_dict(cls, **kwargs):
        if '_id' in kwargs:
            _id = kwargs['_id']
            del kwargs['_id']
        kwargs = cls.process_kwargs(**kwargs)
        return cls(**kwargs).set_id(_id)
    

