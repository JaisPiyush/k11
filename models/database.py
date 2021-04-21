from dataclasses import dataclass, field
from typing import List, Dict, Optional, Union
from vault.adapter import MongoAdapter
from bson.objectid import ObjectId

from pymongo.operations import IndexModel



@dataclass
class DatabaseConfiguration:
    service: str
    host: str
    port: int
    database: str
    username: Optional[str] = None
    password: Optional[str] = None
    driver: Optional[str] = None

    def get_driver(self) -> str:
        return f"+{self.driver}" if self.driver is not None else ''
    
    def get_service(self) -> str:
        return self.service
    
    def get_database(self) -> str:
        return self.database
    
    def _get_user(self, in_str=True) -> Union[str, None]:
        if self.username == None and in_str:
            return ''
        return self.username
    
    def _get_password(self, in_str=True) -> Union[str, None]:
        if self.password == None and in_str:
            return ''
        return self.password
    
    def get_auth(self) -> str:
        auth = ''
        if self.username is not None:
            auth += self._get_user()
        if self.password is not None:
            auth += ":"+ self._get_password()
        return auth +"@" if len(auth) > 0 else auth
            

    def parse(self) -> str:
        return f"{self.service}{self.get_driver()}://{self.get_auth()}{self.host}:{self.port}/{self.database}"
    
    @classmethod
    def from_dict(cls, **kwargs):
        return cls(**kwargs)



@dataclass
class MongoModels:
    __collection_name__ = None
    __database__ = None
    _id = None
    indexes = []
    primary_key = "_id"
    non_dictables = []
    _non_dictables = ["__service__","__database__","__collection_name__", "indexes", "primary_key", "non_dictables"]
    
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
    def _adapter(cls) -> MongoAdapter:
        return MongoAdapter(cls)

    def adapter() -> MongoAdapter:
        return MongoModels._adapter()
    
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
    






