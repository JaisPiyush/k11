from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import distinct, select
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.schema import Index
from sqlalchemy.inspection import inspect
from .exceptions import NoDocumentExists
import pymongo
from pymongo.operations import IndexModel
from sqlalchemy.orm import mapper
from typing import Any, Generator, List, NoReturn, Sequence, Union
from .app import register_digger, register_treasure, register_connection



class TableAdapter:
    def __init__(self, model_cls) -> None:
        self.model_cls = model_cls
    def get_connection(self, db=None):
        if db is not None:
            return register_connection(db)
        return register_digger()
    
    @property
    def session(self) -> Session:
        connection = self.get_connection(db=self.model_cls.__database__ if hasattr(self.model_cls, "__database__") else 'digger')
        if not connection.postgres_engine.dialect.has_table(connection.postgres_connection, self.model_cls.__tablename__):
            self.create_table()
        return connection.postgres_session
    
    """
    Create Table in the given database
    >>> Base.metadata.create_all(engine)
    """

    def create_table(self):
        self.model_cls.metadata.create_all(self.get_connection().postgres_engine)
    
    def create(self, **kwargs):
        model = self.model_cls(**kwargs)
        self.session.add(self.model_cls(**kwargs))
        self.session.commit()
        setattr(model, "adapter", self)
        return model
    
    def _instance_create(self,  model):
        self.session.add(model)
        self.session.commit()
    
    def bulk_create(self, ls: List):
        self.session.add_all(ls)
        self.session.commit()
        for model in ls:
            setattr(model, "adapter", self)
            yield model
    
    def count(self) -> int:
        query = self.session.query(func.count(distinct(self.model_cls.get_primary_key())))
        return self.session.execute(query).scalar()
    
    def exists(self, *criterion) -> bool:
        stmt = self.session.query(self.model_cls).filter(*criterion)
        return self.session.query(stmt.exists()).scalar()




     
    

    

class MongoAdapter:
    def __init__(self) -> None:
        self.model_cls = None
        self.collection_name = None
    
    def contribute_to_class(self, model_cls) -> None:
        self.model_cls = model_cls
        self.collection_name = model_cls.__collection_name__
    
    def __hash__(self) -> int:
        return hash((self.model_cls, self.collection_name))
    
    def __eq__(self, o: object) -> bool:
        return(hasattr(o, "model_cls") and getattr(o,"model_cls") != None
         and o.model_cls == self.model_cls and o.collection_name == self.collection_name)

    

    """
    connect to collection
    The method is nothing but a wrapper around pymongo API

    >>> import pymongo
    >>> client = MongoClient('afdf')  # Inserted through register_digger()
    >>> db = client['db_name]
    >>> collection = db['collection_name']
    """
    def _connect(self):
        if self.model_cls == None:
            return None
        connection = None
        if hasattr(self.model_cls, "__database__") and (db := getattr(self.model_cls, "__database__")) != None:
            connection = register_connection(db)
        else:
            connection = register_digger(postgres=False)
        collection = connection.mongo_db[self.collection_name]
        # print(collection, "printing collection name")
        return collection

    """
    Creates index with the given column, as it is necessary to index things like source_id

    >>> collection.create_index(('field_name_to_index'), ('field_name2_to_index', 1/-1))
    """
    
    @staticmethod
    def create_single_field_index(collection, indexes: Union[str,Sequence[IndexModel]]):
        try:
            collection.create_indexes(indexes)
        except pymongo.errors.OperationFailure:
            pass
    

   
    def _create_index(self,collection, model_cls):
        if self.model_cls.indexes is not None and len(self.model_cls.indexes) > 0:
            # print([MongoAdapter.create_index_model(index) if not isinstance(index, IndexModel) else index for index in self.model_cls.indexes])
            self.create_single_field_index(collection, model_cls.create_indexes())
        if self.model_cls.primary_key != '_id':
            self.create_single_field_index(collection, [model_cls.create_primary_key_index()])

    """
    Inserting document into collection

    >>> collection = self._connect() # 
    >>> model = {"name" : "Piyush" ,"age": 19}
    >>> model = self.model.from_dict(**collection.inser_one(model))
    >>> return model
    """
    def create(self, **model):
        if self.model_cls == None:
            return None
        collection = self._connect()
        _id = collection.insert_one(model).inserted_id
        model_cls = self.model_cls.from_dict(**model)
        setattr(model_cls, "adapter", self)
        setattr(model_cls, "_id", _id)
        self._create_index(collection, model_cls)
        return model_cls
    

    def bulk_insert(self, ls) -> Generator:
        if self.model_cls == None:
            return None
        collection = self._connect()
        collection.insert_many([model.to_dcit() for model in ls])
        return ls      
    

    """
    Base find implementation

    >>> collection.find({})
    """
    def find(self, filter, **kwargs) -> Generator[Any, None, None]:
        collection = self._connect()
        for value in collection.find(filter,**kwargs):
            yield self.model_cls.from_dict(**value)
    
    def find_one(self, filter,silent=False, *args, **kwargs):
        collection = self._connect()
        doc = collection.find_one(filter, *args, **kwargs)
        if doc is None:
            if silent == True:
                return None
            raise NoDocumentExists(collection, query=filter)
        return self.model_cls.from_dict(**doc)
    
    def count(self, filter, **kwargs):
        collection = self._connect()
        collection.count_documents(filter, **kwargs)

        
       

