from sqlalchemy.orm import mapper
from typing import NoReturn
from .app import register_digger


class TableAdapter:
    
    def __init__(self, model) -> NoReturn:
        self.model = model
        connection = register_digger()
        self.table = self.model.to_table(connection.meta_data)
        self.map()
    
    def map(self):
        mapper(self.model, self.table)

    """
    Inserts data into database
    """
    def create(self, **kwargs):
        connection = register_digger()
        data = self.model(**kwargs)
        connection.postgres_session.add(data)
        connection.postgres_session.commit()
        return data
    
    """
    Create Table method calls to_table, and creates new Table
    """
    def create_table(self) -> NoReturn:
        connection = register_digger()
        connection.meta_data.create_all(self.model.to_table(connection.meta_data))
    

class MongoAdapter:
    def __init__(self, model) -> None:
        self.model = model
        self.collection_name = model.__collection__name__

    """
    connect to collection
    The method is nothing but a wrapper around pymongo API

    >>> import pymongo
    >>> client = MongoClient('afdf')  # Inserted through register_digger()
    >>> db = client['db_name]
    >>> collection = db['collection_name']
    """
    def _connect(self, model):
        if not isinstance(model, self.model):
            raise Exception(f"{model} is not an instance of {self.model}")
        connection = register_digger(postgres=False)
        collection = connection.mongo_db[self.collection_name]
        return collection
    
    """
    Inserting document into collection

    >>> collection = self._connect() # 
    >>> model = self.model.from_dict(**collection.inser_one(model))
    >>> return model
    """
    def create(self, model):
        collection = self._connect(model)
        model = self.model.from_dict(**collection.insert_one(model))
        # if hasattr(model, 'primary_key') and isinstance(getattr(model, 'primary_key'), str):
        #     key = getattr(model, 'primary_key')
        #     collection.createIndex({key: getattr(model, key)}, {"unique": True})
        return model

        

        

        
       

