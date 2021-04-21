import pymongo
from pymongo.operations import IndexModel
from sqlalchemy.orm import mapper
from typing import List, NoReturn, Sequence, Union
from .app import register_digger


class TableAdapter:
    
    def __init__(self, model) -> NoReturn:
        self.model = model
        connection = register_digger()
        self.table = self.model.to_table(connection.meta_data)
        # self.map()
    
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
        self.model_cls = model
        self.collection_name = model.__collection_name__
    

    

    """
    connect to collection
    The method is nothing but a wrapper around pymongo API

    >>> import pymongo
    >>> client = MongoClient('afdf')  # Inserted through register_digger()
    >>> db = client['db_name]
    >>> collection = db['collection_name']
    """
    def _connect(self):
        connection = register_digger(postgres=False)
        collection = connection.mongo_db[self.collection_name]
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
    
    """
    Inserting document into collection

    >>> collection = self._connect() # 
    >>> model = {"name" : "Piyush" ,"age": 19}
    >>> model = self.model.from_dict(**collection.inser_one(model))
    >>> return model
    """
    def create(self, **model):
        collection = self._connect()
        collection.insert_one(model)
        print(model)
        model_cls = self.model_cls.from_dict(**model)
        if self.model_cls.indexes is not None and len(self.model_cls.indexes) > 0:
            # print([MongoAdapter.create_index_model(index) if not isinstance(index, IndexModel) else index for index in self.model_cls.indexes])
            self.create_single_field_index(collection, model_cls.create_indexes())
        if self.model_cls.primary_key != '_id':
            self.create_single_field_index(collection, [model_cls.create_primary_key_index()])
        return model_cls

        

        

        
       

