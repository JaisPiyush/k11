
from typing import NoReturn
from .app import register_digger


class TableAdapter:
    
    def __init__(self, model) -> NoReturn:
        self.model = model
        connection = register_digger()
        self.table = self.model.to_table(connection.meta_data)

    """
    Inserts data into database
    """
    def create(self, **kwargs):
        connection = register_digger()
        return self.model.to_table(connection.meta_data).insert().values(**kwargs)
    
    """
    Create Table method calls to_table, and creates new Table
    """
    def create_table(self) -> NoReturn:
        connection = register_digger()
        connection.meta_data.create_all(self.model.to_table(connection.meta_data))
    



        
       

