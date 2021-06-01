from k11.models.database import DatabaseConfiguration
from typing import Callable, List, Union
from .utils import CursorWrapper
from sqlalchemy.orm.session import Session
from pymongo import MongoClient

UniversalConnection = Union[Session, MongoClient]


class BaseDatabaseWrapper:

    def __init__(self, configuration) -> None:
        # The Database connection
        self.connection: UniversalConnection = None
        # The database parameters like name, and others
        self.configuration: DatabaseConfiguration = configuration
        # Hooks to tun after commit
        self.run_on_commit: List[Callable] = []

    def get_connection_params(self):
        """Returns decorated dictionary for database connection"""
        ...  
    
    def get_new_connection(self, conn_params):
        raise NotImplementedError("get_new_connection method(*args) is needed to be implemented")
    
    def connect(self):
        """Initialize the database connection settings."""
        raise NotImplementedError("connect() method is needed to be implemented")
    
    def create_cursor(self, name=None):
        """Create a cursor of the database"""
        raise NotImplementedError("create_cursor() method is needed to be implemented")
    
    def _close(self):
        if self.connection is not None:
            self.connection.close()
    
    def _commit(self):
        if self.connection is not None:
            self.connection.commit()
    
    def is_usable(self):
        """Test if database connection is usable"""
        raise NotImplementedError("is_useable() method is needed to be implemented")
    
    def make_cursor(self, cursor):
        """Create a universal cursor"""
        return CursorWrapper(cursor, self)
    
    def on_commit(self, func: Callable):
        if not callable(func):
            raise TypeError("arg must be callable")
        else:
            self.run_on_commit.append(func)
    
    def close(self):
        self._close()
    
    def commit(self):
        self.commit()
    