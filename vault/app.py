from sqlalchemy import create_engine
from pymongo import MongoClient
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.sql.base import NO_ARG
from sqlalchemy.sql.schema import MetaData
from models.database import DatabaseConfiguration
from sqlalchemy.orm import scoped_session, sessionmaker
from typing import Dict, Union


"""
This class stores Database Configurations
using connect_db callback, it becomes easy to create engine or client,
based on service used.
"""

class DatabaseConnection():
    sql_serviecs = ['postgresql']
    no_sql_services = ['mongodb']
    def __init__(self, **configs) -> None:
        self.configs: Dict[str, DatabaseConfiguration] = {key: DatabaseConfiguration.from_dict(**value) for key, value in configs.items()}
        self.postgres_connection = None
        self.postgres_engine: Engine = None
        self.mongo_client = None
        self.meta_data = None
        self.postgres_db = None
        self.postgres_session = None
    
    def connect_db(self, db: str) -> Union[Engine, MongoClient]:
        config = self.configs[db]
        if config.service in self.sql_serviecs:
            return create_engine(config.parse())
        elif config.service in self.no_sql_services:
            return MongoClient(config.parse())
    
    def register(self, database):
        if self.configs[database].service in self.sql_serviecs:
            self._register_postgres_connection(database)
    
    def _register_postgres_connection(self, database_name):
        if self.postgres_connection is None or self.postgres_db != database_name:
            self.postgres_engine = self.connect_db(database_name)
            self.meta_data: MetaData = MetaData(bind=self.postgres_engine) if self.meta_data is None else self.meta_data
            self.postgres_connection: Connection = self.postgres_engine.connect()
            self.postgres_session = scoped_session(sessionmaker())
            self.postgres_session.configure(bind=self.postgres_engine,autoflush=False, expire_on_commit=False)
            
       

"""
All the database configurations must be defined here
"""

DATABASES = {
    "mongo_digger": {
        "service": "mongodb",
        "database": "digger",
        "host": "localhost",
        "port": 27017
        
    },
    "postgres_digger": {
        "service": "postgresql",
        "database": "postgres",
        "host": "localhost",
        "port": 5432,
        "driver": "psycopg2",
        "username": "postgres",
        "password": "piyush@103"
    }
}


CONNECTIONS = DatabaseConnection(**DATABASES)

def register_digger():
    CONNECTIONS.register('postgres_digger')
    return CONNECTIONS


