from sqlalchemy import create_engine
from pymongo import MongoClient
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.sql.base import NO_ARG
from sqlalchemy.sql.schema import MetaData
from k11.models.database import DatabaseConfiguration
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

    __instance__ = None
    _configs = None

    def __init__(self, **configs):

        # Singleton Implementation, the implementation will work if configs file changes
        if DatabaseConnection.__instance__ == None or (DatabaseConnection._configs != None and DatabaseConnection._configs == configs):
            self._configs = configs
            self.configs: Dict[str, DatabaseConfiguration] = {key: DatabaseConfiguration.from_dict(**value) for key, value in configs.items()}
            self.postgres_connection = None
            self.postgres_engine: Engine = None
            self.mongo_client = None
            self._mongodb_name = None
            self.mongo_db = None
            self.meta_data = None
            self.postgres_db = None
            self.postgres_session = None
            DatabaseConnection.__instance__ = self
        
    
    def connect_db(self, db: str) -> Union[Engine, MongoClient]:
        config = self.configs[db]
        if config.service in self.sql_serviecs:
            return create_engine(config.parse())
        elif config.service in self.no_sql_services:
            # print(config.parse())
            return MongoClient(config.parse())
    
    def register(self, database):
        if self.configs[database].service in self.sql_serviecs:
            self._register_postgres_connection(database)
        elif self.configs[database].service in self.no_sql_services:
            self._register_mongo_connection(database_name=database)
    
    def _register_postgres_connection(self, database_name):
        if self.postgres_connection is None or self.postgres_db != database_name:
            self.postgres_engine = self.connect_db(database_name)
            self.meta_data: MetaData = MetaData(bind=self.postgres_engine) if self.meta_data is None else self.meta_data
            self.postgres_db = database_name
            self.postgres_connection: Connection = self.postgres_engine.connect()
            self.postgres_session = scoped_session(sessionmaker())
            self.postgres_session.configure(bind=self.postgres_engine,autoflush=False, expire_on_commit=False)
    
    def _register_mongo_connection(self, database_name):
        if self.mongo_db is None or self._mongodb_name != database_name:
            self.mongo_client = self.connect_db(database_name)
            self._mongodb_name = database_name
            self.mongo_db = self.mongo_client[self.configs[database_name].database]
            
       

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
    },
    "mongo_treasure": {
        "service": "mongodb",
        "database": "treasure",
        "host": "localhost",
        "port": 27017
    },
    "postgres_treasure": {
        "service": "postgresql",
        "database": "treasure",
        "host": "localhost",
        "port": 5432,
        "driver": "psycopg2",
        "username": "postgres",
        "password": "piyush@103"
    }
}


CONNECTIONS = DatabaseConnection(**DATABASES)

def register_digger(postgres=True):
    if postgres:
        CONNECTIONS.register('postgres_digger')
        
    else:
        CONNECTIONS.register('mongo_digger')
    return CONNECTIONS


def register_treasure(postgres=True):
    if postgres:
        CONNECTIONS.register('postgres_treasure')
    else:
        CONNECTIONS.register('mongo_treasure')
    return CONNECTIONS

def register_connection(db):
    CONNECTIONS.register(db)
    return CONNECTIONS


