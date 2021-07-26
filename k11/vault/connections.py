from typing import Dict
from playhouse.postgres_ext import PostgresqlExtDatabase
from mongoengine import connect, disconnect
from sqlalchemy.orm.session import Session, sessionmaker


def create_sql_engine(conf):
    return PostgresqlExtDatabase(
        conf["database"],
        user=conf["username"],
        password=conf['password'],
        port=conf['port'],
        host=conf['host']
    )

def disconnect_mongo_engine(alias):
    disconnect(alias=alias)


# SqlBase = declarative_base()

class ConnectionHandler:
    database_driver = {}
    engines = {}
    mongo_engines = {}

    def __init__(self, settings) -> None:
        self.settings = settings
    
    def add_service_driver(self, name, value):
        self.database_driver[name] = value
    
    def flush_driver(self, name):
        if name in self.database_driver:
            del self.database_driver[name]
    
    @staticmethod
    def _get_database_uri(conf):
        driver  =  '+' + conf['driver'] if 'driver' in conf else ''
        auth = ''
        if 'username' in conf:
            auth = conf['username']
        if 'password' in conf:
            auth += f':{conf["password"]}'
        if len(auth) > 0:
            auth += '@'
        return f"{conf['service']}{driver}://{auth}{conf['host']}:{conf['port']}/{conf['database']}"
    
    def get_database_uri(self,alias):
        if alias not in self.settings:
            raise KeyError(f"{alias} is not present in settings.")
        conf = self.settings[alias]
        if conf['service'] in self.database_driver:
            conf['driver'] = self.database_driver[conf['service']]
        return self._get_database_uri(conf)

    def bind_sql_engine(self,alias, conf):
        self.engines[alias] = create_sql_engine(conf)
    
    def bind_mongo_engine(self, alias, conf):
        uri = self._get_database_uri(conf)
        engine = connect(alias=alias, host=uri)
        self.mongo_engines[alias] = engine
    
    def mount_all_engines(self):
        for alias, conf in self.settings.items():
            if conf['is_sql']:
                self.bind_sql_engine(alias, conf)
            else:
                self.bind_mongo_engine(alias, conf)
    def mount_sql_engines(self):
        for alias, conf in self.settings.items():
            if conf['is_sql']:
                self.bind_sql_engine(alias,conf)
    
    def mount_mongo_engines(self):
        for alias, conf in self.settings.items():
            if not conf['is_sql']:
                self.bind_mongo_engine(alias,conf)
    
    def disconnect_mongo_engines(self):
        for alias, conf in self.settings.items():
            if not conf['is_sql']:
                disconnect(alias)
    
    def dispose_sql_engines(self):
        for _,db  in self.engines.items():
            db.close()

    



    
    

    

        