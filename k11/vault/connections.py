from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from k11.logger import log, logging


def create_sql_engine(uri):
    return create_engine(uri)

def create_session(engine, autocommit=False, autoflush=False):
    return sessionmaker(autocommit=autocommit, autoflush=autoflush, bind=engine)


SqlBase = declarative_base()


def get_sql_database(database_uri):
    engine = create_sql_engine(database_uri)
    SessionLocal = create_session(engine)
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        log(e, level=logging.ERROR)
        db.close()


class ConnectionHandler:
    database_driver = {}

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



    

        