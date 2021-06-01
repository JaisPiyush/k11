from ..base import BaseDatabaseWrapper
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy import create_engine


class PostgresDatabaseWrapper(BaseDatabaseWrapper):
    service="postgres"
    driver="psycopg2"
    connection: Session

    def get_connection_params(self):
        if getattr(self.configuration, "service", None) is None:
            self.configuration.service = self.service
        if getattr(self.configuration, "driver", None) is None:
            self.configuration.driver = self.service
        return self.configuration.parse()
    
    def get_new_connection(self, conn_params):
        self.engine = create_engine(conn_params)
        _session = sessionmaker(bind=self.engine)
        _conn = self.engine.connect()
        return _session(bind=self.engine)
    
    def connect(self):
        conn_params = self.get_connection_params()
        self.connection = self.get_new_connection(conn_params)
    
    def create_cursor(self, name):
        cursor = self.connection



        
    

        