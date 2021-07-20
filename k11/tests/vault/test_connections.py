import unittest
from k11.vault.connections import ConnectionHandler
from unittest import TestCase



class TestConnectionHandler(TestCase):

    def setUp(self) -> None:
        self.settings = {
            "mongo_digger": {
                "service": "mongodb",
                "database": "digger",
                "host": "localhost",
                "port": 27017,
                "is_sql": False

            },
            "postgres_digger": {
                "service": "postgresql",
                "database": "postgres",
                "host": "localhost",
                "port": 5432,
                "username": "postgres",
                "password": "piyush@103",
                "is_sql": True
            },
        }
        self.handler_class = ConnectionHandler
        self.handler = ConnectionHandler(self.settings)
        return super().setUp()
    
    def test_get_database_uri_without_adding_driver(self):
        self.handler.flush_driver('postgresql')
        outputs = [self.handler.get_database_uri('mongo_digger'), self.handler.get_database_uri('postgres_digger')]
        real = ['mongodb://localhost:27017/digger', "postgresql://postgres:piyush@103@localhost:5432/postgres"]
        self.assertListEqual(outputs, real)
    
    def test_get_database_uri(self):
        self.handler.add_service_driver('postgresql', 'psycopg2')
        real = ['mongodb://localhost:27017/digger', "postgresql+psycopg2://postgres:piyush@103@localhost:5432/postgres"]
        outputs = [self.handler.get_database_uri('mongo_digger'), self.handler.get_database_uri('postgres_digger')]
        self.assertListEqual(outputs, real)
    
    def test_mount_no_sql_engines(self):
        self.handler.mount_mongo_engines()
        self.assertEqual('mongo_digger' in self.handler.mongo_engines, True)

