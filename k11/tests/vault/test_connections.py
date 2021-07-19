import unittest
from k11.vault.connections import get_sql_database, ConnectionHandler
from unittest import TestCase


def test_get_database():
    uri = "postgresql+psycopg2://postgres:piyush@103@localhost:5432/test"
    database = get_sql_database(uri)
    assert database != None, "Database creation failed"


class TestConnectionHandler(TestCase):

    def setUp(self) -> None:
        self.settings = {
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
                "username": "postgres",
                "password": "piyush@103"
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

