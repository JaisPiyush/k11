import pytest
from sqlalchemy.sql.schema import Column, Table
from sqlalchemy import String, Integer
from ..app import CONNECTIONS, register_digger


def test_database_postgres_connection():
    # Checking digger database registration
    register_digger()
    assert CONNECTIONS.postgres_connection != None, "Connection is failed to load"
    assert CONNECTIONS.postgres_db == 'postgres_digger', 'Register function failed to load database'

