import pytest
from sqlalchemy.sql.schema import Column, Table
from sqlalchemy import String, Integer
from ..app import CONNECTIONS, register_digger


def test_database_postgres_connection():
    # Checking digger database registration
    register_digger()
    assert CONNECTIONS.postgres_connection != None, "Connection is failed to load"
    assert CONNECTIONS.postgres_db == 'postgres_digger', 'Register function failed to load database'



def test_database_mongo_connection():
    pass

def create_test_table():
    register_digger()
    student = Table('students', CONNECTIONS.meta_data,
    Column('id', Integer, primary_key=True),
    Column('name', String(75)) 
    )
    # student.create(CONNECTIONS.postgres_engine)
    return student


def test_execute_in_postgres_connection():
    student = create_test_table()
    # inserting values
    vals = [
        {
            "id": 1,
            "name": "Piyush"
        },
        {
            "id": 2,
            "name": "Jaiswal"
        },
        {
            "id": 3,
            "name": "Vatsalya"
        },
        {
            "id": 4,
            "name": "Tanmay"
        }
    ]
    # CONNECTIONS.postgres_session.bulk_insert_mappings(student, vals)
    CONNECTIONS.postgres_session.commit()
    assert CONNECTIONS.postgres_connection.execute(student.count()) == 4, "Insert commend isn't working"


