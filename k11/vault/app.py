from .connections import ConnectionHandler 

"""
All the database configurations must be defined here
"""

DATABASES = {
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
        "driver": "psycopg2",
        "username": "postgres",
        "password": "piyush@103",
        "is_sql": True
    },
    "mongo_treasure": {
        "service": "mongodb",
        "database": "treasure",
        "host": "localhost",
        "port": 27017,
        "is_sql": False
    },
    "postgres_treasure": {
        "service": "postgresql",
        "database": "treasure",
        "host": "localhost",
        "port": 5432,
        "driver": "psycopg2",
        "username": "postgres",
        "password": "piyush@103",
        "is_sql": True
    },
    "mongo_grave": {
        "service": "mongodb",
        "database": "grave",
        "host": "localhost",
        "port": 27017,
        "is_sql": False
    },
    "mongo_admin": {
        "service": "mongodb",
        "database": "system",
        "host": "localhost",
        "port": 27017,
        "is_sql": False
    },
}

connection_handler = ConnectionHandler(DATABASES)



