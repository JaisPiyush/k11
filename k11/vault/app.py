from .connections import ConnectionHandler 

"""
All the database configurations must be defined here
"""

MONGO_PORT = 27017
MONGO_HOST = "localhost"
POSTG_PORT = 5432
POSTG_HOST = "localhost"

DATABASES = {
    "mongo_digger": {
        "service": "mongodb",
        "database": "digger",
        "host": MONGO_HOST,
        "port": MONGO_PORT,
        "is_sql": False
        
    },
    "postgres_digger": {
        "service": "postgresql",
        "database": "postgres",
        "host": POSTG_HOST,
        "port": POSTG_PORT,
        "driver": "psycopg2",
        "username": "postgres",
        "password": "piyush@103",
        "is_sql": True
    },
    "mongo_treasure": {
        "service": "mongodb",
        "database": "treasure",
        "host": MONGO_HOST,
        "port": MONGO_PORT,
        "is_sql": False
    },
    "postgres_treasure": {
        "service": "postgresql",
        "database": "treasure",
        "host": POSTG_HOST,
        "port": POSTG_PORT,
        "driver": "psycopg2",
        "username": "postgres",
        "password": "piyush@103",
        "is_sql": True
    },
    "mongo_grave": {
        "service": "mongodb",
        "database": "grave",
        "host": MONGO_HOST,
        "port": MONGO_PORT,
        "is_sql": False
    },
    "mongo_admin": {
        "service": "mongodb",
        "database": "errors",
        "host": MONGO_HOST,
        "port": MONGO_PORT,
        "is_sql": False
    },
}

connection_handler = ConnectionHandler(DATABASES)



