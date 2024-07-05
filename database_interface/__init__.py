from database_interface.database_interface import DatabaseInterface

LOCAL_ENV = "local"
DEV_ENV = "development"
PROD_ENV = "production"

ORIGIN_ROLE = "origin"
TARGET_ROLE = "target"
REFERENCE_ROLE = "reference"

DB_ORIGIN_SETTINGS = {
    LOCAL_ENV: {
        "db": '',
        "dbname": '',
        "host": '',
        "port": 0,
        "user": '',
        "passwd": ''
    },

    DEV_ENV: {
        "db": '',
        "dbname": '',
        "host": '',
        "port": 0,
        "user": '',
        "passwd": ''
    },

    PROD_ENV: {
        "db": '',
        "dbname": '',
        "host": '',
        "port": 0,
        "user": '',
        "passwd": ''
    }
}

DB_TARGET_SETTINGS = {
    LOCAL_ENV: {
        'base_url': '',
        "db": '',
        "dbname": '',
        "host": '',
        "port": 0,
        "user": '',
        "passwd": ''
    },
    DEV_ENV: {
        'base_url': '',
        "db": '',
        "dbname": '',
        "host": '',
        "port": 0,
        "user": '',
        "passwd": ''
    }
}

GET_COMPLETE_SCHEMA = 'get_complete_schema'

DB_AUTOCOMMIT = True
DB_ISOLATION_LEVEL = 1

DB_SCRIPT_PATH = 'database_interface/scripts'

SKIP_INSERT_CONCEPT_CONFIRMATION = False


