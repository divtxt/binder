
from binder import SqliteConnection
from binder import PostgresConnection
import os


# SQLite
_bindertest_dir = os.path.dirname( __file__ )
DBFILE = os.path.join(_bindertest_dir, "test.db3")
def connect_sqlite(read_only=None):
    return SqliteConnection(DBFILE, read_only)

# Postgres test database details - modify as needed
TEST_HOST = "localhost"
TEST_USER = "bindertest"
TEST_PASSWORD = "binderpassword"
TEST_DATABASE = "bindertestdb"

# Postgres - modify as needed
def connect_postgres(read_only=None, isolation_level=None):
    d = {
        "host": TEST_HOST,
        "user": TEST_USER,
        "password": TEST_PASSWORD,
        "database": TEST_DATABASE,
        }
    d['read_only'] = read_only
    if isolation_level:
        d['isolation_level'] = isolation_level
    return PostgresConnection(**d)


# Test DB - modify as needed
#connect = connect_sqlite
connect = connect_postgres

