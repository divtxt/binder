
from binder import SqliteConnection
from binder import MysqlConnection
import os


# SQLite
_bindertest_dir = os.path.dirname( __file__ )
DBFILE = os.path.join(_bindertest_dir, "test.db3")
def connect_sqlite(read_only=None):
    return SqliteConnection(DBFILE, read_only)

# MySQL - modify as needed
MYSQL_TESTDB = {
    "host": "localhost",
    "user": "bindertest",
    "passwd": "binderpassword",
    "db": "bindertestdb",
    }
def connect_mysql(read_only=None, isolation_level=None):
    d = dict(MYSQL_TESTDB)
    d['read_only'] = read_only
    if isolation_level:
        d['isolation_level'] = isolation_level
    return MysqlConnection(**d)

# Postgres - modify as needed
def connect_postgres(read_only=None, isolation_level=None):
    raise NotImplementedError


# Test DB - modify as needed
connect = connect_sqlite
#connect = connect_mysql
#connect = connect_postgres

