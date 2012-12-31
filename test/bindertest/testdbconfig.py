
from binder import Sqlite3Connection
from binder import MysqlConnection
import os


# sqlite3
_bindertest_dir = os.path.dirname( __file__ )
DBFILE = os.path.join(_bindertest_dir, "test.db3")
def connect_sqlite3(read_only=None):
    return Sqlite3Connection(DBFILE, read_only)

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


# Test DB - modify as needed
connect = connect_sqlite3
#connect = connect_mysql

