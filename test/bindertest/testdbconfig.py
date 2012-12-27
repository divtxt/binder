
import os
_bindertest_dir = os.path.dirname( __file__ )
DBFILE = os.path.join(_bindertest_dir, "test.db3")


from binder import Sqlite3Connection

def connect(read_only=None):
    return Sqlite3Connection(DBFILE, read_only)

