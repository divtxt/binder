
import os
_bindertest_dir = os.path.dirname( __file__ )
DBFILE = os.path.join(_bindertest_dir, "test.db3")


from binder.conn import Connection

def connect(read_only=None):
    return Connection(DBFILE, read_only)

