
import sqlite3

from binder.conn import Connection

class Sqlite3Connection(Connection):

    def __init__(self, dbfile, read_only=None):
        dbconn = sqlite3.connect(dbfile)
        dberror = sqlite3.Error
        Connection.__init__(self, dbconn, dberror, read_only)

