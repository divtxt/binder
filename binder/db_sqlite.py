
import sqlite3

from binder.conn import Connection

class SqliteConnection(Connection):

    def __init__(self, dbfile, read_only=False):
        dbconn = sqlite3.connect(dbfile)
        dberror = sqlite3.Error
        Connection.__init__(self, dbconn, dberror, True, read_only)

