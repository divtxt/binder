
import sqlite3

from binder.conn import Connection
from binder.sqlgen import DIALECT_SQLITE3

class Sqlite3Connection(Connection):

    def __init__(self, dbfile, read_only):
        dbconn = sqlite3.connect(dbfile)
        dberror = sqlite3.Error
        Connection.__init__(
            self, dbconn, dberror,
            DIALECT_SQLITE3, "?",
            read_only
            )

