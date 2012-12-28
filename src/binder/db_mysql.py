
from binder.conn import Connection

class MysqlConnection(Connection):

    def __init__(self, *args, **kwargs):
        import MySQLdb
        read_only = kwargs.pop('read_only', None)
        dbconn = MySQLdb.connect(*args, **kwargs)
        dberror = MySQLdb.Error
        Connection.__init__(self, dbconn, dberror, read_only)

