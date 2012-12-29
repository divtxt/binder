
from binder.conn import Connection
from binder.sqlgen import DIALECT_MYSQL

class MysqlConnection(Connection):

    def __init__(self, *args, **kwargs):
        import MySQLdb
        read_only = kwargs.pop('read_only', None)
        dbconn = MySQLdb.connect(*args, **kwargs)
        dberror = MySQLdb.Error
        Connection.__init__(self, dbconn, dberror, DIALECT_MYSQL, read_only)

