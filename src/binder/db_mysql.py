
from binder.conn import Connection, REPEATABLE_READ, _VALID_ISOLATION_LEVELS
from binder.sqlgen import DIALECT_MYSQL

_ISOLATION_SQL = "SET SESSION TRANSACTION ISOLATION LEVEL %s"

class MysqlConnection(Connection):

    def __init__(self, *args, **kwargs):
        import MySQLdb
        read_only = kwargs.pop('read_only', None)
        isolation_level = kwargs.pop('isolation_level', REPEATABLE_READ)
        assert isolation_level in _VALID_ISOLATION_LEVELS, \
            ("Unknown isolation_level", isolation_level)
        #
        assert not 'charset' in kwargs
        kwargs['charset'] = 'utf8'
        assert not 'use_unicode' in kwargs
        kwargs['use_unicode'] = True
        #
        dbconn = MySQLdb.connect(*args, **kwargs)
        dberror = MySQLdb.Error
        Connection.__init__(
            self, dbconn, dberror,
            DIALECT_MYSQL, "%s",
            read_only
            )
        isolation_sql = _ISOLATION_SQL % isolation_level
        self._execute(isolation_sql)

