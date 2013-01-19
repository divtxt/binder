
from binder.conn import Connection, READ_COMMITTED, REPEATABLE_READ, \
    _VALID_ISOLATION_LEVELS
from binder.sqlgen import DIALECT_POSTGRES


_psycopg2_imported = False
_ISOLATION_LEVEL_MAP = {}

def _import_psycopg2():
    if _psycopg2_imported:
        return
    #
    global psycopg2
    import psycopg2
    #
    from psycopg2 import extensions
    _ISOLATION_LEVEL_MAP.update({
        #ISOLATION_LEVEL_AUTOCOMMIT
        #ISOLATION_LEVEL_READ_UNCOMMITTED
        READ_COMMITTED: extensions.ISOLATION_LEVEL_READ_COMMITTED,
        REPEATABLE_READ: extensions.ISOLATION_LEVEL_REPEATABLE_READ,
        #ISOLATION_LEVEL_SERIALIZABLE
        })


class PostgresConnection(Connection):

    def __init__(self, *args, **kwargs):
        _import_psycopg2()
        read_only = kwargs.pop('read_only', None)
        isolation_level = kwargs.pop('isolation_level', REPEATABLE_READ)
        assert isolation_level in _VALID_ISOLATION_LEVELS, \
            ("Unknown isolation_level", isolation_level)
        #
#        assert not 'charset' in kwargs
#        kwargs['charset'] = 'utf8'
#        assert not 'use_unicode' in kwargs
#        kwargs['use_unicode'] = True
        #
        pg_isolation_level = _ISOLATION_LEVEL_MAP[isolation_level]
        #
        dbconn = psycopg2.connect(*args, **kwargs)
        dbconn.set_session(pg_isolation_level)
        dberror = psycopg2.Error
        Connection.__init__(
            self, dbconn, dberror,
            DIALECT_POSTGRES, "%s",
            read_only
            )
