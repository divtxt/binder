
from binder.conn import Connection, READ_COMMITTED, REPEATABLE_READ, \
    _VALID_ISOLATION_LEVELS
from binder.sqlgen import DIALECT_POSTGRES


_psycopg2_imported = False
_ISOLATION_LEVEL_MAP = {}

def _import_psycopg2():
    global _psycopg2_imported, psycopg2, _ISOLATION_LEVEL_MAP
    #
    if _psycopg2_imported:
        return
    #
    import psycopg2
    _psycopg2_imported = True
    #
    from psycopg2 import extensions
    #
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
    #
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
