
from binder import sqlgen
from binder.sqlgen import DIALECT_POSTGRES

_debug = False


READ_COMMITTED = "READ COMMITTED"
REPEATABLE_READ = "REPEATABLE READ"

_VALID_ISOLATION_LEVELS = [
    READ_COMMITTED,
    REPEATABLE_READ,
    ]


class Connection:

    def __init__(self, dbconn, dberror, dialect, paramstr, read_only):
        self._is_open = True
        self._dbconn = dbconn
        self._last_ri = None
        self._read_only = read_only
        self.DbError = dberror
        self.dialect = dialect
        self.paramstr = paramstr


    def commit(self):
        self._dbconn.commit()

    def rollback(self):
        self._dbconn.rollback()

    def close(self):
        self._close_last_ri()
        self._dbconn.close()
        self._dbconn = None
        self._is_open = False


    def _close_last_ri(self):
        if self._last_ri:
            self._last_ri.close()
            self._last_ri = None

    def _execute(self, sql, values=[]):
        assert self._is_open, "Connection is closed"
        if _debug:
            print "DEBUG: _execute(%s, %s)" % (repr(sql), values)
        self._close_last_ri()
        cursor = self._dbconn.cursor()
        cursor.execute(sql, values)
        return cursor

    def _check_write_ok(self):
        if self._read_only:
            raise Exception, "Connection is read only: " + self._read_only


    def create_table(self, table):
        # read only check
        self._check_write_ok()
        #
        sql = sqlgen.create_table(self.dialect, table)
        self._execute(sql)

    def drop_table(self, table, if_exists=False):
        # read only check
        self._check_write_ok()
        #
        sql = sqlgen.drop_table(table, if_exists)
        self._execute(sql)

    def drop_table_if_exists(self, table):
        self.drop_table(table, True)


    def insert(self, table, row):
        # read only check
        self._check_write_ok()
        # gen sql
        sql, values, auto_id_used = \
            sqlgen.insert(table, row, self.dialect, self.paramstr)
        # execute sql
        cursor = self._execute(sql, values)
        assert cursor.rowcount == 1, \
            "insert(): expected rowcount=1, got %s" % cursor.rowcount
        # replace AutoIdCol None with actual id
        if auto_id_used:
            if self.dialect == DIALECT_POSTGRES:
                new_id = cursor.fetchone()[0]
            else:
                new_id = cursor.lastrowid
            row[table.auto_id_col.col_name] = new_id



    def update(self, table, row, where):
        # read only check
        self._check_write_ok()
        # gen sql
        sql, values = sqlgen.update(
            table, row, where, self.dialect, self.paramstr
            )
        # execute sql
        cursor = self._execute(sql, values)
        rowcount = cursor.rowcount
        return rowcount


    def update_by_id(self, table, row):
        # read only check
        self._check_write_ok()
        # gen sql
        sql, values = sqlgen.update_by_id(table, row, self.paramstr)
        # execute sql
        cursor = self._execute(sql, values)
        rc = cursor.rowcount
        if rc == 1:
            return True
        if rc == 0:
            return False
        row_id = values[-1]
        assert False, (
                "update_by_id(): more than 1 row updated", 
                (table.table_name, table.auto_id_col.col_name, row_id, rc)
            )


    def delete(self, table, where):
        # read only check
        self._check_write_ok()
        # gen sql
        sql, values = sqlgen.delete(
            table, where, self.dialect, self.paramstr
            )
        # execute sql
        cursor = self._execute(sql, values)
        rowcount = cursor.rowcount
        return rowcount


    def delete_by_id(self, table, row_id):
        # read only check
        self._check_write_ok()
        # gen sql
        sql, values = sqlgen.delete_by_id(table, row_id, self.paramstr)
        # execute sql
        cursor = self._execute(sql, values)
        rc = cursor.rowcount
        if rc == 1:
            return True
        if rc == 0:
            return False
        row_id = values[-1]
        assert False, (
                "delete_by_id(): more than 1 row deleted",
                (table.table_name, table.auto_id_col.col_name, row_id, rc)
            )


    def select_by_id(self, table, row_id):
        # construct where clause
        auto_id_col = table.auto_id_col
        assert auto_id_col, \
            "select_by_id(): table '%s' does not have AutoIdCol" % \
                table.table_name
        assert not row_id is None, \
            "select_by_id(): cannot use None for AutoIdCol"
        q_auto_id_col = getattr(table.q, auto_id_col.col_name)
        where_id = (q_auto_id_col == row_id)
        # call select_one
        return self.select_one(table, where_id)

    get = select_by_id

    def xselect(self, table, where=None, order_by=None):
        # gen sql
        sql, values = sqlgen.select(
            table, where, order_by, self.dialect, self.paramstr
            )
        # execute sql
        cursor = self._execute(sql, values)
        # result iterator
        i = ResultIterator(cursor, table, self.DbError, where)
        self._last_ri = i
        return i

    def select(self, table, where=None, order_by=None):
        return list(
                self.xselect(table, where, order_by)
            )


    def select_one(self, table, where=None, order_by=None):
        #
        i = self.xselect(table, where, order_by)
        #
        row = None
        try:
            row = i.next()
            i.next()
            assert False, (
                    "select_one(): more than 1 row",
                    (table.table_name, i.where) #, rc)
                )
        except StopIteration:
            return row


    def xselect_distinct(self, table, qcol, where=None, order_by=None):
        # gen sql
        sql, values = sqlgen.select_distinct(
            table, qcol, where, order_by, self.dialect, self.paramstr
            )
        # execute sql
        cursor = self._execute(sql, values)
        # result iterator
        i = SelectDistinctResultIterator(cursor, self.DbError)
        self._last_ri = i
        return i

    def select_distinct(self, table, qcol, where=None, order_by=None):
        return list(
                self.xselect_distinct(table, qcol, where, order_by)
            )




class ResultIterator:

    def __init__(self, cursor, table, DbError, where):
        self.cursor = cursor
        self.table = table
        self.DbError = DbError
        self.where = where
        self.closed = False

    def __iter__(self):
        return self

    def next(self):
        if self.closed:
            raise self.DbError, "Result cursor closed."
        row = {}
        values = self.cursor.fetchone()
        if values is None:
            self.cursor = None
            raise StopIteration
        cols = self.table.cols
        for i in range(len(cols)):
            col = cols[i]
            dbvalue = values[i]
            value = col.db_to_py(dbvalue)
            row[col.col_name] = value
        return row


    def close(self):
        self.closed = True
        if self.cursor:
            self.cursor.close()




class SelectDistinctResultIterator:

    def __init__(self, cursor, DbError):
        self.cursor = cursor
        self.DbError = DbError
        self.closed = False

    def __iter__(self):
        return self

    def next(self):
        if self.closed:
            raise self.DbError, "Result cursor closed."
        values = self.cursor.fetchone()
        if values is None:
            self.cursor = None
            raise StopIteration
        #assert len(values) == 1
        return values[0]

    def close(self):
        self.closed = True
        if self.cursor:
            self.cursor.close()
