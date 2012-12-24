
import sqlite3
import itertools

from binder import sqlgen
from binder.table import SqlCondition, AND

_debug = False

class Connection:

    def __init__(self, dbfile, read_only=None):
        self._dbconn = sqlite3.connect(dbfile)
        self._last_ri = None
        self._read_only = read_only
        self.DbError = sqlite3.Error


    def commit(self):
        self._dbconn.commit()

    def rollback(self):
        self._dbconn.rollback()

    def close(self):
        self._close_last_ri()
        self._dbconn.close()


    def _close_last_ri(self):
        if self._last_ri:
            self._last_ri.close()
            self._last_ri = None

    def _execute(self, sql, values=[]):
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
        sql = sqlgen.create_table(table)
        self._execute(sql)

    def drop_table(self, table):
        # read only check
        self._check_write_ok()
        #
        sql = sqlgen.drop_table(table)
        self._execute(sql)


    def insert(self, table, row):
        # read only check
        self._check_write_ok()
        # gen sql
        sql, values, auto_id_used = sqlgen.insert(table, row)
        # execute sql
        cursor = self._execute(sql, values)
        assert cursor.rowcount == 1, \
            "insert(): expected rowcount=1, got %s" % cursor.rowcount
        # replace AutoIdCol None with actual id
        if auto_id_used:
            row[table.auto_id_col.col_name] = cursor.lastrowid


    def update(self, table, row, where):
        # read only check
        self._check_write_ok()
        # gen sql
        sql, values = sqlgen.update(table, row, where)
        # execute sql
        cursor = self._execute(sql, values)
        rowcount = cursor.rowcount
        return rowcount


    def update_by_id(self, table, row):
        # read only check
        self._check_write_ok()
        # gen sql
        sql, values = sqlgen.update_by_id(table, row)
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
        sql, values = sqlgen.delete(table, where)
        # execute sql
        cursor = self._execute(sql, values)
        rowcount = cursor.rowcount
        return rowcount


    def delete_by_id(self, table, row_id):
        # read only check
        self._check_write_ok()
        # gen sql
        sql, values = sqlgen.delete_by_id(table, row_id)
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


    def get(self, table, row_id):
        # construct where clause
        auto_id_col = table.auto_id_col
        assert auto_id_col, \
                "get(): table '%s' does not have AutoIdCol" % table.table_name
        assert not row_id is None, "get(): cannot use None for AutoIdCol"
        q_auto_id_col = getattr(table.q, auto_id_col.col_name)
        where_id = (q_auto_id_col == row_id)
        # call select_one
        return self.select_one(table, where_id)


    def xselect(self, table, where=None, order_by=None):
        # gen sql
        sql, values = sqlgen.select(table, where, order_by)
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
        sql, values = sqlgen.select_distinct(table, qcol, where, order_by)
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
        #row = dict(itertools.izip(self.col_names, values))
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
