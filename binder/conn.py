
from binder import sqlgen

_debug = False


READ_COMMITTED = "READ COMMITTED"
REPEATABLE_READ = "REPEATABLE READ"

_VALID_ISOLATION_LEVELS = [
    READ_COMMITTED,
    REPEATABLE_READ,
    ]


class Connection:

    def __init__(self, dbconn, dberror, sqlite, read_only):
        self._is_open = True
        self._dbconn = dbconn
        self._last_ri = None
        self._read_only = read_only
        self.DbError = dberror
        self.sqlite = sqlite
        self.paramstr = "?" if sqlite else "%s"


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
        sql = sqlgen.create_table(self.sqlite, table)
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
            sqlgen.insert(table, row, self.sqlite, self.paramstr)
        # execute sql
        cursor = self._execute(sql, values)
        assert cursor.rowcount == 1, \
            "insert(): expected rowcount=1, got %s" % cursor.rowcount
        # replace AutoIdCol None with actual id
        if auto_id_used:
            if not self.sqlite:
                new_id = cursor.fetchone()[0]
            else:
                new_id = cursor.lastrowid
            row[table.auto_id_col.col_name] = new_id



    def update(self, table, row, where):
        # read only check
        self._check_write_ok()
        # gen sql
        sql, values = sqlgen.update(
            table, row, where, self.sqlite, self.paramstr
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


    def delete(self, table, where=None):
        # read only check
        self._check_write_ok()
        # gen sql
        sql, values = sqlgen.delete(
            table, where, self.sqlite, self.paramstr
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

    def select(self, table, where=None, order_by=None):
        # gen sql
        sql, values = sqlgen.select(
            table, where, order_by, self.sqlite, self.paramstr
            )
        # execute sql
        cursor = self._execute(sql, values)
        # convert results
        cols = table.cols
        r = range(len(cols))
        db_to_pys = [c.db_to_py for c in cols]
        cnames = [c.col_name for c in cols]
        l = []
        for values in cursor.fetchall():
            row = {}
            for i in r:
                row[cnames[i]] = db_to_pys[i](values[i])
            l.append(row)
        #
        return l


    def select_one(self, table, where=None, order_by=None):
        #
        l = self.select(table, where, order_by)
        #
        if l:
            assert len(l) == 1, (
                    "select_one(): more than 1 row",
                    (table.table_name, where) #, rc)
                )
            return l[0]
        else:
            return None


    def select_distinct(self, table, qcol, where=None, order_by=None):
        # gen sql
        sql, values = sqlgen.select_distinct(
            table, qcol, where, order_by, self.sqlite, self.paramstr
            )
        # execute sql
        cursor = self._execute(sql, values)
        # convert results
        l = []
        for values in cursor.fetchall():
            #assert len(values) == 1
            l.append(values[0])
        #
        return l

