
from binder.col import *
from binder.table import SqlCondition, SqlSort, AND, OR, QueryCol

_COL_TYPE = {
    AutoIdCol: "INTEGER PRIMARY KEY",
    IntCol: "INTEGER",
    BoolCol: "INTEGER",
    StringCol: "TEXT",
    DateCol: "TEXT",
    DateTimeUTCCol: "TEXT",
}

def create_table(table):
    col_defs = []
    for col in table.cols:
        col_type = _COL_TYPE[col.__class__]
        col_def = "%s %s" % (col.col_name, col_type)
        if col.not_null and not col.__class__ is AutoIdCol:
            col_def = col_def + " NOT NULL"
        if col.__class__ is StringCol and col.unique:
            col_def = col_def + " UNIQUE"
        col_defs.append(col_def)
    cols_sql = ",\n    ".join(col_defs)
    sql = "CREATE TABLE %s (\n    %s\n)" \
        % (table.table_name, cols_sql)
    return sql


def drop_table(table, if_exists):
    sql = "DROP TABLE IF EXISTS %s" if if_exists else "DROP TABLE %s"
    sql = sql % table.table_name
    return sql



def insert(table, row):
    values = []
    col_names = []
    value_qs = []
    auto_id_col = table.auto_id_col
    auto_id_used = False
    for col in table.cols:
        col_name = col.col_name
        col_names.append(col_name)
        value = row[col_name]
        col.check_value(value)
        if value is None:
            value_qs.append('NULL')
            if col is auto_id_col:
                auto_id_used = True
        else:
            value_qs.append('?')
            value = col.py_to_db(value)
            values.append(value)
    col_names_sql = ",".join(col_names)
    values_sql = ",".join(value_qs)
    sql = "INSERT INTO %s (%s) VALUES (%s)" \
        % (table.table_name, col_names_sql, values_sql)
    return sql, values, auto_id_used


def update(table, row, where):
    values = []
    col_names = []
    auto_id_col = table.auto_id_col
    for col in table.cols:
        col_name = col.col_name
        value = row[col_name]
        col.check_value(value)
        if value is None:
            assert not col is auto_id_col, \
                "update(): cannot use None for AutoIdCol"
            col_names.append(col_name + "=NULL")
        else:
            col_names.append(col_name + "=?")
            value = col.py_to_db(value)
            values.append(value)
    col_sql = ",".join(col_names)
    sql_parts = ["UPDATE", table.table_name, "SET", col_sql]
    if not where is None:
        cond_sql, where_values = _sqlcond_to_sql(where)
        sql_parts.append("WHERE")
        sql_parts.append(cond_sql)
        values.extend(where_values)
    sql = " ".join(sql_parts)
    return sql, values


def update_by_id(table, row):
    assert table.auto_id_col, \
        "update_by_id(): table '%s' does not have AutoIdCol" \
            % table.table_name
    values = []
    col_names = []
    auto_id_col = table.auto_id_col
    row_id = None
    for col in table.cols:
        col_name = col.col_name
        value = row[col_name]
        col.check_value(value)
        if col is auto_id_col:
            assert not value is None, "update_by_id(): cannot use None for AutoIdCol"
            row_id = value
        else:
            col_names.append(col_name)
            value = col.py_to_db(value)
            values.append(value)
    values.append(row_id)
    col_sqls = [(col_name + "=?") for col_name in col_names]
    col_sql = ",".join(col_sqls)
    sql = "UPDATE %s SET %s" % (table.table_name, col_sql)
    sql = "%s WHERE %s=?" % (sql, auto_id_col.col_name)
    return sql, values


def delete(table, where):
    cond_sql, values = _sqlcond_to_sql(where)
    sql = "DELETE FROM %s WHERE %s" % (table.table_name, cond_sql)
    return sql, values


def delete_by_id(table, row_id):
    assert table.auto_id_col, \
        "delete_by_id(): table '%s' does not have AutoIdCol" \
            % table.table_name
    assert not row_id is None, "delete_by_id(): cannot use None for AutoIdCol"
    auto_id_col = table.auto_id_col
    auto_id_col.check_value(row_id)
    sql = "DELETE FROM %s WHERE %s=?" \
        % (table.table_name, auto_id_col.col_name)
    values = [row_id]
    return sql, values


def select(table, where=None, order_by=None):
    col_names = [col.col_name for col in table.cols]
    col_names_sql = ",".join(col_names)
    sql_parts = ["SELECT", col_names_sql, "FROM", table.table_name]
    if where:
        sql_parts.append("WHERE")
        cond_sql, values = _sqlcond_to_sql(where)
        sql_parts.append(cond_sql)
    else:
        values = []
    if order_by:
        sql_parts.append("ORDER BY")
        sql_parts.append(_sqlsort_to_sql(order_by))
    sql = " ".join(sql_parts)
    return sql, values


def select_distinct(table, qcol, where=None, order_by=None):
    assert isinstance(qcol, QueryCol), "Column must be instance of QueryCol"
    col_name = qcol._col.col_name
    sql_parts = ["SELECT DISTINCT", col_name, "FROM", table.table_name]
    if where:
        sql_parts.append("WHERE")
        cond_sql, values = _sqlcond_to_sql(where)
        sql_parts.append(cond_sql)
    else:
        values = []
    if order_by:
        sql_parts.append("ORDER BY")
        sql_parts.append(_sqlsort_to_sql(order_by))
        assert qcol._col is order_by.col, \
            "SELECT DISTINCT column must match 'order_by' column"
    sql = " ".join(sql_parts)
    return sql, values


def _sqlcond_to_sql(where):
    combiner = " AND "
    if isinstance(where, SqlCondition):
        sqlconds = [where]
    elif isinstance(where, AND):
        sqlconds = where.sqlconds
    elif isinstance(where, OR):
        combiner = " OR "
        sqlconds = where.sqlconds
    else:
        raise AssertionError, "Unsupported 'where' clause: %s" % where
    values = []
    cond_sqls = []
    for sqlcond in sqlconds:
        if sqlcond.op == "=":
            if sqlcond.other == None:
                cond_sql = "%s is NULL"
            else:
                cond_sql = "%s=?"
                value = sqlcond.col.py_to_db(sqlcond.other)
                values.append(value)
        elif sqlcond.op in [">", ">=", "<", "<="]:
            assert not isinstance(sqlcond.col, BoolCol), \
                "Op '%s' does not support BoolCol" % sqlcond.op
            assert not isinstance(sqlcond.col, DateTimeUTCCol), \
                "Op '%s' does not support DateTimeUTCCol" % sqlcond.op
            assert not sqlcond.other is None, \
                "Op '%s' does not support None" % sqlcond.op
            cond_sql = "%%s%s?" % sqlcond.op
            value = sqlcond.col.py_to_db(sqlcond.other)
            values.append(value)
        elif sqlcond.op == "YEAR":
            assert isinstance(sqlcond.col, DateCol), \
                "YEAR condition can only be used for DateCol"
            assert sqlcond.other != None, \
                "YEAR condition cannot use None"
            cond_sql = "%s LIKE ?"
            value = "%d-%%" % sqlcond.other.year
            values.append(value)
        elif sqlcond.op == "YEAR_MONTH":
            assert isinstance(sqlcond.col, DateCol), \
                "YEAR_MONTH condition can only be used for DateCol"
            assert sqlcond.other != None, \
                "YEAR_MONTH condition cannot use None"
            cond_sql = "%s LIKE ?"
            value = "%d-%02d-%%" % (sqlcond.other.year, sqlcond.other.month)
            values.append(value)
        elif sqlcond.op == "LIKE":
            assert isinstance(sqlcond.col, StringCol), \
                "LIKE condition can only be used for StringCol"
            assert sqlcond.other != None, \
                "LIKE condition cannot use None"
            cond_sql = "%s LIKE ?"
            value = sqlcond.other
            values.append(value)
        else:
            raise AssertionError, "Unsupported op '%s'" % sqlcond.op
        cond_sql = cond_sql % sqlcond.col.col_name
        cond_sqls.append(cond_sql)
    sql = combiner.join(cond_sqls)
    return sql, values

def _sqlsort_to_sql(order_by):
    assert isinstance(order_by, SqlSort), "'order_by' must be SqlSort"
    sql = order_by.col.col_name
    if isinstance(order_by.col, StringCol):
        sql = sql + " COLLATE NOCASE"
    if order_by.asc:
        sql = sql + " ASC"
    else:
        sql = sql + " DESC"
    return sql


