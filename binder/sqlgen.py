
from binder.col import *
from binder.table import SqlCondition, SqlSort, AND, OR, QueryCol


_COL_TYPE_SQLITE = {
    AutoIdCol: "INTEGER PRIMARY KEY",
    IntCol: "INTEGER",
    FloatCol: "REAL",
    BoolCol: "INTEGER",
    UnicodeCol: "TEXT",
    DateCol: "TEXT",
    DateTimeUTCCol: "TEXT",
}

_COL_TYPE_POSTGRES = {
    AutoIdCol: "SERIAL UNIQUE",
    IntCol: "BIGINT",
    FloatCol: "DOUBLE PRECISION",
    BoolCol: "BOOLEAN",
    UnicodeCol: "VARCHAR",
    DateCol: "DATE",
    DateTimeUTCCol: "TIMESTAMP",
}

def create_table(sqlite, table):
    if sqlite:
        col_types = _COL_TYPE_SQLITE
        collate_nocase_name = "NOCASE"
    else:
        col_types = _COL_TYPE_POSTGRES
        collate_nocase_name = '"C"'
    col_defs = []
    for col in table.cols:
        col_type = col_types[col.__class__]
        col_def = "%s %s" % (col.col_name, col_type)
        if col.__class__ is UnicodeCol and not sqlite:
            col_def = "%s(%d)" % (col_def, col.length)
        if col.not_null and not col.__class__ is AutoIdCol:
            col_def = col_def + " NOT NULL"
        if col.__class__ is UnicodeCol:
            if col.unique:
                col_def = col_def + " UNIQUE"
            if col.collate_nocase:
                col_def = col_def + " COLLATE " + collate_nocase_name
        col_defs.append(col_def)
    cols_sql = ",\n    ".join(col_defs)
    sql = "CREATE TABLE %s (\n    %s\n)" \
        % (table.table_name, cols_sql)
    return sql


def drop_table(table, if_exists):
    sql = "DROP TABLE IF EXISTS %s" if if_exists else "DROP TABLE %s"
    sql = sql % table.table_name
    return sql



def insert(table, row, sqlite, paramstr):
    values = []
    col_names = []
    value_qs = []
    auto_id_col = table.auto_id_col
    auto_id_used = False
    for col in table.cols:
        col_name = col.col_name
        value = row[col_name]
        col.check_value(value)
        if value is None:
            if col is auto_id_col:
                auto_id_used = True
            else:
                col_names.append(col_name)
                value_qs.append('NULL')
        else:
            col_names.append(col_name)
            value_qs.append(paramstr)
            value = col.py_to_db(value)
            values.append(value)
    col_names_sql = ",".join(col_names)
    values_sql = ",".join(value_qs)
    sql = "INSERT INTO %s (%s) VALUES (%s)" \
        % (table.table_name, col_names_sql, values_sql)
    if auto_id_used and not sqlite:
        sql = sql + " RETURNING " + auto_id_col.col_name
    return sql, values, auto_id_used


def update(table, row, where, sqlite, paramstr):
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
            col_names.append(col_name + "=" + paramstr)
            value = col.py_to_db(value)
            values.append(value)
    col_sql = ",".join(col_names)
    sql_parts = ["UPDATE", table.table_name, "SET", col_sql]
    if not where is None:
        cond_sql, where_values = _sqlcond_to_sql(where, sqlite, paramstr)
        sql_parts.append("WHERE")
        sql_parts.append(cond_sql)
        values.extend(where_values)
    sql = " ".join(sql_parts)
    return sql, values


def update_by_id(table, row, paramstr):
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
    col_sqls = [(col_name + "=" + paramstr) for col_name in col_names]
    col_sql = ",".join(col_sqls)
    sql = "UPDATE %s SET %s" % (table.table_name, col_sql)
    sql = "%s WHERE %s=%s" % (sql, auto_id_col.col_name, paramstr)
    return sql, values


def delete(table, where, sqlite, paramstr):
    cond_sql, values = _sqlcond_to_sql(where, sqlite, paramstr)
    sql = "DELETE FROM %s WHERE %s" % (table.table_name, cond_sql)
    return sql, values


def delete_by_id(table, row_id, paramstr):
    assert table.auto_id_col, \
        "delete_by_id(): table '%s' does not have AutoIdCol" \
            % table.table_name
    assert not row_id is None, "delete_by_id(): cannot use None for AutoIdCol"
    auto_id_col = table.auto_id_col
    auto_id_col.check_value(row_id)
    sql = "DELETE FROM %s WHERE %s=%s" \
        % (table.table_name, auto_id_col.col_name, paramstr)
    values = [row_id]
    return sql, values


def select(table, where, order_by, sqlite, paramstr):
    col_names = [col.col_name for col in table.cols]
    col_names_sql = ",".join(col_names)
    sql_parts = ["SELECT", col_names_sql, "FROM", table.table_name]
    if where:
        sql_parts.append("WHERE")
        cond_sql, values = _sqlcond_to_sql(where, sqlite, paramstr)
        sql_parts.append(cond_sql)
    else:
        values = []
    if order_by:
        sql_parts.append("ORDER BY")
        sql_parts.append(_sqlsort_to_sql(order_by))
    sql = " ".join(sql_parts)
    return sql, values


def select_distinct(table, qcol, where, order_by, sqlite, paramstr):
    assert isinstance(qcol, QueryCol), "Column must be instance of QueryCol"
    col_name = qcol._col.col_name
    sql_parts = ["SELECT DISTINCT", col_name, "FROM", table.table_name]
    if where:
        sql_parts.append("WHERE")
        cond_sql, values = _sqlcond_to_sql(where, sqlite, paramstr)
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


def _op_eq(sqlcond, sqlite, paramstr):
    if sqlcond.other == None:
        return "%s is NULL", False, None
    else:
        cond_sql = "%s=" + paramstr
        value = sqlcond.col.py_to_db(sqlcond.other)
        return cond_sql, True, value

def _op_gtgteltlte(sqlcond, sqlite, paramstr):
    assert not isinstance(sqlcond.col, BoolCol), \
        "Op '%s' does not support BoolCol" % sqlcond.op
    assert not isinstance(sqlcond.col, DateTimeUTCCol), \
        "Op '%s' does not support DateTimeUTCCol" % sqlcond.op
    assert not sqlcond.other is None, \
        "Op '%s' does not support None" % sqlcond.op
    cond_sql = "%s" + sqlcond.op + paramstr
    value = sqlcond.col.py_to_db(sqlcond.other)
    return cond_sql, True, value

def _op_YEAR(sqlcond, sqlite, paramstr):
    assert isinstance(sqlcond.col, DateCol), \
        "YEAR condition can only be used for DateCol"
    assert sqlcond.other != None, \
        "YEAR condition cannot use None"
    if sqlite:
        cond_sql = "%s LIKE " + paramstr
        value = "%d-%%" % sqlcond.other.year
    else:
        cond_sql = "EXTRACT(YEAR FROM %s)=" + paramstr
        value = sqlcond.other.year
    return cond_sql, True, value

def _op_MONTH(sqlcond, sqlite, paramstr):
    assert isinstance(sqlcond.col, DateCol), \
        "MONTH condition can only be used for DateCol"
    assert sqlcond.other != None, \
        "MONTH condition cannot use None"
    if sqlite:
        cond_sql = "%s LIKE " + paramstr
        value = "%%-%02d-%%" % sqlcond.other.month
    else:
        cond_sql = "EXTRACT(MONTH FROM %s)=" + paramstr
        value = sqlcond.other.month
    return cond_sql, True, value

def _op_DAY(sqlcond, sqlite, paramstr):
    assert isinstance(sqlcond.col, DateCol), \
        "DAY condition can only be used for DateCol"
    assert sqlcond.other != None, \
        "DAY condition cannot use None"
    if sqlite:
        cond_sql = "%s LIKE " + paramstr
        value = "%%-%02d" % sqlcond.other.day
    else:
        cond_sql = "EXTRACT(DAY FROM %s)=" + paramstr
        value = sqlcond.other.day
    return cond_sql, True, value

def _op_LIKE(sqlcond, sqlite, paramstr):
    assert isinstance(sqlcond.col, UnicodeCol), \
        "LIKE condition can only be used for UnicodeCol"
    assert sqlcond.other != None, \
        "LIKE condition cannot use None"
    if sqlite:
        raise NotImplementedError
    else:
        cond_sql = "%s LIKE " + paramstr
    value = sqlcond.other
    return cond_sql, True, value

def _op_ILIKE(sqlcond, sqlite, paramstr):
    assert isinstance(sqlcond.col, UnicodeCol), \
        "LIKE condition can only be used for UnicodeCol"
    assert sqlcond.other != None, \
        "LIKE condition cannot use None"
    if sqlite:
        cond_sql = "%s LIKE " + paramstr
    else:
        cond_sql = "%s ILIKE " + paramstr
    value = sqlcond.other
    return cond_sql, True, value


_OP_MAP = {
    "=": _op_eq,
    ">": _op_gtgteltlte,
    ">=": _op_gtgteltlte,
    "<": _op_gtgteltlte,
    "<=": _op_gtgteltlte,
    "YEAR": _op_YEAR,
    "MONTH": _op_MONTH,
    "DAY": _op_DAY,
    "LIKE": _op_LIKE,
    "ILIKE": _op_ILIKE,
    }


def _sqlcond_to_sql(where, sqlite, paramstr):
    if paramstr == "%s":
        paramstr = "%%s"
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
        op_fn = _OP_MAP.get(sqlcond.op)
        if not op_fn:
            raise AssertionError, "Unsupported op '%s'" % sqlcond.op
        cond_sql, have_value, value = op_fn(sqlcond, sqlite, paramstr)
        cond_sql = cond_sql % sqlcond.col.col_name
        cond_sqls.append(cond_sql)
        if have_value:
            values.append(value)
    sql = combiner.join(cond_sqls)
    return sql, values

def _sqlsort_to_sql(order_by):
    assert isinstance(order_by, SqlSort), "'order_by' must be SqlSort"
    sql = order_by.col.col_name
    if order_by.asc:
        sql = sql + " ASC"
    else:
        sql = sql + " DESC"
    return sql


