
from binder.col import *
from binder.table import SqlCondition, SqlSort, AND, OR, QueryCol


DIALECT_SQLITE = "sqlite"
DIALECT_MYSQL = "mysql"

_COL_TYPE_SQLITE = {
    AutoIdCol: "INTEGER PRIMARY KEY",
    IntCol: "INTEGER",
    FloatCol: "REAL",
    BoolCol: "INTEGER",
    UnicodeCol: "TEXT",
    DateCol: "TEXT",
    DateTimeUTCCol: "TEXT",
}

_COL_TYPE_MYSQL = {
    AutoIdCol: "INT AUTO_INCREMENT PRIMARY KEY",
    IntCol: "INT",
    FloatCol: "DOUBLE PRECISION",
    BoolCol: "BOOL",
    UnicodeCol: "VARCHAR",
    DateCol: "DATE",
    DateTimeUTCCol: "DATETIME",
}

def create_table(dialect, table):
    if dialect == DIALECT_SQLITE:
        col_types = _COL_TYPE_SQLITE
        collate_nocase_name = "NOCASE"
    elif dialect == DIALECT_MYSQL:
        col_types = _COL_TYPE_MYSQL
        collate_nocase_name = "utf8_general_ci"
    else:
        assert False, "Unknown dialect: %s" % dialect
    col_defs = []
    for col in table.cols:
        col_type = col_types[col.__class__]
        col_def = "%s %s" % (col.col_name, col_type)
        if col.__class__ is UnicodeCol and dialect != DIALECT_SQLITE:
            col_def = "%s(%d)" % (col_def, col.length)
            col_def = col_def + " CHARACTER SET utf8"
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
    if dialect == DIALECT_MYSQL:
        sql = sql + " ENGINE=INNODB"
    return sql


def drop_table(table, if_exists):
    sql = "DROP TABLE IF EXISTS %s" if if_exists else "DROP TABLE %s"
    sql = sql % table.table_name
    return sql



def insert(table, row, paramstr):
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
            value_qs.append(paramstr)
            value = col.py_to_db(value)
            values.append(value)
    col_names_sql = ",".join(col_names)
    values_sql = ",".join(value_qs)
    sql = "INSERT INTO %s (%s) VALUES (%s)" \
        % (table.table_name, col_names_sql, values_sql)
    return sql, values, auto_id_used


def update(table, row, where, dialect, paramstr):
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
        cond_sql, where_values = _sqlcond_to_sql(where, dialect, paramstr)
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


def delete(table, where, dialect, paramstr):
    cond_sql, values = _sqlcond_to_sql(where, dialect, paramstr)
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


def select(table, where, order_by, dialect, paramstr):
    col_names = [col.col_name for col in table.cols]
    col_names_sql = ",".join(col_names)
    sql_parts = ["SELECT", col_names_sql, "FROM", table.table_name]
    if where:
        sql_parts.append("WHERE")
        cond_sql, values = _sqlcond_to_sql(where, dialect, paramstr)
        sql_parts.append(cond_sql)
    else:
        values = []
    if order_by:
        sql_parts.append("ORDER BY")
        sql_parts.append(_sqlsort_to_sql(order_by))
    sql = " ".join(sql_parts)
    return sql, values


def select_distinct(table, qcol, where, order_by, dialect, paramstr):
    assert isinstance(qcol, QueryCol), "Column must be instance of QueryCol"
    col_name = qcol._col.col_name
    sql_parts = ["SELECT DISTINCT", col_name, "FROM", table.table_name]
    if where:
        sql_parts.append("WHERE")
        cond_sql, values = _sqlcond_to_sql(where, dialect, paramstr)
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


def _op_eq(sqlcond, dialect, paramstr):
    if sqlcond.other == None:
        return "%s is NULL", False, None
    else:
        cond_sql = "%s=" + paramstr
        value = sqlcond.col.py_to_db(sqlcond.other)
        return cond_sql, True, value

def _op_gtgteltlte(sqlcond, dialect, paramstr):
    assert not isinstance(sqlcond.col, BoolCol), \
        "Op '%s' does not support BoolCol" % sqlcond.op
    assert not isinstance(sqlcond.col, DateTimeUTCCol), \
        "Op '%s' does not support DateTimeUTCCol" % sqlcond.op
    assert not sqlcond.other is None, \
        "Op '%s' does not support None" % sqlcond.op
    cond_sql = "%s" + sqlcond.op + paramstr
    value = sqlcond.col.py_to_db(sqlcond.other)
    return cond_sql, True, value

def _op_YEAR(sqlcond, dialect, paramstr):
    assert isinstance(sqlcond.col, DateCol), \
        "YEAR condition can only be used for DateCol"
    assert sqlcond.other != None, \
        "YEAR condition cannot use None"
    if dialect == DIALECT_SQLITE:
        cond_sql = "%s LIKE " + paramstr
        value = "%d-%%" % sqlcond.other.year
    elif dialect == DIALECT_MYSQL:
        cond_sql = "YEAR(%s)=" + paramstr
        value = sqlcond.other.year
    else:
        raise Exception, ("Unknown dialect", dialect)
    return cond_sql, True, value

def _op_YEAR_MONTH(sqlcond, dialect, paramstr):
    assert isinstance(sqlcond.col, DateCol), \
        "YEAR_MONTH condition can only be used for DateCol"
    assert sqlcond.other != None, \
        "YEAR_MONTH condition cannot use None"
    if dialect == DIALECT_SQLITE:
        cond_sql = "%s LIKE " + paramstr
        value = "%d-%02d-%%" % (sqlcond.other.year, sqlcond.other.month)
    elif dialect == DIALECT_MYSQL:
        cond_sql = "EXTRACT(YEAR_MONTH FROM %s)=" + paramstr
        value = "%d%02d" % (sqlcond.other.year, sqlcond.other.month)
    else:
        raise Exception, ("Unknown dialect", dialect)
    return cond_sql, True, value

def _op_MONTH(sqlcond, dialect, paramstr):
    assert isinstance(sqlcond.col, DateCol), \
        "MONTH condition can only be used for DateCol"
    assert sqlcond.other != None, \
        "MONTH condition cannot use None"
    if dialect == DIALECT_SQLITE:
        cond_sql = "%s LIKE " + paramstr
        value = "%%-%02d-%%" % sqlcond.other.month
    elif dialect == DIALECT_MYSQL:
        cond_sql = "EXTRACT(MONTH FROM %s)=" + paramstr
        value = sqlcond.other.month
    else:
        raise Exception, ("Unknown dialect", dialect)
    return cond_sql, True, value

def _op_DAY(sqlcond, dialect, paramstr):
    assert isinstance(sqlcond.col, DateCol), \
        "DAY condition can only be used for DateCol"
    assert sqlcond.other != None, \
        "DAY condition cannot use None"
    if dialect == DIALECT_SQLITE:
        cond_sql = "%s LIKE " + paramstr
        value = "%%-%02d" % sqlcond.other.day
    elif dialect == DIALECT_MYSQL:
        cond_sql = "EXTRACT(DAY FROM %s)=" + paramstr
        value = sqlcond.other.day
    else:
        raise Exception, ("Unknown dialect", dialect)
    return cond_sql, True, value

def _op_LIKE(sqlcond, dialect, paramstr):
    assert isinstance(sqlcond.col, UnicodeCol), \
        "LIKE condition can only be used for UnicodeCol"
    assert sqlcond.other != None, \
        "LIKE condition cannot use None"
    cond_sql = "%s LIKE " + paramstr
    value = sqlcond.other
    return cond_sql, True, value

_OP_MAP = {
    "=": _op_eq,
    ">": _op_gtgteltlte,
    ">=": _op_gtgteltlte,
    "<": _op_gtgteltlte,
    "<=": _op_gtgteltlte,
    "YEAR": _op_YEAR,
    "YEAR_MONTH": _op_YEAR_MONTH,
    "MONTH": _op_MONTH,
    "DAY": _op_DAY,
    "LIKE": _op_LIKE,
    }


def _sqlcond_to_sql(where, dialect, paramstr):
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
        cond_sql, have_value, value = op_fn(sqlcond, dialect, paramstr)
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


