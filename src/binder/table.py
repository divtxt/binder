
from binder.col import AutoIdCol

class Table:

    def __init__(self, table_name, *cols):
        self.table_name = table_name
        self.cols = cols
        col_map = {}
        auto_id_col = None
        for col in cols:
            col_name = col.col_name
            assert not col_map.has_key(col_name), \
                "Table '%s' has more than one column with name '%s'" \
                    % (table_name, col_name)
            col_map[col_name] = col
            if col.__class__ is AutoIdCol:
                assert auto_id_col is None, \
                    "Table '%s' has more than one AutoIdCol" % table_name
                auto_id_col = col
        self.auto_id_col = auto_id_col
        self.q = QueryCols(table_name, cols)

    def new(self, **col_values):
        row = {}
        for col in self.cols:
            col_name = col.col_name
            value = col_values.get(col_name, col.default_value)
            col.check_value(value)
            row[col_name] = value
        return row

    def parse(self, **col_values):
        row = {} #self.new(**col_values)
        for col in self.cols:
            col_name = col.col_name
            value = col_values.get(col_name, col.default_value)
            if type(value) in [str, unicode]:
                if value == "":
                    value = col.default_value
                else:
                    value = col.parse_str(value)
            col.check_value(value)
            row[col_name] = value
        return row

    def check_values(self, row):
        auto_id_used = False
        for col in self.cols:
            value = row[col.col_name]
            col.check_value(value)
            if value is None:
                auto_id_used = True
        return auto_id_used



class QueryCols:

    def __init__(self, table_name, cols):
        for col in cols:
            qcol = QueryCol(col)
            setattr(self, col.col_name, qcol)


class QueryCol:

    def __init__(self, col):
        self._col = col
        self._auto_id_col = col.__class__ is AutoIdCol
        self.ASC = SqlSort(col, True)
        self.DESC = SqlSort(col, False)

    def __eq__(self, other):
        return SqlCondition(self._col, "=", other)

    def __gt__(self, other):
        return SqlCondition(self._col, ">", other)

    def __ge__(self, other):
        return SqlCondition(self._col, ">=", other)

    def __lt__(self, other):
        return SqlCondition(self._col, "<", other)

    def __le__(self, other):
        return SqlCondition(self._col, "<=", other)

    def YEAR(self, date):
        return SqlCondition(self._col, "YEAR", date)

    def YEAR_MONTH(self, date):
        return SqlCondition(self._col, "YEAR_MONTH", date)

    def MONTH(self, date):
        return SqlCondition(self._col, "MONTH", date)

    def DAY(self, date):
        return SqlCondition(self._col, "DAY", date)

    def LIKE(self, s):
        return SqlCondition(self._col, "LIKE", s)


class SqlCondition:

    def __init__(self, col, op, other):
        col.check_value(other)
        if col.__class__ is AutoIdCol:
            assert other != None, \
                "SqlCondition: cannot use None for AutoIdCol"
        self.col = col
        self.op = op
        self.other = other

    def __repr__(self):
        return '"%s"' % self._repr1()

    def _repr1(self):
        return "%s %s %s" \
            % (self.col.col_name, self.op, repr(self.other))


class SqlSort:

    def __init__(self, col, asc):
        self.col = col
        self.asc = asc



class AND:

    def __init__(self, *sqlconds):
        assert len(sqlconds) > 1, "AND: must have at least 2 conditions"
        for sqlcond in sqlconds:
            assert isinstance(sqlcond, SqlCondition), \
                "AND: conditions must be SqlCondition"
        self.sqlconds = sqlconds

    def __repr__(self):
        conds = " AND ".join(c._repr1() for c in self.sqlconds)
        conds = '"%s"' % conds
        return conds


class OR:

    def __init__(self, *sqlconds):
        assert len(sqlconds) > 1, "OR: must have at least 2 conditions"
        for sqlcond in sqlconds:
            assert isinstance(sqlcond, SqlCondition), \
                "OR: conditions must be SqlCondition"
        self.sqlconds = sqlconds

    def __repr__(self):
        conds = " OR ".join(c._repr1() for c in self.sqlconds)
        conds = '"%s"' % conds
        return conds


