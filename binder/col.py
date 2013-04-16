"Classes representing SQL column types."

from datetime import date, datetime

__all__ = [
    "AutoIdCol",
    "IntCol",
    "FloatCol",
    "BoolCol",
    "UnicodeCol",
    "DateCol",
    "DateTimeUTCCol",
    ]


def parse_isodatetime(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")


class ColBase:
    "Column type base."

    def __init__(self, col_name, not_null, pytype, default_value):
        assert type(col_name) is str, "col_name must be str"
        assert type(not_null) is bool, "not_null must be bool"
        assert type(pytype) is type, "pytype must be type"
        self.col_name = col_name
        self.not_null = not_null
        self.pytype = pytype
        self.default_value = default_value

    def py_to_db(self, value):
        return value

    def db_to_py(self, dbvalue):
        return dbvalue

    def parse_str(self, value):
        raise NotImplementedError

    def to_str(self, value):
        if value is None:
            return ""
        else:
            return str(value)

    def to_unicode_or_str(self, value):
        return self.to_str(value)

    def check_value(self, value):
        if value is None:
            # NULL/None value
            if self.not_null:
                raise TypeError, \
                    "Column '%s' is NOT NULL, got None" % self.col_name
        else:
            if not type(value) is self.pytype:
                raise TypeError, "%s '%s': %s expected, got %s" \
                    % (self.__class__.__name__, self.col_name, \
                    self.pytype.__name__, type(value).__name__)

    def __repr__(self):
        return '<%s:col_name="%s",not_null=%s>' \
            % (self.__class__.__name__, self.col_name, self.not_null)


class AutoIdCol(ColBase):
    "Automatic integer primary key."

    def __init__(self, col_name):
        ColBase.__init__(self, col_name, True, int, None)

    def parse_str(self, value):
        if not value is None:
            value = int(value)
        return value

    def check_value(self, value):
        if value is None:
            return
        if type(value) is long:
            return
        ColBase.check_value(self, value)


class IntCol(ColBase):
    "Integer column."

    def __init__(self, col_name, not_null=True):
        if not_null:
            default_value = 0
        else:
            default_value = None
        ColBase.__init__(self, col_name, not_null, int, default_value)

    def parse_str(self, value):
        if value == "":
            if self.not_null:
                raise ValueError, \
                    "Column '%s' is NOT NULL, got blank string" % self.col_name
            return None
        return int(value)

    def check_value(self, value):
        if not type(value) is long:
            ColBase.check_value(self, value)


class FloatCol(ColBase):
    "Floating point column."

    def __init__(self, col_name, not_null=True):
        if not_null:
            default_value = 0
        else:
            default_value = None
        ColBase.__init__(self, col_name, not_null, float, default_value)

    def parse_str(self, value):
        if value == "":
            if self.not_null:
                raise ValueError, \
                    "Column '%s' is NOT NULL, got blank string" % self.col_name
            return None
        return float(value)

    def check_value(self, value):
        if not type(value) is int:
            ColBase.check_value(self, value)


class BoolCol(ColBase):
    "Boolean column."

    def __init__(self, col_name, not_null=True):
        if not_null:
            default_value = False
        else:
            default_value = None
        ColBase.__init__(self, col_name, not_null, bool, default_value)

    def parse_str(self, value):
        if value == "":
            if self.not_null:
                raise ValueError, \
                    "Column '%s' is NOT NULL, got blank string" % self.col_name
            return None
        return bool(int(value))

    def to_str(self, value):
        if value is None:
            return ""
        else:
            return str(int(value))


# TODO: what is default and round trip of null/blank?
class UnicodeCol(ColBase):
    "Unicode string column."

    def __init__(self, col_name, length, unique=False, collate_nocase=False):
        assert type(length) is int, "length must be int"
        assert length > 0, "length must be > 0"
        ColBase.__init__(self, col_name, True, unicode, "")
        self.length = length
        self.unique = unique
        self.collate_nocase = collate_nocase

    def check_value(self, value):
        if not type(value) is str:
            ColBase.check_value(self, value)
        if len(value) > self.length:
            raise ValueError, "%s '%s': string too long" \
                % (self.__class__.__name__, self.col_name)

    def py_to_db(self, value):
        return unicode(value)

    def parse_str(self, value):
        return value

    def to_unicode_or_str(self, value):
        return value



class DateCol(ColBase):
    "Date column."

    def __init__(self, col_name):
        ColBase.__init__(self, col_name, False, date, None)
    
    def py_to_db(self, value):
        if value is None:
            return None
        else:
            # store in ISO format
            return value.isoformat()

    def db_to_py(self, dbvalue):
        if dbvalue is None:
            return None
        elif isinstance(dbvalue, date):
            return dbvalue
        else:
            # sqlite: parse ISO date string
            assert isinstance(dbvalue, basestring)
            yyyy, mm, dd = dbvalue.split("-")
            return date(int(yyyy), int(mm), int(dd))

    def parse_str(self, value):
        if value == "":
            value = None
        return self.db_to_py(value)



class DateTimeUTCCol(ColBase):
    "DateTime UTC column."

    def __init__(self, col_name):
        ColBase.__init__(self, col_name, False, datetime, None)
    
    def py_to_db(self, value):
        if value is None:
            return None
        else:
            # store in ISO format
            assert value.tzinfo is None
            return value.isoformat()[:19] + 'Z'

    def db_to_py(self, dbvalue):
        if dbvalue is None:
            return None
        else:
            # parse ISO datetime string
            assert type(dbvalue) in [str, unicode]
            return parse_isodatetime(dbvalue)

    def parse_str(self, value):
        if value == "":
            value = None
        return self.db_to_py(value)

    def to_str(self, value):
        if value is None:
            return ""
        return self.py_to_db(value)
