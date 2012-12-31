
from binder import col
from binder.col import *
import datetime
import sys
import unittest



class _XCol(col.ColBase):
    pass


class ColBaseTest(unittest.TestCase):

    def test_to(self):
        col = _XCol('a', False, str, 'x')
        x = object()
        self.assert_(col.py_to_db(x) is x)
        self.assert_(col.db_to_py(x) is x)

    def test_to_str(self):
        col = _XCol('a', False, str, 'x')
        x = object()
        self.assertEquals(col.to_str(x), str(x))

    def test_check_value_type(self):
        col1 = _XCol('alpha', False, str, None)
        col2 = _XCol('beta', True, int, None)
        # correct type
        col1.check_value('x')
        col2.check_value(1)
        # wrong type
        try:
            col1.check_value(1)
        except TypeError, e:
            self.assertEquals("_XCol 'alpha': str expected, got int", str(e))
        else:
            self.fail()
        try:
            col2.check_value('x')
        except TypeError, e:
            self.assertEquals("_XCol 'beta': int expected, got str", str(e))
        else:
            self.fail()

    def test_check_value_null(self):
        col1 = _XCol('alpha', False, str, None)
        col2 = _XCol('beta', True, int, None)
        col1.check_value(None)
        try:
            col2.check_value(None)
        except TypeError, e:
            self.assertEquals("Column 'beta' is NOT NULL, got None", str(e))
        else:
            self.fail()

    def test_repr(self):
        col = _XCol('a', False, str, 'x')
        self.assertEquals('<_XCol:col_name="a",not_null=False>', repr(col))



class AutoIdColTest(unittest.TestCase):

    def test_check_value(self):
        col = AutoIdCol('alpha')
        col.check_value(1)
        col.check_value(-42L)
        col.check_value(None)

    def test_parse_str(self):
        col = AutoIdCol('alpha')
        self.assertEquals(2, col.parse_str('2'))
        try:
            col.parse_str('')
        except ValueError, e:
            self.assert_(
                    str(e) in [
                            "invalid literal for int(): ",
                            "invalid literal for int() with base 10: ''",
                        ]
                )
        else:
            self.fail()

    def test_to_str(self):
        col = AutoIdCol('alpha')
        self.assertEquals('2', col.to_str(2))



class IntColTest(unittest.TestCase):

    def test_default(self):
        coln = IntCol('alpha')
        colnn = IntCol('beta', False)
        # default
        self.assertEquals(0, coln.default_value)
        self.assertEquals(None, colnn.default_value)

    def test_check_value_type(self):
        coln = IntCol('alpha')
        colnn = IntCol('beta', False)
        # correct type
        coln.check_value(1)
        colnn.check_value(1L)
        # wrong type
        try:
            coln.check_value(1.0)
        except TypeError, e:
            self.assertEquals("IntCol 'alpha': int expected, got float", str(e))
        else:
            self.fail()

    def test_parse_str(self):
        coln = IntCol('alpha')
        colnn = IntCol('beta', False)
        self.assertEquals(3, coln.parse_str('3'))
        self.assertEquals(4, colnn.parse_str('4'))
        try:
            coln.parse_str('')
        except ValueError, e:
            self.assertEquals("Column 'alpha' is NOT NULL, got blank string", str(e))
        else:
            self.fail()
        self.assertEquals(None, colnn.parse_str(''))
        # long
        l = sys.maxint + 1
        self.assertEquals(l, colnn.parse_str(str(l)))

    def test_to_str(self):
        coln = IntCol('alpha')
        colnn = IntCol('beta', False)
        # to_str
        self.assertEquals('12', coln.to_str(12))
        self.assertEquals('12', colnn.to_str(12))
        self.assertEquals('', colnn.to_str(None))


class BoolColTest(unittest.TestCase):

    def test_default_parse_str_to_str(self):
        coln = BoolCol('alpha')
        colnn = BoolCol('beta', False)
        # default
        self.assertEquals(False, coln.default_value)
        self.assertEquals(None, colnn.default_value)
        # parse_str
        self.assertEquals(False, coln.parse_str('0'))
        self.assertEquals(True, colnn.parse_str('1'))
        try:
            coln.parse_str('')
        except ValueError, e:
            self.assertEquals("Column 'alpha' is NOT NULL, got blank string", str(e))
        else:
            self.fail()
        self.assertEquals(None, colnn.parse_str(''))
        # to_str
        self.assertEquals('0', coln.to_str(False))
        self.assertEquals('1', colnn.to_str(True))
        self.assertEquals('', colnn.to_str(None))


class UnicodeColTest(unittest.TestCase):

    def test_check_value(self):
        col = UnicodeCol('alpha', 10)
        col.check_value('')
        col.check_value(u'')
        col.check_value('123456789X')
        col.check_value(u'123456789X')
        try:
            col.check_value('123456789X1')
        except ValueError, e:
            self.assertEquals("UnicodeCol 'alpha': string too long", str(e))
        else:
            self.fail()
        try:
            col.check_value(u'123456789X1')
        except ValueError, e:
            self.assertEquals("UnicodeCol 'alpha': string too long", str(e))
        else:
            self.fail()
        try:
            col.check_value(None)
        except TypeError, e:
            self.assertEquals("Column 'alpha' is NOT NULL, got None", str(e))
        else:
            self.fail()

    def test_non_ascii(self):
        # check_value allows non-ascii - will be caught by py_to_db()
        col = UnicodeCol('alpha', 10)
        col.check_value('f\x81nf')


class DateColTest(unittest.TestCase):

    def test_py_to_db(self):
        col = DateCol('alpha')
        self.assertEquals(None, col.py_to_db(None))
        d = datetime.date(2006,5,30)
        self.assertEquals("2006-05-30", col.py_to_db(d))

    def test_db_to_py(self):
        col = DateCol('alpha')
        self.assertEquals(None, col.db_to_py(None))
        d = datetime.date(2006,5,30)
        self.assertEquals(d, col.db_to_py("2006-05-30"))

    def test_parse_str_to_str(self):
        col = DateCol('alpha')
        # default
        self.assertEquals(None, col.default_value)
        # parse_str
        d = datetime.date(2006,5,30)
        self.assertEquals(d, col.parse_str("2006-05-30"))
        self.assertEquals(None, col.parse_str(''))
        # to_str
        self.assertEquals("2006-05-30", col.py_to_db(d))
        self.assertEquals('', col.to_str(None))


class DateTimeUTCColTest(unittest.TestCase):

    def test_py_to_db(self):
        col = DateTimeUTCCol('alpha')
        self.assertEquals(None, col.py_to_db(None))
        d = datetime.datetime(2006,5,30,17,02,13,403000)
        self.assertEquals("2006-05-30T17:02:13Z", col.py_to_db(d))

    def test_db_to_py(self):
        col = DateTimeUTCCol('alpha')
        self.assertEquals(None, col.db_to_py(None))
        d = datetime.datetime(2006,5,30,17,02,13)
        self.assertEquals(d, col.db_to_py("2006-05-30T17:02:13Z"))

    def test_parse_str_to_str(self):
        col = DateTimeUTCCol('alpha')
        # default
        self.assertEquals(None, col.default_value)
        # parse_str
        d = datetime.datetime(2006,5,30,17,02,13)
        self.assertEquals(d, col.parse_str("2006-05-30T17:02:13Z"))
        self.assertEquals(None, col.parse_str(''))
        # to_str
        self.assertEquals("2006-05-30T17:02:13Z", col.to_str(d))
        self.assertEquals('', col.to_str(None))


if __name__ == '__main__':
    unittest.main()
