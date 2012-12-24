
import unittest

from datetime import date
from binder.col import *
from binder.table import Table, SqlCondition, SqlSort, AND, OR

from bindertest.tabledefs import Foo, Bar


class TableTest(unittest.TestCase):

    def test_init_2_AutoIdCols(self):
        # Table can have only 1 AutoIdCol
        try:
            Table("xyz", AutoIdCol("id1"), IntCol("x"), AutoIdCol("id2"))
        except AssertionError, e:
            self.assertEquals("Table 'xyz' has more than one AutoIdCol", str(e))
        else:
            self.fail()

    def test_init_duplicate_col_name(self):
        try:
            Table("xyz", AutoIdCol("id1"), IntCol("x"), StringCol("x", 20))
        except AssertionError, e:
            self.assertEquals("Table 'xyz' has more than one column with name 'x'", str(e))
        else:
            self.fail()

    def test_cols(self):
        expected = ["foo_id", "i1", "s1", "d1"]
        actual = [col.col_name for col in Foo.cols]
        self.assertEquals(expected, actual)
        expected = ["bi", "bs", "bd", "bdt1", "bb"]
        actual = [col.col_name for col in Bar.cols]
        self.assertEquals(expected, actual)

    def test_auto_id_col(self):
        # AutoIdCol field identified by __init__
        self.assert_(Foo.auto_id_col is Foo.cols[0])
        self.assert_(Bar.auto_id_col is None)

    def test_new_parse_defaults(self):
        expected = {
            "foo_id": None,
            "i1": 0,
            "s1": "",
            "d1": None,
        }
        actual = Foo.new()
        self.assertEquals(expected, actual)
        actual = Foo.parse()
        self.assertEquals(expected, actual)
        expected = {
            "bi": None,
            "bs": "",
            "bd": None,
            "bdt1": None,
            "bb": False,
        }
        actual = Bar.new()
        self.assertEquals(expected, actual)
        actual = Bar.parse()
        self.assertEquals(expected, actual)

    def test_parse_auto_id(self):
        expected = {
            "foo_id": None,
            "i1": 0,
            "s1": "",
            "d1": None,
        }
        actual = Foo.parse(foo_id=None)
        self.assertEquals(expected, actual)

    def test_new_parse_all(self):
        expected = {
            "foo_id": 42,
            "i1": 101,
            "s1": "alpha",
            "d1": date(2006,6,6),
        }
        actual = Foo.new(foo_id=42, i1=101, s1="alpha", d1=date(2006,6,6))
        self.assertEquals(expected, actual)
        actual = Foo.parse(foo_id="42", i1="101", s1="alpha", d1="2006-06-06")
        self.assertEquals(expected, actual)
        # parse some fields str
        actual = Foo.parse(foo_id="42", i1=101, s1="alpha", d1=date(2006,6,6))
        self.assertEquals(expected, actual)

    def test_new_parse_some_fields(self):
        expected = {
            "foo_id": 42,
            "i1": 0,
            "s1": "alpha",
            "d1": None,
        }
        actual = Foo.new(foo_id=42, s1="alpha")
        self.assertEquals(expected, actual)
        actual = Foo.parse(foo_id="42", s1="alpha")
        self.assertEquals(expected, actual)

    def test_new_parse_clone(self):
        # new() and parse() should return a new dictionary
        expected = {
            "foo_id": 42,
            "i1": 0,
            "s1": "alpha",
            "d1": None,
        }
        actual = Foo.new(**expected)
        self.assertEquals(expected, actual)
        self.assertFalse(actual is expected)
        actual = Foo.parse(**expected)
        self.assertEquals(expected, actual)
        self.assertFalse(actual is expected)

    def test_new_parse_unkown_cols(self):
        # DONT copy unknown columns
        expected = {
            "foo_id": None,
            "i1": 16,
            "s1": "",
            "d1": None,
        }
        actual = Foo.new(i1=16, s2="beta")
        self.assertEquals(expected, actual)
        actual = Foo.parse(i1="16", s2="beta")
        self.assertEquals(expected, actual)

    def test_parse_empty_string(self):
        # parse() replaces empty strings with default value
        expected = {
            "foo_id": None,
            "i1": 0,
            "s1": "",
            "d1": None,
        }
        actual = Foo.parse(foo_id="", i1="", s1="", d1="")
        self.assertEquals(expected, actual)
        expected = {
            "bi": None,
            "bs": "",
            "bd": None,
            "bdt1": None,
            "bb": False,
        }
        actual = Bar.parse(bi="", bs="", bd="", bdt1="", bb="")
        self.assertEquals(expected, actual)

    def test_new_bad_values(self):
        # new() does not allow bad values
        try:
            Foo.new(i1="bar", s2=1.1)
        except TypeError, e:
            self.assertEquals("IntCol 'i1': int expected, got str", str(e))
        else:
            self.fail()

    def test_parse_bad_values(self):
        # parse() does not allow non-string bad values
        try:
            Foo.parse(i1=2.3, s2=1.1)
        except TypeError, e:
            self.assertEquals("IntCol 'i1': int expected, got float", str(e))
        else:
            self.fail()

    def test_parse_error(self):
        # parse() gives parse error for bad strings
        try:
            Foo.parse(i1="2.3", s2=1.1)
        except ValueError, e:
            self.assert_(
                    str(e) in [
                            "invalid literal for int(): 2.3",
                            "invalid literal for int() with base 10: '2.3'",
                        ]
                )
        else:
            self.fail()

    def test_check_values(self):
        # defaults / None
        foo = Foo.new()
        auto_id = Foo.check_values(foo)
        self.assert_(auto_id)
        # given values / no None
        foo = {
            "foo_id": 42,
            "i1": 101,
            "s1": "alpha",
            "d1": date(2006,6,6),
        }
        auto_id = Foo.check_values(foo)
        self.assertFalse(auto_id)
        # bad value
        foo = Foo.new()
        foo["i1"] = "bar"
        try:
            Foo.check_values(foo)
        except TypeError, e:
            self.assertEquals("IntCol 'i1': int expected, got str", str(e))
        else:
            self.fail()
        # bad value
        foo = Foo.new()
        foo["s1"] = 1.1
        try:
            Foo.check_values(foo)
        except TypeError, e:
            self.assertEquals("StringCol 's1': unicode expected, got float", str(e))
        else:
            self.fail()
        # unknown columns ignored
        foo = Foo.new(s2=None)
        foo["s3"] = 1.2
        auto_id = Foo.check_values(foo)
        self.assert_(True, auto_id)

    def test_q(self):
        q = Foo.q
        # existing columns
        q_foo_id = Foo.q.foo_id
        q_i1 = Foo.q.i1
        # non-existing column
        try:
            Foo.q.i2
        except AttributeError, e:
            self.assertEquals("QueryCols instance has no attribute 'i2'", str(e))
        else:
            self.fail()

    def test_q_ops(self):
        qexpr = Foo.q.foo_id == 1
        self.assert_(isinstance(qexpr, SqlCondition))
        qexpr = Foo.q.d1 == None
        self.assert_(isinstance(qexpr, SqlCondition))
        qexpr = Foo.q.d1 > date(2007, 5, 22)
        self.assert_(isinstance(qexpr, SqlCondition))
        qexpr = Foo.q.d1 >= date(2007, 5, 22)
        self.assert_(isinstance(qexpr, SqlCondition))
        qexpr = Foo.q.d1 < date(2007, 5, 22)
        self.assert_(isinstance(qexpr, SqlCondition))
        qexpr = Foo.q.d1 <= date(2007, 5, 22)
        self.assert_(isinstance(qexpr, SqlCondition))

    def test_q_ops_check_value(self):
        try:
            Foo.q.foo_id == "xyz"
        except TypeError, e:
            self.assertEquals("AutoIdCol 'foo_id': int expected, got str", str(e))
        else:
            self.fail()
        try:
            Foo.q.s1 > 23
        except TypeError, e:
            self.assertEquals("StringCol 's1': unicode expected, got int", str(e))
        else:
            self.fail()

    def test_q_ops_auto_id(self):
        try:
            Foo.q.foo_id == None
        except AssertionError, e:
            self.assertEquals("SqlCondition: cannot use None for AutoIdCol", str(e))
        else:
            self.fail()

    def test_AND(self):
        qexpr1 = Foo.q.foo_id == 1
        qexpr2 = Foo.q.s1 == 'x'
        qexpr3 = Foo.q.d1 == None
        AND(qexpr1, qexpr2)
        AND(qexpr1, qexpr2, qexpr3)
        try:
            AND(qexpr1, "xyz")
        except AssertionError, e:
            self.assertEquals("AND: conditions must be SqlCondition", str(e))
        else:
            self.fail()
        try:
            AND(qexpr1)
        except AssertionError, e:
            self.assertEquals("AND: must have at least 2 conditions", str(e))
        else:
            self.fail()

    def test_OR(self):
        qexpr1 = Foo.q.foo_id == 1
        qexpr2 = Foo.q.s1 == 'x'
        qexpr3 = Foo.q.d1 == None
        OR(qexpr1, qexpr2)
        OR(qexpr1, qexpr2, qexpr3)
        try:
            OR(qexpr1, "xyz")
        except AssertionError, e:
            self.assertEquals("OR: conditions must be SqlCondition", str(e))
        else:
            self.fail()
        try:
            OR(qexpr1)
        except AssertionError, e:
            self.assertEquals("OR: must have at least 2 conditions", str(e))
        else:
            self.fail()

    def test_q_sort(self):
        qexpr = Foo.q.foo_id.ASC
        self.assert_(isinstance(qexpr, SqlSort))
        qexpr = Foo.q.d1.DESC
        self.assert_(isinstance(qexpr, SqlSort))




if __name__ == '__main__':
    unittest.main()
