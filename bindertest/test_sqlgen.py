
import unittest

from binder.col import *
from binder.table import Table, AND, OR
from binder import sqlgen
import datetime

from bindertest.tabledefs import Foo, Bar, Baz


class CreateTableTest(unittest.TestCase):

    def test(self):
        sql = sqlgen.create_table(True, Foo)
        self.assertEquals(
            """CREATE TABLE foo (
    foo_id INTEGER PRIMARY KEY,
    i1 INTEGER NOT NULL,
    s1 TEXT NOT NULL COLLATE NOCASE,
    d1 TEXT
)""",
            sql
            )
        sql = sqlgen.create_table(False, Foo)
        self.assertEquals(
            """CREATE TABLE foo (
    foo_id SERIAL UNIQUE,
    i1 BIGINT NOT NULL,
    s1 VARCHAR(10) NOT NULL COLLATE "C",
    d1 DATE
)""",
            sql
            )
        sql = sqlgen.create_table(True, Bar)
        self.assertEquals(
            """CREATE TABLE bar (
    bi INTEGER,
    bs TEXT NOT NULL,
    bd TEXT,
    bdt1 TEXT,
    bb INTEGER NOT NULL
)""",
            sql
            )
        sql = sqlgen.create_table(False, Bar)
        self.assertEquals(
            """CREATE TABLE bar (
    bi BIGINT,
    bs VARCHAR(10) NOT NULL,
    bd DATE,
    bdt1 TIMESTAMP,
    bb BOOLEAN NOT NULL
)""",
            sql
            )
        sql = sqlgen.create_table(True, Baz)
        self.assertEquals(
            """CREATE TABLE baz (
    baz_id INTEGER PRIMARY KEY,
    f3 REAL NOT NULL,
    s3 TEXT NOT NULL UNIQUE
)""",
            sql
            )
        sql = sqlgen.create_table(False, Baz)
        self.assertEquals(
            """CREATE TABLE baz (
    baz_id SERIAL UNIQUE,
    f3 DOUBLE PRECISION NOT NULL,
    s3 VARCHAR(5) NOT NULL UNIQUE
)""",
            sql
            )


class DropTableTest(unittest.TestCase):

    def test(self):
        sql = sqlgen.drop_table(Foo, False)
        self.assertEquals("DROP TABLE foo", sql)
        sql = sqlgen.drop_table(Bar, True)
        self.assertEquals("DROP TABLE IF EXISTS bar", sql)


class InsertTest(unittest.TestCase):

    def test(self):
        foo = Foo.new(foo_id=4, i1=23, s1="pqr", d1=datetime.date(2006, 5, 4))
        sql, values, auto_id_used = sqlgen.insert(Foo, foo, True, "?")
        self.assertEquals(
            "INSERT INTO foo (foo_id,i1,s1,d1) VALUES (?,?,?,?)",
            sql
            )
        self.assertEquals([4, 23, u"pqr", "2006-05-04"], values)
        self.assertFalse(auto_id_used)

    def test_no_auto_id_col(self):
        bar = Bar.new(
                bi=5, bs="abc", bd=datetime.date(2006, 3, 21),
                bdt1=datetime.datetime(2006, 4, 13, 23, 58, 14),
                bb=True
            )
        sql, values, auto_id_used = sqlgen.insert(Bar, bar, False, "%s")
        self.assertEquals(
            "INSERT INTO bar (bi,bs,bd,bdt1,bb) VALUES (%s,%s,%s,%s,%s)",
            sql
            )
        self.assertEquals(
                [5, u"abc", "2006-03-21", "2006-04-13T23:58:14Z", 1],
                values
            )
        self.assertFalse(auto_id_used)

    def test_auto_id(self):
        foo = Foo.new(foo_id=None, i1=25, s1="xyz")
        sql, values, auto_id_used = sqlgen.insert(Foo, foo, True, "?")
        self.assertEquals(
            "INSERT INTO foo (i1,s1,d1) VALUES (?,?,NULL)",
            sql
            )
        self.assertEquals([25, "xyz"], values)
        sql, values, auto_id_used = sqlgen.insert(Foo, foo, False, "?")
        self.assertEquals(
            "INSERT INTO foo (i1,s1,d1) VALUES (?,?,NULL) RETURNING foo_id",
            sql
            )
        self.assertEquals([25, "xyz"], values)
        self.assert_(auto_id_used)

    def test_auto_id_used(self):
        foo = Foo.new(foo_id=12, i1=101, s1="xyz", d1=None)
        sql, values, auto_id_used = sqlgen.insert(Foo, foo, False, "%s")
        self.assertEquals(
            "INSERT INTO foo (foo_id,i1,s1,d1) VALUES (%s,%s,%s,NULL)",
            sql
            )
        self.assertEquals([12, 101, "xyz"], values)
        self.assertFalse(auto_id_used)

    def test_bad_values(self):
        foo = Foo.new()
        foo["i1"] = "xyz"
        try:
            sqlgen.insert(Foo, foo, True, "?")
        except TypeError, e:
            self.assertEquals("IntCol 'i1': int expected, got str", str(e))
        else:
            self.fail()


class UpdateTest(unittest.TestCase):

    def test(self):
        foo = Foo.new(foo_id=4, i1=23, s1="pqr", d1=datetime.date(2006, 5, 4))
        bar = Bar.new(
                bi=5, bs="abc", bd=datetime.date(2006, 3, 21),
                bdt1=datetime.datetime(2005, 11, 22, 0, 43, 12),
                bb=True
            )
        # no where condition
        sql, values = sqlgen.update(
            Foo, foo, None, True, "?"
            )
        self.assertEquals(
            "UPDATE foo SET foo_id=?,i1=?,s1=?,d1=?",
            sql
            )
        self.assertEquals([4, 23, u"pqr", "2006-05-04"], values)
        # AutoIdCol
        sql, values = sqlgen.update(
            Foo, foo, Foo.q.foo_id == 2, False, "%s"
            )
        self.assertEquals(
            "UPDATE foo SET foo_id=%s,i1=%s,s1=%s,d1=%s WHERE foo_id=%s",
            sql
            )
        self.assertEquals([4, 23, u"pqr", "2006-05-04", 2], values)
        # IntCol
        sql, values = sqlgen.update(
            Foo, foo, Foo.q.i1 == 32, True,"?"
            )
        self.assertEquals(
            "UPDATE foo SET foo_id=?,i1=?,s1=?,d1=? WHERE i1=?",
            sql
            )
        self.assertEquals([4, 23, u"pqr", "2006-05-04", 32], values)
        # IntCol AND UnicodeCol
        sql, values = sqlgen.update(
            Foo, foo, AND(Foo.q.i1 == 12, Foo.q.s1 == "aeiou"),
            False, "%s"
            )
        self.assertEquals(
            "UPDATE foo SET foo_id=%s,i1=%s,s1=%s,d1=%s WHERE i1=%s AND s1=%s",
            sql
            )
        self.assertEquals([4, 23, u"pqr", "2006-05-04", 12, "aeiou"], values)
        # IntCol AND DateCol / NULL
        sql, values = sqlgen.update(
                Bar, bar, AND(Bar.q.bi == 12, Bar.q.bd == None),
                True, "?"
                )
        self.assertEquals(
            "UPDATE bar SET bi=?,bs=?,bd=?,bdt1=?,bb=? WHERE bi=? AND bd is NULL",
            sql
            )
        self.assertEquals([5, u"abc", "2006-03-21", "2005-11-22T00:43:12Z", 1, 12], values)
        # IntCol OR UnicodeCol
        sql, values = sqlgen.update(
            Foo, foo, OR(Foo.q.i1 == 12, Foo.q.s1 == "aeiou"),
            False, "%s"
            )
        self.assertEquals(
            "UPDATE foo SET foo_id=%s,i1=%s,s1=%s,d1=%s WHERE i1=%s OR s1=%s",
            sql
            )
        self.assertEquals([4, 23, u"pqr", "2006-05-04", 12, "aeiou"], values)
        # IntCol OR DateCol / NULL
        sql, values = sqlgen.update(
            Bar, bar, OR(Bar.q.bi == 12, Bar.q.bd == None),
            True, "?"
            )
        self.assertEquals(
            "UPDATE bar SET bi=?,bs=?,bd=?,bdt1=?,bb=? WHERE bi=? OR bd is NULL",
            sql
            )
        self.assertEquals([5, u"abc", "2006-03-21", "2005-11-22T00:43:12Z", 1, 12], values)

    def test_null(self):
        foo = Foo.new(foo_id=4, i1=23, s1="pqr", d1=None)
        sql, values = sqlgen.update(
            Foo, foo, Foo.q.i1 == 434, False, "%s"
            )
        self.assertEquals(
            "UPDATE foo SET foo_id=%s,i1=%s,s1=%s,d1=NULL WHERE i1=%s",
            sql
            )
        self.assertEquals([4, 23, u"pqr", 434], values)

    def test_auto_id(self):
        foo = Foo.new(foo_id=None, i1=25, s1="xyz")
        try:
            sqlgen.update(Foo, foo, Foo.q.i1 == 12, True, "?")
        except AssertionError, e:
            self.assertEquals("update(): cannot use None for AutoIdCol", str(e))
        else:
            self.fail()

    def test_bad_values(self):
        foo = Foo.new()
        foo["foo_id"] = 6
        foo["i1"] = "xyz"
        try:
            sqlgen.update(Foo, foo, Foo.q.i1 == 23, True, "?")
        except TypeError, e:
            self.assertEquals("IntCol 'i1': int expected, got str", str(e))
        else:
            self.fail()


class UpdateByIdTest(unittest.TestCase):

    def test(self):
        foo = Foo.new(foo_id=4, i1=23, s1="pqr", d1=datetime.date(2006, 5, 4))
        sql, values = sqlgen.update_by_id(Foo, foo, "%s")
        self.assertEquals(
            "UPDATE foo SET i1=%s,s1=%s,d1=%s WHERE foo_id=%s",
            sql
            )
        self.assertEquals([23, u"pqr", "2006-05-04", 4], values)

    def test_no_auto_id_col(self):
        bar = Bar.new(bi=5, bs="abc", bd=datetime.date(2006, 3, 21))
        try:
            sqlgen.update_by_id(Bar, bar, "?")
        except AssertionError, e:
            self.assertEquals("update_by_id(): table 'bar' does not have AutoIdCol", str(e))
        else:
            self.fail()

    def test_auto_id(self):
        foo = Foo.new(foo_id=None, i1=25, s1="xyz")
        try:
            sqlgen.update_by_id(Foo, foo, "?")
        except AssertionError, e:
            self.assertEquals("update_by_id(): cannot use None for AutoIdCol", str(e))
        else:
            self.fail()

    def test_bad_values(self):
        foo = Foo.new()
        foo["foo_id"] = 6
        foo["i1"] = "xyz"
        try:
            sqlgen.update_by_id(Foo, foo, "?")
        except TypeError, e:
            self.assertEquals("IntCol 'i1': int expected, got str", str(e))
        else:
            self.fail()



class DeleteTest(unittest.TestCase):

    def test(self):
        # AutoIdCol
        sql, values = sqlgen.delete(
            Foo, Foo.q.foo_id == 2, True, "?"
            )
        self.assertEquals("DELETE FROM foo WHERE foo_id=?", sql)
        self.assertEquals([2], values)
        # UnicodeCol
        sql, values = sqlgen.delete(
            Foo, Foo.q.i1 == 32, False, "%s"
            )
        self.assertEquals("DELETE FROM foo WHERE i1=%s", sql)
        self.assertEquals([32], values)
        # IntCol AND UnicodeCol
        sql, values = sqlgen.delete(
            Foo, AND(Foo.q.i1 == 12, Foo.q.s1 == "aeiou"),
            False, "?"
            )
        self.assertEquals("DELETE FROM foo WHERE i1=? AND s1=?", sql)
        self.assertEquals([12, "aeiou"], values)
        # IntCol AND DateCol / NULL
        sql, values = sqlgen.delete(
            Bar, AND(Bar.q.bi == 12, Bar.q.bd == None),
            False, "%s"
            )
        self.assertEquals("DELETE FROM bar WHERE bi=%s AND bd is NULL", sql)
        self.assertEquals([12], values)
        # IntCol OR UnicodeCol
        sql, values = sqlgen.delete(
            Foo, OR(Foo.q.i1 == 12, Foo.q.s1 == "aeiou"),
            True, "?"
            )
        self.assertEquals("DELETE FROM foo WHERE i1=? OR s1=?", sql)
        self.assertEquals([12, "aeiou"], values)

    def test_bad_values(self):
        try:
            sqlgen.delete(
                Foo, Foo.q.i1 == "xyz", True, "?"
                )
        except TypeError, e:
            self.assertEquals("IntCol 'i1': int expected, got str", str(e))
        else:
            self.fail()


class DeleteByIdTest(unittest.TestCase):

    def test(self):
        sql, values = sqlgen.delete_by_id(Foo, 4, "%s")
        self.assertEquals("DELETE FROM foo WHERE foo_id=%s", sql)
        self.assertEquals([4], values)

    def test_no_auto_id_col(self):
        try:
            sql = sqlgen.delete_by_id(Bar, 3, "?")
        except AssertionError, e:
            self.assertEquals("delete_by_id(): table 'bar' does not have AutoIdCol", str(e))
        else:
            self.fail()

    def test_auto_id(self):
        try:
            sqlgen.delete_by_id(Foo, None, "?")
        except AssertionError, e:
            self.assertEquals("delete_by_id(): cannot use None for AutoIdCol", str(e))
        else:
            self.fail()

    def test_bad_values(self):
        try:
            sqlgen.delete_by_id(Foo, '4', "?")
        except TypeError, e:
            self.assertEquals("AutoIdCol 'foo_id': int expected, got str", str(e))
        else:
            self.fail()



class SelectTest(unittest.TestCase):

    def test(self):
        sql, values = sqlgen.select(
            Foo, None, None, True, "?"
            )
        self.assertEquals(
            "SELECT foo_id,i1,s1,d1 FROM foo",
            sql
            )
        self.assertEquals([], values)

    def test_where_eq(self):
        sql, values = sqlgen.select(
            Foo, Foo.q.foo_id == 1, None, False, "%s"
            )
        self.assertEquals(
            "SELECT foo_id,i1,s1,d1 FROM foo WHERE foo_id=%s",
            sql
            )
        self.assertEquals([1], values)
        sql, values = sqlgen.select(
            Bar, Bar.q.bs == 'x', None, True, "?"
            )
        self.assertEquals(
            "SELECT bi,bs,bd,bdt1,bb FROM bar WHERE bs=?",
            sql
            )
        self.assertEquals(['x'], values)
        #
        sql, values = sqlgen.select(
            Bar, Bar.q.bs.LIKE('x%'), None, False, "%s"
            )
        self.assertEquals(
            "SELECT bi,bs,bd,bdt1,bb FROM bar WHERE bs LIKE %s",
            sql
            )
        self.assertEquals(['x%'], values)
        #
        sql, values = sqlgen.select(
            Foo,
            AND(Foo.q.i1 == 12, Foo.q.d1 == None, Foo.q.s1 == 'y'),
            None,
            True,
            "?"
            )
        self.assertEquals(
            "SELECT foo_id,i1,s1,d1 FROM foo WHERE i1=? AND d1 is NULL AND s1=?",
            sql
            )
        self.assertEquals([12, 'y'], values)
        sql, values = sqlgen.select(
            Foo,
            OR(Foo.q.i1 == 12, Foo.q.d1 == None, Foo.q.s1 == 'y'),
            None,
            True,
            "?"
            )
        self.assertEquals(
            "SELECT foo_id,i1,s1,d1 FROM foo WHERE i1=? OR d1 is NULL OR s1=?",
            sql
            )
        self.assertEquals([12, 'y'], values)

    def test_where_gt_ge_lt_le(self):
        sql, values = sqlgen.select(
            Foo, Foo.q.foo_id > 1, None, False, "%s"
            )
        self.assertEquals(
            "SELECT foo_id,i1,s1,d1 FROM foo WHERE foo_id>%s",
            sql
            )
        self.assertEquals([1], values)
        sql, values = sqlgen.select(
            Bar, Bar.q.bs >= 'x', None, False, "%s"
            )
        self.assertEquals(
            "SELECT bi,bs,bd,bdt1,bb FROM bar WHERE bs>=%s",
            sql
            )
        self.assertEquals(['x'], values)
        sql, values = sqlgen.select(
            Foo, AND(Foo.q.i1 < 12, Foo.q.s1 <= 'y'), None,
            True, "?"
            )
        self.assertEquals(
            "SELECT foo_id,i1,s1,d1 FROM foo WHERE i1<? AND s1<=?",
            sql
            )
        self.assertEquals([12, 'y'], values)
        # None/NULL
        def check_no_None(cond, opstr):
            try:
                sqlgen.select(Bar, cond, None, True, "?")
            except AssertionError, e:
                self.assertEquals("Op '%s' does not support None" % opstr, str(e))
            else:
                self.fail()
        check_no_None(Bar.q.bi > None, ">")
        check_no_None(Bar.q.bi >= None, ">=")
        check_no_None(Bar.q.bi < None, "<")
        check_no_None(Bar.q.bi <= None, "<=")
        # BoolCol
        try:
            sqlgen.select(
                Bar, Bar.q.bb > False, None, True, "?"
                )
        except AssertionError, e:
            self.assertEquals("Op '>' does not support BoolCol", str(e))
        else:
            self.fail()
        # DateTimeUTCCol
        try:
            sqlgen.select(
                Bar,
                Bar.q.bdt1 < datetime.datetime(2007, 5, 23, 0, 42, 12),
                None,
                True,
                "?"
                )
        except AssertionError, e:
            self.assertEquals("Op '<' does not support DateTimeUTCCol", str(e))
        else:
            self.fail()


    def test_where_YEAR(self):
        from datetime import date
        # non DateCol
        try:
            sqlgen.select(
                Foo, Foo.q.i1.YEAR(12), None, True, "?"
                )
        except AssertionError, e:
            self.assertEquals("YEAR condition can only be used for DateCol", str(e))
        else:
            self.fail()
        # YEAR / NULL
        try:
            sqlgen.select(
                Foo, Foo.q.d1.YEAR(None), None, True, "?"
                )
        except AssertionError, e:
            self.assertEquals("YEAR condition cannot use None", str(e))
        else:
            self.fail()
        # YEAR
        sql, values = sqlgen.select(
            Foo, Foo.q.d1.YEAR(date(2006,3,14)), None,
            True, "?"
            )
        self.assertEquals(
            "SELECT foo_id,i1,s1,d1 FROM foo WHERE d1 LIKE ?",
            sql
            )
        self.assertEquals(["2006-%"], values)
        sql, values = sqlgen.select(
            Foo, Foo.q.d1.YEAR(date(2006,3,14)), None,
            False, "%s"
            )
        self.assertEquals(
            "SELECT foo_id,i1,s1,d1 FROM foo WHERE EXTRACT(YEAR FROM d1)=%s",
            sql
            )
        self.assertEquals([2006], values)

    def test_where_MONTH(self):
        from datetime import date
        # non DateCol
        try:
            sqlgen.select(
                Foo, Foo.q.i1.MONTH(12), None,
                True, "?"
                )
        except AssertionError, e:
            self.assertEquals("MONTH condition can only be used for DateCol", str(e))
        else:
            self.fail()
        # NULL
        try:
            sqlgen.select(
                Foo, Foo.q.d1.MONTH(None), None,
                True, "?"
                )
        except AssertionError, e:
            self.assertEquals("MONTH condition cannot use None", str(e))
        else:
            self.fail()
        # MONTH
        sql, values = sqlgen.select(
            Foo, Foo.q.d1.MONTH(date(2005,7,12)), None,
            True, "?"
            )
        self.assertEquals(
            "SELECT foo_id,i1,s1,d1 FROM foo WHERE d1 LIKE ?",
            sql
            )
        self.assertEquals(["%-07-%"], values)
        sql, values = sqlgen.select(
            Foo, Foo.q.d1.MONTH(date(2005,7,12)), None,
            False, "%s"
            )
        self.assertEquals(
            "SELECT foo_id,i1,s1,d1 FROM foo" \
                + " WHERE EXTRACT(MONTH FROM d1)=%s",
            sql
            )
        self.assertEquals([7], values)

    def test_where_DAY(self):
        from datetime import date
        # non DateCol
        try:
            sqlgen.select(
                Foo, Foo.q.i1.DAY(12), None,
                True, "?"
                )
        except AssertionError, e:
            self.assertEquals("DAY condition can only be used for DateCol", str(e))
        else:
            self.fail()
        # NULL
        try:
            sqlgen.select(
                Foo, Foo.q.d1.DAY(None), None,
                True, "?"
                )
        except AssertionError, e:
            self.assertEquals("DAY condition cannot use None", str(e))
        else:
            self.fail()
        # MONTH
        sql, values = sqlgen.select(
            Foo, Foo.q.d1.DAY(date(2005,7,2)), None,
            True, "?"
            )
        self.assertEquals(
            "SELECT foo_id,i1,s1,d1 FROM foo WHERE d1 LIKE ?",
            sql
            )
        self.assertEquals(["%-02"], values)
        sql, values = sqlgen.select(
            Foo, Foo.q.d1.DAY(date(2005,7,2)), None,
            False, "%s"
            )
        self.assertEquals(
            "SELECT foo_id,i1,s1,d1 FROM foo" \
                + " WHERE EXTRACT(DAY FROM d1)=%s",
            sql
            )
        self.assertEquals([2], values)


    def x():
        sql, values = sqlgen.select(
            Bar, Bar.q.bs == 'x', None, True, "?"
            )
        self.assertEquals(
            "SELECT bi,bs,bd FROM bar WHERE bs=?",
            sql
            )
        self.assertEquals(['x'], values)
        sql, values = sqlgen.select(
            Foo,
            AND(Foo.q.i1 == 12, Foo.q.d1 == None, Foo.q.s1 == 'y'),
            None,
            True,
            "?"
            )
        self.assertEquals(
            "SELECT foo_id,i1,s1,d1 FROM foo WHERE i1=? AND d1 is NULL AND s1=?",
            sql
            )
        self.assertEquals([12, 'y'], values)

    def test_order_by(self):
        # ASC
        sql, values = sqlgen.select(
            Foo, None, Foo.q.foo_id.ASC, True, "?"
            )
        self.assertEquals(
            "SELECT foo_id,i1,s1,d1 FROM foo ORDER BY foo_id ASC",
            sql
            )
        self.assertEquals([], values)
        # DESC
        sql, values = sqlgen.select(
            Foo, None, Foo.q.s1.DESC, False, "%s"
            )
        self.assertEquals(
            "SELECT foo_id,i1,s1,d1 FROM foo ORDER BY s1 DESC",
            sql
            )
        self.assertEquals([], values)


class SelectDistinctTest(unittest.TestCase):

    def test(self):
        sql, values = sqlgen.select_distinct(
            Foo, Foo.q.i1, None, None, True, "?"
            )
        self.assertEquals(
            "SELECT DISTINCT i1 FROM foo",
            sql
            )
        self.assertEquals([], values)

    def test_where(self):
        sql, values = sqlgen.select_distinct(
            Foo, Foo.q.s1, Foo.q.foo_id == 1, None, True, "?"
            )
        self.assertEquals(
            "SELECT DISTINCT s1 FROM foo WHERE foo_id=?",
            sql
            )
        self.assertEquals([1], values)
        #
        sql, values = sqlgen.select_distinct(
            Foo,
            Foo.q.d1,
            AND(Foo.q.i1 == 12, Foo.q.d1 == None, Foo.q.s1 == 'y'),
            None,
            False,
            "%s"
            )
        self.assertEquals(
            "SELECT DISTINCT d1 FROM foo WHERE i1=%s AND d1 is NULL AND s1=%s",
            sql
            )
        self.assertEquals([12, 'y'], values)
        #
        sql, values = sqlgen.select_distinct(
            Foo,
            Foo.q.d1,
            OR(Foo.q.i1 == 12, Foo.q.d1 == None, Foo.q.s1 == 'y'),
            None,
            True,
            "?"
            )
        self.assertEquals(
            "SELECT DISTINCT d1 FROM foo WHERE i1=? OR d1 is NULL OR s1=?",
            sql
            )
        self.assertEquals([12, 'y'], values)

    def test_order_by(self):
        sql, values = sqlgen.select_distinct(
            Foo, Foo.q.foo_id, None, Foo.q.foo_id.ASC,
            True, "?"
            )
        self.assertEquals(
            "SELECT DISTINCT foo_id FROM foo ORDER BY foo_id ASC",
            sql
            )

    def test_order_by_different_col(self):
        try:
            sqlgen.select_distinct(
                Foo, Foo.q.i1, None, Foo.q.s1.ASC,
                True, "?"
                )
        except AssertionError, e:
            self.assertEquals(
                "SELECT DISTINCT column must match 'order_by' column", str(e)
                )
        else:
            self.fail()



if __name__ == '__main__':
    unittest.main()
