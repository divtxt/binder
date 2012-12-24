
import unittest

from binder import *
import datetime

from bindertest.testdbconfig import connect
from bindertest.tabledefs import Foo, Bar


class ConnDeleteTest(unittest.TestCase):

    def setUp(self):
        conn = connect()
        try:
            conn.drop_table(Foo)
        except conn.DbError, e:
            self.assertEquals("no such table: foo", str(e))
        try:
            conn.drop_table(Bar)
        except conn.DbError, e:
            self.assertEquals("no such table: bar", str(e))
        conn = connect()
        conn.create_table(Foo)
        conn.create_table(Bar)

    def test_delete_by_id(self):
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101, s1="alpha")
        foo2 = Foo.new(foo_id=2, s1="beta")
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        # existing id
        found = conn.delete_by_id(Foo, 2)
        self.assert_(found)
        self.assertEquals(foo1, conn.get(Foo, 1))
        self.assertEquals(None, conn.get(Foo, 2))
        # no such id
        found = conn.delete_by_id(Foo, 4)
        self.assertFalse(found)
        # None not allowed
        try:
            conn.delete_by_id(Foo, None)
        except AssertionError, e:
            self.assertEquals("delete_by_id(): cannot use None for AutoIdCol", str(e))
        else:
            self.fail()

    def test_delete_by_id_more_than_one_row(self):
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101)
        foo2 = Foo.new(foo_id=2, i1=101)
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        Foo2 = Table("foo", AutoIdCol("i1"), IntCol("foo_id"))
        try:
            conn.delete_by_id(Foo2, 101)
        except AssertionError, e:
            expected = "delete_by_id(): more than 1 row deleted", ("foo", "i1", 101, 2)
            self.assertEquals(expected, e.args)
        else:
            self.fail()

    def test_delete_by_id_RO(self):
        conn = connect("test123")
        try:
            conn.delete_by_id(Foo, 2)
        except Exception, e:
            self.assertEquals("Connection is read only: test123", str(e))
        else:
            self.fail()


    def test_delete_0_rows(self):
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101, s1="alpha")
        foo2 = Foo.new(foo_id=2, i1=23, s1="beta")
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        rowcount = conn.delete(Foo, Foo.q.foo_id == 3)
        self.assertEquals(0, rowcount)
        foo_list = conn.select(Foo, order_by=Foo.q.foo_id.ASC)
        self.assertEquals([foo1, foo2], foo_list)

    def test_delete_1_row(self):
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101, s1="alpha")
        foo2 = Foo.new(foo_id=2, i1=23, s1="beta")
        foo3 = Foo.new(foo_id=3, i1=42, s1="alpha", d1=datetime.date(2006, 6, 10))
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        conn.insert(Foo, foo3)
        rowcount = conn.delete(Foo, Foo.q.foo_id == 2)
        self.assertEquals(1, rowcount)
        foo_list = conn.select(Foo, order_by=Foo.q.foo_id.ASC)
        self.assertEquals([foo1, foo3], foo_list)
        rowcount = conn.delete(Foo, Foo.q.i1 == 101)
        self.assertEquals(1, rowcount)
        foo_list = conn.select(Foo, order_by=Foo.q.foo_id.ASC)
        self.assertEquals([foo3], foo_list)

    def test_delete_multiple_rows(self):
        conn = connect()
        bar1 = Bar.new(bi=2001, bs="abc")
        bar2 = Bar.new(bi=2002, bs="abc")
        bar3 = Bar.new(bi=2004, bs="def", bd=datetime.date(2006, 6, 10))
        bar4 = Bar.new(bi=2004, bs="ghi")
        conn.insert(Bar, bar1)
        conn.insert(Bar, bar2)
        conn.insert(Bar, bar3)
        conn.insert(Bar, bar4)
        rowcount = conn.delete(Bar, Bar.q.bs == "abc")
        self.assertEquals(2, rowcount)
        bar_list = conn.select(Bar, order_by=Bar.q.bi.ASC)
        self.assertEquals([bar3, bar4], bar_list)
        rowcount = conn.delete(Bar, Bar.q.bi == 2004)
        self.assertEquals(2, rowcount)
        bar_list = conn.select(Bar, order_by=Bar.q.bi.ASC)
        self.assertEquals([], bar_list)

    def test_delete_gt_ge(self):
        conn = connect()
        bar1 = Bar.new(bi=2001, bs="abc")
        bar2 = Bar.new(bi=2002, bs="abc")
        bar3 = Bar.new(bi=2004, bs="def", bd=datetime.date(2006, 6, 10))
        bar4 = Bar.new(bi=2004, bs="ghi")
        conn.insert(Bar, bar1)
        conn.insert(Bar, bar2)
        conn.insert(Bar, bar3)
        conn.insert(Bar, bar4)
        # gt
        rowcount = conn.delete(Bar, Bar.q.bs > "abc")
        self.assertEquals(2, rowcount)
        bar_list = conn.select(Bar, order_by=Bar.q.bi.ASC)
        self.assertEquals([bar1, bar2], bar_list)
        # ge
        rowcount = conn.delete(Bar, Bar.q.bi >= 2001)
        self.assertEquals(2, rowcount)
        bar_list = conn.select(Bar)
        self.assertEquals([], bar_list)

    def test_delete_lt_le(self):
        conn = connect()
        bar1 = Bar.new(bi=2001, bs="abc")
        bar2 = Bar.new(bi=2002, bs="abc")
        bar3 = Bar.new(bi=2004, bs="def", bd=datetime.date(2006, 6, 10))
        bar4 = Bar.new(bi=2004, bs="ghi")
        conn.insert(Bar, bar1)
        conn.insert(Bar, bar2)
        conn.insert(Bar, bar3)
        conn.insert(Bar, bar4)
        # lt
        rowcount = conn.delete(Bar, Bar.q.bs < "def")
        self.assertEquals(2, rowcount)
        bar_list = conn.select(Bar, order_by=Bar.q.bi.ASC)
        self.assertEquals([bar3, bar4], bar_list)
        # le
        rowcount = conn.delete(Bar, Bar.q.bi <= 2004)
        self.assertEquals(2, rowcount)
        bar_list = conn.select(Bar)
        self.assertEquals([], bar_list)

    def test_delete_RO(self):
        conn = connect("test123")
        try:
            conn.delete(Foo, Foo.q.foo_id == 3)
        except Exception, e:
            self.assertEquals("Connection is read only: test123", str(e))
        else:
            self.fail()



if __name__ == '__main__':
    unittest.main()
