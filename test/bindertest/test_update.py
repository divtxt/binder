
import unittest
from assertutil import get_assert_tuple_args

from binder import *
import datetime

from bindertest.testdbconfig import connect
from bindertest.tabledefs import Foo, Bar, Baz


class ConnUpdateTest(unittest.TestCase):

    def setUp(self):
        conn = connect()
        conn.drop_table(Foo, True)
        conn.drop_table(Bar, True)
        conn.drop_table(Baz, True)
        conn = connect()
        conn.create_table(Foo)
        conn.create_table(Bar)
        conn.create_table(Baz)

    def test_update_by_id(self):
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101, s1="alpha")
        foo2 = Foo.new(foo_id=2, s1="beta")
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        # existing id
        foo1["i1"] = 102
        found = conn.update_by_id(Foo, foo1)
        self.assert_(found)
        self.assertEquals(foo1, conn.get(Foo, 1))
        # no such id
        foo3 = Foo.new(**foo2)
        foo3["foo_id"] = 3
        found = conn.update_by_id(Foo, foo3)
        self.assertFalse(found)
        self.assertEquals(foo2, conn.get(Foo, 2))
        # None not allowed
        foo4 = Foo.new()
        try:
            conn.update_by_id(Foo, foo4)
        except AssertionError, e:
            self.assertEquals("update_by_id(): cannot use None for AutoIdCol", str(e))
        else:
            self.fail()

    def test_update_by_id_more_than_one_row(self):
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101)
        foo2 = Foo.new(foo_id=2, i1=101)
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        Foo2 = Table("foo", AutoIdCol("i1"), StringCol("s1", 10))
        try:
            foo3 = Foo2.new(i1=101, s1="xyz")
            conn.update_by_id(Foo2, foo3)
        except AssertionError, e:
            expected = "update_by_id(): more than 1 row updated", ("foo", "i1", 101, 2)
            self.assertEquals(expected, get_assert_tuple_args(e))
        else:
            self.fail()

    def test_update_by_id_RO(self):
        conn = connect("test123")
        try:
            foo1 = Foo.new(i1=101, s1="xyz")
            conn.update_by_id(Foo, foo1)
        except Exception, e:
            self.assertEquals("Connection is read only: test123", str(e))
        else:
            self.fail()

    def test_update_0_rows(self):
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101, s1="alpha")
        foo2 = Foo.new(foo_id=2, i1=23, s1="beta")
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        foo4a = Foo.new(foo_id=4, i1=102, s1="gamma")
        rowcount = conn.update(Foo, foo4a, Foo.q.foo_id == 3)
        self.assertEquals(0, rowcount)
        foo_list = conn.select(Foo, order_by=Foo.q.foo_id.ASC)
        self.assertEquals([foo1, foo2], foo_list)

    def test_update_1_row(self):
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101, s1="alpha")
        foo2 = Foo.new(foo_id=2, i1=23, s1="beta")
        foo3 = Foo.new(foo_id=3, i1=42, s1="alpha", d1=datetime.date(2006, 6, 10))
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        conn.insert(Foo, foo3)
        foo4a = Foo.new(foo_id=4, i1=102, s1="gamma")
        rowcount = conn.update(Foo, foo4a, Foo.q.foo_id == 2)
        self.assertEquals(1, rowcount)
        foo_list = conn.select(Foo, order_by=Foo.q.foo_id.ASC)
        self.assertEquals([foo1, foo3, foo4a], foo_list)

    def test_update_idclash(self):
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101, s1="alpha")
        foo2 = Foo.new(foo_id=2, i1=23, s1="beta")
        foo3 = Foo.new(foo_id=3, i1=42, s1="alpha", d1=datetime.date(2006, 6, 10))
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        conn.insert(Foo, foo3)
        foo4a = Foo.new(foo_id=4, i1=102, s1="gamma")
        try:
            conn.update(Foo, foo4a, Foo.q.s1 == "alpha")
        except conn.DbError, e:
            self.assertIn(
                e.args,
                [
                    "PRIMARY KEY must be unique",
                    (1062, "Duplicate entry \'4\' for key \'PRIMARY\'"),
                    ]
                )
        else:
            self.fail()

    def test_update_unique_clash(self):
        conn = connect()
        baz1 = Baz.new(i3=101, s3="alpha")
        baz2 = Baz.new(i3=23, s3="beta")
        baz3 = Baz.new(i3=42, s3="gamma")
        conn.insert(Baz, baz1)
        conn.insert(Baz, baz2)
        conn.insert(Baz, baz3)
        baz3["s3"] = "beta"
        try:
            conn.update(Baz, baz3, Baz.q.i3 == 42)
        except conn.DbError, e:
            self.assertEquals("column s3 is not unique", str(e))
        else:
            self.fail()

    def test_update_multiple_rows(self):
        conn = connect()
        bar1 = Bar.new(bi=2001, bs="abc")
        bar2 = Bar.new(bi=2002, bs="abc")
        bar3 = Bar.new(bi=2002, bs="def", bd=datetime.date(2006, 6, 10))
        conn.insert(Bar, bar1)
        conn.insert(Bar, bar2)
        conn.insert(Bar, bar3)
        bar4a = Bar.new(bi=2004, bs="ghi")
        rowcount = conn.update(Bar, bar4a, Bar.q.bi == 2002)
        self.assertEquals(2, rowcount)
        bar_list = conn.select(Bar, order_by=Bar.q.bi.ASC)
        self.assertEquals([bar1, bar4a, bar4a], bar_list)
        bar5a = Bar.new(bi=2005, bs="jkl")
        rowcount = conn.update(Bar, bar5a, AND(Bar.q.bi == 2004, Bar.q.bs == "ghi"))
        self.assertEquals(2, rowcount)
        bar_list = conn.select(Bar, order_by=Bar.q.bi.ASC)
        self.assertEquals([bar1, bar5a, bar5a], bar_list)
        bar6a = Bar.new(bi=2006, bs="mno", bd=datetime.date(2006, 11, 22))
        rowcount = conn.update(Bar, bar6a, Bar.q.bd == None)
        self.assertEquals(3, rowcount)
        bar_list = conn.select(Bar, order_by=Bar.q.bi.ASC)
        self.assertEquals([bar6a, bar6a, bar6a], bar_list)

    def test_update_gt_ge_lt_le(self):
        conn = connect()
        bar1 = Bar.new(bi=2001, bs="abc")
        bar2 = Bar.new(bi=2002, bs="abc")
        bar3 = Bar.new(bi=2003, bs="def", bd=datetime.date(2006, 6, 10))
        conn.insert(Bar, bar1)
        conn.insert(Bar, bar2)
        conn.insert(Bar, bar3)
        bar4a = Bar.new(bi=2004, bs="ghi")
        # gt
        rowcount = conn.update(Bar, bar4a, Bar.q.bi > 2002)
        self.assertEquals(1, rowcount)
        bar_list = conn.select(Bar, order_by=Bar.q.bi.ASC)
        self.assertEquals([bar1, bar2, bar4a], bar_list)
        # ge
        rowcount = conn.update(Bar, bar4a, Bar.q.bi >= 2002)
        self.assertEquals(2, rowcount)
        bar_list = conn.select(Bar, order_by=Bar.q.bi.ASC)
        self.assertEquals([bar1, bar4a, bar4a], bar_list)
        # lt
        bar5a = Bar.new(bi=2005, bs="jkl")
        rowcount = conn.update(Bar, bar5a, AND(Bar.q.bi < 2005, Bar.q.bs == "ghi"))
        self.assertEquals(2, rowcount)
        bar_list = conn.select(Bar, order_by=Bar.q.bi.ASC)
        self.assertEquals([bar1, bar5a, bar5a], bar_list)
        # le
        bar6a = Bar.new(bi=2006, bs="mno", bd=datetime.date(2006, 11, 22))
        rowcount = conn.update(Bar, bar6a, Bar.q.bi <= 2005)
        self.assertEquals(3, rowcount)
        bar_list = conn.select(Bar, order_by=Bar.q.bi.ASC)
        self.assertEquals([bar6a, bar6a, bar6a], bar_list)

    def test_update_all_rows(self):
        conn = connect()
        bar1 = Bar.new(bi=2001, bs="abc")
        bar2 = Bar.new(bi=2002, bs="abc")
        conn.insert(Bar, bar1)
        conn.insert(Bar, bar2)
        bar4a = Bar.new(bi=2004, bs="ghi")
        rowcount = conn.update(Bar, bar4a, None)
        self.assertEquals(2, rowcount)
        bar_list = conn.select(Bar, order_by=Bar.q.bi.ASC)
        self.assertEquals([bar4a, bar4a], bar_list)

    def test_update_RO(self):
        conn = connect("test123")
        try:
            bar1 = Bar.new(bi=2001, bs="abc")
            conn.update(Bar, bar1, Bar.q.bi == 2002)
        except Exception, e:
            self.assertEquals("Connection is read only: test123", str(e))
        else:
            self.fail()


if __name__ == '__main__':
    unittest.main()
