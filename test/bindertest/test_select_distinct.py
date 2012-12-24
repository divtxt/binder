
import unittest

from binder import *
import datetime

from bindertest.testdbconfig import connect
from bindertest.tabledefs import Foo


foo1 = Foo.new(foo_id=1, i1=101, s1="alpha")
foo2 = Foo.new(foo_id=2, i1=101, s1="beta")
foo3 = Foo.new(foo_id=3, i1=102, s1="beta")


class ConnSelectDistinctTest(unittest.TestCase):

    def setUp(self):
        conn = connect()
        try:
            conn.drop_table(Foo)
        except conn.DbError, e:
            self.assertEquals("no such table: foo", str(e))
        conn = connect()
        conn.create_table(Foo)
        conn = connect()
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        conn.insert(Foo, foo3)
        conn.commit()

    def test_xselect_distinct(self):
        conn = connect()
        foo_list = \
            conn.xselect_distinct(Foo, Foo.q.i1)
        # FIXME: depending on db order
        self.assertEquals(101, foo_list.next())
        self.assertEquals(102, foo_list.next())

    def test_where(self):
        conn = connect()
        foo_list = \
            conn.select_distinct(Foo, Foo.q.foo_id, Foo.q.s1 == "alpha")
        self.assertEquals([1], foo_list)

    def test_order_by(self):
        conn = connect()
        foo_list = \
            conn.select_distinct(
                    Foo,
                    Foo.q.s1,
                    order_by=Foo.q.s1.DESC
                )
        self.assertEquals(["beta", "alpha"], foo_list)

    def test_where_order_by(self):
        conn = connect()
        foo_list = \
            conn.select_distinct(
                    Foo,
                    Foo.q.foo_id,
                    Foo.q.s1 == "beta",
                    Foo.q.foo_id.ASC
                )
        self.assertEquals([2, 3], foo_list)



if __name__ == '__main__':
    unittest.main()
