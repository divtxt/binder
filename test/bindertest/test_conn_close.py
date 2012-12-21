
import unittest

from binder import *
import datetime

from bindertest.testdbconfig import connect
from bindertest.tabledefs import Foo, Bar



class ConnCloseTest(unittest.TestCase):

    def setUp(self):
        conn = connect()
        try:
            conn.drop_table(Foo)
        except conn.DbError, e:
            self.assertEquals("no such table: foo", str(e))
        conn = connect()
        conn.create_table(Foo)

    def test_close_insert(self):
        "Closing the connection prevents further operations on the connection."
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101, s1="alpha")
        conn.insert(Foo, foo1)
        conn.close()
        foo2 = Foo.new(foo_id=2, s1="beta")
        try:
            conn.insert(Foo, foo2)
        except conn.DbError, e:
            self.assertEquals("Cannot operate on a closed database.", str(e))
        else:
            self.fail()

    def test_select_close(self):
        "Closing the connection closes a partly consumed cursor."
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101, s1="alpha")
        foo2 = Foo.new(foo_id=2, i1=23, s1="beta")
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        foo_iter = conn.xselect(Foo)
        self.assertEquals(foo1, foo_iter.next())
        conn.close()
        try:
            foo_iter.next()
        except conn.DbError, e:
            self.assertEquals("Result cursor closed.", str(e))
        else:
            self.fail()

    def test_select_insert(self):
        "New operation closes a partly consumed cursor."
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101, s1="alpha")
        foo2 = Foo.new(foo_id=2, i1=23, s1="beta")
        conn.insert(Foo, foo1)
        foo_iter = conn.xselect(Foo)
        conn.insert(Foo, foo2)
        try:
            foo_iter.next()
        except conn.DbError, e:
            self.assertEquals("Result cursor closed.", str(e))
        else:
            self.fail()
        conn.close()


if __name__ == '__main__':
    unittest.main()
