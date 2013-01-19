
import unittest
from assertutil import get_assert_tuple_args

from binder import *

from bindertest.testdbconfig import connect
from bindertest.tabledefs import Foo


class ConnSelectOneTest(unittest.TestCase):

    def setUp(self):
        conn = connect()
        conn.drop_table_if_exists(Foo)
        conn.create_table(Foo)
        conn.commit()

    def test(self):
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101, s1="xyz")
        foo2 = Foo.new(foo_id=2, s1="xyz")
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        # no rows
        row = conn.select_one(Foo, AND(Foo.q.foo_id == 2, Foo.q.s1 == "abc"))
        self.assertEquals(None, row)
        # one row
        row = conn.select_one(Foo, Foo.q.i1 == 101)
        self.assertEquals(foo1, row)
        # more than one row
        try:
            conn.select_one(Foo, Foo.q.s1 == "xyz")
        except AssertionError, e:
            msg, info = get_assert_tuple_args(e)
            self.assertEquals("select_one(): more than 1 row", msg)
            table_name, sqlcond = info
            #table_name, sqlcond, rc = info
            self.assertEquals("foo", table_name)
            self.assertEquals('"s1 = \'xyz\'"', repr(sqlcond))
            #self.assertEquals(2, rc)
        else:
            self.fail()



if __name__ == '__main__':
    unittest.main()
