
import unittest

from binder import *

from bindertest.testdbconfig import connect


Foo = Table(
    "foo",
    AutoIdCol("foo_id"),
    IntCol("i1"),
    StringCol("s1", 10),
    DateCol("d1"),
)



class CommitRollbackTest(unittest.TestCase):

    def setUp(self):
        conn = connect()
        conn.drop_table_if_exists(Foo)
        conn.create_table(Foo)

    def test_commit(self):
        foo = Foo.new(foo_id=1, i1=10, s1="xyz")
        conn1 = connect()
        conn2 = connect()
        conn1.insert(Foo, foo)
        self.assertEquals([], conn2.select(Foo))
        conn1.commit()
        self.assertEquals([foo], conn2.select(Foo))

    def test_rollback(self):
        foo = Foo.new(foo_id=1, i1=10, s1="xyz")
        conn1 = connect()
        conn1.insert(Foo, foo)
        self.assertEquals([foo], conn1.select(Foo))
        conn1.rollback()
        self.assertEquals([], conn1.select(Foo))
        conn2 = connect()
        self.assertEquals([], conn2.select(Foo))

    def test_del(self):
        # del should cause rollback
        foo = Foo.new(foo_id=1, i1=10, s1="xyz")
        conn1 = connect()
        conn1.insert(Foo, foo)
        conn1 = None
        self.assertEquals([], connect().select(Foo))

    def test_close(self):
        # close should cause rollback
        foo = Foo.new(foo_id=1, i1=10, s1="xyz")
        conn1 = connect()
        conn1.insert(Foo, foo)
        conn1.close()
        self.assertEquals([], connect().select(Foo))



if __name__ == '__main__':
    unittest.main()
