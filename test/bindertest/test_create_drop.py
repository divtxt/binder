
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



class CreateDropTest(unittest.TestCase):

    def test(self):
        conn = connect()
        try:
            conn.drop_table(Foo)
        except conn.DbError, e:
            self.assertEquals("no such table: foo", str(e))
        conn = connect()
        conn.create_table(Foo)
        conn = connect()
        try:
            conn.create_table(Foo)
        except conn.DbError, e:
            self.assertEquals("table foo already exists", str(e))
        conn = connect()
        conn.drop_table(Foo)
        conn = connect()
        try:
            conn.drop_table(Foo)
        except conn.DbError, e:
            self.assertEquals("no such table: foo", str(e))

    def test_create_RO(self):
        conn = connect("test123")
        try:
            conn.create_table(Foo)
        except Exception, e:
            self.assertEquals("Connection is read only: test123", str(e))
        else:
            self.fail()

    def test_drop_RO(self):
        conn = connect("test123")
        try:
            conn.drop_table(Foo)
        except Exception, e:
            self.assertEquals("Connection is read only: test123", str(e))
        else:
            self.fail()


if __name__ == '__main__':
    unittest.main()
