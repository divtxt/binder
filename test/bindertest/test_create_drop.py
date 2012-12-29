
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

_NO_TABLE_FOO = [
    ("no such table: foo",),            # sqlite3
    (1051, "Unknown table \'foo\'"),    # MySQL
    ]

_TABLE_FOO_EXISTS = [
    ("table foo already exists",),          # sqlite3
    (1050, "Table \'foo\' already exists"), # MySQL
    ]



class CreateDropTest(unittest.TestCase):

    def test_create_drop(self):
        conn = connect()
        try:
            conn.drop_table(Foo)
        except conn.DbError, e:
            self.assertIn(e.args, _NO_TABLE_FOO)
        conn = connect()
        conn.create_table(Foo)
        conn = connect()
        try:
            conn.create_table(Foo)
        except conn.DbError, e:
            self.assertIn(e.args, _TABLE_FOO_EXISTS)
        conn = connect()
        conn.drop_table(Foo)
        conn = connect()
        try:
            conn.drop_table(Foo)
        except conn.DbError, e:
            self.assertIn(e.args, _NO_TABLE_FOO)

    def test_drop_if_exists(self):
        conn = connect()
        try:
            conn.drop_table(Foo)
        except conn.DbError, e:
            self.assertIn(e.args, _NO_TABLE_FOO)
        conn = connect()
        conn.create_table(Foo)
        conn = connect()
        conn.drop_table(Foo, if_exists=True)
        try:
            conn.drop_table(Foo)
        except conn.DbError, e:
            self.assertIn(e.args, _NO_TABLE_FOO)
        conn = connect()
        conn.drop_table(Foo, if_exists=True)
        conn.drop_table_if_exists(Foo)

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
