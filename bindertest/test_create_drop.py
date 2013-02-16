
import unittest
from assertutil import assertIn

from binder import *

from bindertest.testdbconfig import connect, connect_postgres


Foo = Table(
    "foo",
    AutoIdCol("foo_id"),
    IntCol("i1"),
    UnicodeCol("s1", 10),
    DateCol("d1"),
)

_NO_TABLE_FOO = [
    ("no such table: foo",),            # sqlite
    ('table "foo" does not exist\n',),  # Postgres
    ]

_TABLE_FOO_EXISTS = [
    ("table foo already exists",),          # sqlite
    ('relation "foo" already exist\n',),    # Postgres
    ]



class CreateDropTest(unittest.TestCase):

    def test_create_drop(self):
        conn = connect()
        try:
            conn.drop_table(Foo)
        except conn.DbError, e:
            assertIn(self, e.args, _NO_TABLE_FOO)
        conn.commit()
        conn = connect()
        conn.create_table(Foo)
        conn = connect()
        try:
            conn.create_table(Foo)
        except conn.DbError, e:
            assertIn(self, e.args, _TABLE_FOO_EXISTS)
        conn.drop_table(Foo)
        conn = connect()
        try:
            conn.drop_table(Foo)
        except conn.DbError, e:
            assertIn(self, e.args, _NO_TABLE_FOO)

    def test_create_transactional(self):
        conn = connect()
        conn.create_table(Foo)
        conn = connect()
        if connect == connect_postgres:
            try:
                conn.drop_table(Foo)
            except conn.DbError, e:
                assertIn(self, e.args, _NO_TABLE_FOO)
        else:
            conn.drop_table(Foo)

    def test_drop_if_exists(self):
        conn = connect()
        try:
            conn.drop_table(Foo)
        except conn.DbError, e:
            assertIn(self, e.args, _NO_TABLE_FOO)
        conn = connect()
        conn.create_table(Foo)
        conn = connect()
        conn.drop_table(Foo, if_exists=True)
        try:
            conn.drop_table(Foo)
        except conn.DbError, e:
            assertIn(self, e.args, _NO_TABLE_FOO)
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
