
import unittest
from assertutil import assertIn

from binder import *
import datetime

from bindertest.testdbconfig import connect, connect_sqlite
from bindertest.tabledefs import Foo, Baz


class ConnInsertTest(unittest.TestCase):

    def setUp(self):
        conn = connect()
        conn.drop_table_if_exists(Foo)
        conn.drop_table_if_exists(Baz)
        conn.create_table(Foo)
        conn.create_table(Baz)
        conn.commit()

    def test_manualid(self):
        conn = connect()
        foo = Foo.new(foo_id=1, i1=101, s1="alpha")
        conn.insert(Foo, foo)
        foo_list = conn.select(Foo)
        self.assertEquals([foo], foo_list)

    def test_auto_id(self):
        conn = connect()
        foo1 = Foo.new(i1=101, s1="alpha")
        self.assert_(foo1["foo_id"] is None)
        conn.insert(Foo, foo1)
        self.assertEquals(1, foo1["foo_id"])
        foo2 = Foo.new(s1="beta")
        self.assert_(foo2["foo_id"] is None)
        conn.insert(Foo, foo2)
        self.assertEquals(2, foo2["foo_id"])
        foo_list = conn.select(Foo)
        # FIXME: depending on db order
        self.assertEquals([foo1, foo2], foo_list)

    def test_autoid_clash(self):
        conn = connect()
        foo = Foo.new(foo_id=1, i1=101, s1="alpha")
        conn.insert(Foo, foo)
        foo_list = conn.select(Foo)
        self.assertEquals([foo], foo_list)
        try:
            conn.insert(Foo, foo)
        except conn.DbError, e:
            assertIn(self,
                e.args,
                [
                    ("PRIMARY KEY must be unique",),
                    ('duplicate key value violates unique constraint "foo_foo_id_key"\nDETAIL:  Key (foo_id)=(1) already exists.\n',),
                    (1062, "Duplicate entry \'1\' for key \'PRIMARY\'"),
                    ]
                )
        else:
            self.fail()

    def test_unique_clash(self):
        conn = connect()
        baz = Baz.new(s3="gamma")
        conn.insert(Baz, baz)
        baz_list = conn.select(Baz)
        self.assertEquals([baz], baz_list)
        baz2 = Baz.new(s3="gamma")
        try:
            conn.insert(Baz, baz2)
        except conn.DbError, e:
            assertIn(self,
                e.args,
                [
                    ("column s3 is not unique",),
                    ('duplicate key value violates unique constraint "baz_s3_key"\nDETAIL:  Key (s3)=(gamma) already exists.\n',),
                    (1062, "Duplicate entry \'gamma\' for key \'s3\'"),
                    ]
                )
        else:
            self.fail()

    def test_unique_null_clash(self):
        conn = connect()
        baz = Baz.new(s2=None)
        conn.insert(Baz, baz)
        baz_list = conn.select(Baz)
        self.assertEquals([baz], baz_list)
        baz2 = Baz.new(s2=None)
        try:
            conn.insert(Baz, baz2)
        except conn.DbError, e:
            assertIn(self,
                e.args,
                [
                    ("column s3 is not unique",),
                    ('duplicate key value violates unique constraint "baz_s3_key"\nDETAIL:  Key (s3)=() already exists.\n',),
                    (1062, "Duplicate entry \'\' for key \'s3\'"),
                    ]
                )
        else:
            self.fail()

    def test_roundtrip_check_values(self):
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101, s1="alpha", d1=datetime.date(2006, 6, 12))
        conn.insert(Foo, foo1)
        foo_list = conn.select(Foo)
        self.assertEquals([foo1], foo_list)
        foo2 = foo_list[0]
        Foo.check_values(foo2)

    def test_roundtrip_float_fidelity(self):
        before = 3.14159265358979323846264338327950288
        if connect == connect_sqlite:
            expected = before
        else:
            # XXX Postgres roundtrip issues
            # Postgres: http://psycopg.lighthouseapp.com/projects/62710/tickets/145
            expected = 3.14159265358979000737349451810587198
        import math
        conn = connect()
        conn.insert(Baz, Baz.new(f3=before, s3="pi"))
        after = conn.select_one(Baz, Baz.q.s3 == 'pi')["f3"]
        self.assertEquals(expected, after)

    def test_intcol_roundtrip(self):
        import sys
        conn = connect()
        foo = Foo.new(foo_id=1, i1=sys.maxint)
        conn.insert(Foo, foo)
        foo_list = conn.select(Foo)
        self.assertEquals([foo], foo_list)

    def test_RO(self):
        conn = connect("test123")
        try:
            foo = Foo.new(foo_id=1, i1=101, s1="alpha")
            conn.insert(Foo, foo)
        except Exception, e:
            self.assertEquals("Connection is read only: test123", str(e))
        else:
            self.fail()


if __name__ == '__main__':
    unittest.main()
