
import unittest

from binder import *
import datetime

from bindertest.testdbconfig import connect
from bindertest.tabledefs import Foo, Baz


class ConnInsertTest(unittest.TestCase):

    def setUp(self):
        conn = connect()
        conn.drop_table_if_exists(Foo)
        conn.drop_table_if_exists(Baz)
        conn = connect()
        conn.create_table(Foo)
        conn.create_table(Baz)

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
            self.assertIn(
                e.args,
                [
                    ("PRIMARY KEY must be unique",),
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
            self.assertIn(
                e.args,
                [
                    ("column s3 is not unique",),
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
            self.assertIn(
                e.args,
                [
                    ("column s3 is not unique",),
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

    def test_roundtrip_precision(self):
        import math
        conn = connect()
        conn.insert(Baz, Baz.new(f3=math.pi, s3="pi"))
        conn.insert(Baz, Baz.new(f3=math.e, s3="e"))
        pi = conn.select_one(Baz, Baz.q.s3 == 'pi')["f3"]
        self.assertEquals(math.pi, pi)
        e = conn.select_one(Baz, Baz.q.s3 == 'e')["f3"]
        self.assertEquals(math.e, e)

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
