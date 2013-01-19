
import unittest

from binder import *
import datetime

from bindertest.testdbconfig import connect
from bindertest.tabledefs import Foo, Bar


S_ENGLISH_ASCII = "presentation"
S_ENGLISH_U = unicode("presentation")

S_SPANISH_UTF8 = "presentaci\xc2\xa2n"
S_SPANISH_U = unicode(S_SPANISH_UTF8, "utf-8")

S_FRENCH_UTF8 = "pr\xc2\x82sentation"
S_FRENCH_U = unicode(S_FRENCH_UTF8, "utf-8")

S_GERMAN_UTF8 = "f\xc2\x81nf"
S_GERMAN_U = unicode(S_GERMAN_UTF8, "utf-8")


Foo = Table(
    "foo",
    AutoIdCol("foo_id"),
    UnicodeCol("s1", 40),
)


class ConnInsertTest(unittest.TestCase):

    def setUp(self):
        conn = connect()
        conn.drop_table_if_exists(Foo)
        conn.create_table(Foo)
        conn.commit()

    def test_insert_ascii(self):
        foo1 = Foo.new(foo_id=1, s1=S_ENGLISH_ASCII)
        foo2 = Foo.new(foo_id=2, s1=S_ENGLISH_U)
        conn = connect()
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        conn.commit()
        conn.close()
        conn = connect()
        foo_list = conn.select(Foo, order_by=Foo.q.foo_id.ASC)
        self.assertEquals([foo1, foo2], foo_list)
        conn.close()

    def test_insert_unicode(self):
        foo1 = Foo.new(foo_id=1, s1=S_SPANISH_U)
        foo2 = Foo.new(foo_id=2, s1=S_FRENCH_U)
        conn = connect()
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        conn.commit()
        conn.close()
        conn = connect()
        foo_list = conn.select(Foo, order_by=Foo.q.foo_id.ASC)
        self.assertEquals([foo1, foo2], foo_list)
        conn.close()

    def test_insert_nonascii(self):
        foo1 = Foo.new(foo_id=1, s1=S_GERMAN_U)
        foo2 = Foo.new(foo_id=2, s1=S_GERMAN_UTF8)
        conn = connect()
        conn.insert(Foo, foo1)
        try:
            conn.insert(Foo, foo2)
        except UnicodeDecodeError, e:
            self.assertEquals(
                    (
                        'ascii',
                        S_GERMAN_UTF8,
                        1,
                        2,
                        'ordinal not in range(128)'
                    ),
                    e.args
                )
        else:
            self.fail()
        conn.commit()
        conn.close()
        conn = connect()
        foo_list = conn.select(Foo, order_by=Foo.q.foo_id.ASC)
        self.assertEquals([foo1], foo_list)
        conn.close()

    def test_update_by_id(self):
        foo1 = Foo.new(foo_id=1, s1=S_SPANISH_U)
        foo2 = Foo.new(foo_id=2, s1=S_FRENCH_U)
        conn = connect()
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        conn.commit()
        conn.close()
        conn = connect()
        foo2["s1"] = S_GERMAN_U
        conn.update_by_id(Foo, foo2)
        conn.commit()
        conn.close()
        conn = connect()
        foo2b = conn.get(Foo, 2)
        self.assertEquals(foo2, foo2b)
        conn.close()

    def test_update_where(self):
        foo1 = Foo.new(foo_id=1, s1=S_SPANISH_U)
        foo2 = Foo.new(foo_id=2, s1=S_FRENCH_U)
        conn = connect()
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        conn.commit()
        conn.close()
        conn = connect()
        foo3 = Foo.new(foo_id=3, s1=S_ENGLISH_ASCII)
        rc = conn.update(Foo, foo3, Foo.q.s1 == S_FRENCH_U)
        self.assertEquals(1, rc)
        conn.commit()
        conn.close()
        conn = connect()
        foo_list = conn.select(Foo, order_by=Foo.q.foo_id.ASC)
        self.assertEquals([foo1, foo3], foo_list)
        conn.close()



if __name__ == '__main__':
    unittest.main()
