
import unittest

from binder import *
import datetime

from bindertest.testdbconfig import connect, connect_postgres, connect_sqlite
from bindertest.tabledefs import Foo


class ConnSelectTest(unittest.TestCase):

    def setUp(self):
        conn = connect()
        conn.drop_table_if_exists(Foo)
        conn.create_table(Foo)
        conn.commit()

    def test_select(self):
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101, s1="alpha")
        foo2 = Foo.new(foo_id=2, s1="beta")
        conn.insert(Foo, foo1)
        foo_list = conn.select(Foo)
        self.assertEquals([foo1], foo_list)
        conn.insert(Foo, foo2)
        foo_list = conn.select(Foo)
        # FIXME: depending on db order
        self.assertEquals([foo1, foo2], foo_list)

    def test_where_eq(self):
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101, s1="alpha")
        foo2 = Foo.new(foo_id=2, i1=23, s1="beta")
        foo3 = Foo.new(foo_id=3, i1=42, s1="alpha", d1=datetime.date(2006, 6, 10))
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        conn.insert(Foo, foo3)
        # where AutoIdCol
        foo_list = conn.select(Foo, Foo.q.foo_id == 1)
        self.assertEquals([foo1], foo_list)
        # where UnicodeCol
        foo_list = conn.select(Foo, Foo.q.s1 == 'beta')
        self.assertEquals([foo2], foo_list)
        # where DateCol / NULL
        foo_list = conn.select(Foo, Foo.q.d1 == None)
        # FIXME: depending on db order
        self.assertEquals([foo1, foo2], foo_list)
        # where DateCol match
        foo_list = conn.select(Foo, Foo.q.d1 == datetime.date(2006, 6, 10))
        self.assertEquals([foo3], foo_list)
        # where DateCol no matches
        foo_list = conn.select(Foo, Foo.q.d1 == datetime.date(2006, 5, 23))
        self.assertEquals([], foo_list)
        # where AND
        foo_list = conn.select(Foo, AND(Foo.q.i1 == 101, Foo.q.s1 == 'alpha'))
        self.assertEquals([foo1], foo_list)
        foo_list = conn.select(Foo, AND(Foo.q.foo_id == 2, Foo.q.d1 == None))
        self.assertEquals([foo2], foo_list)
        # AND 3 conditions
        foo_list = conn.select(Foo, AND(Foo.q.i1 == 23, Foo.q.s1 == 'beta', Foo.q.d1 == None))
        self.assertEquals([foo2], foo_list)
        foo_list = conn.select(Foo, AND(Foo.q.foo_id == 2, Foo.q.d1 == None, Foo.q.i1 == 3))
        self.assertEquals([], foo_list)
        # where OR
        foo_list = conn.select(Foo, OR(Foo.q.i1 == 3923, Foo.q.s1 == 'beta'))
        self.assertEquals([foo2], foo_list)
        foo_list = conn.select(Foo, OR(Foo.q.foo_id == 2, Foo.q.d1 == datetime.date(2007, 5, 11)))
        self.assertEquals([foo2], foo_list)

    def test_where_date_ops(self):
        conn = connect()
        foo1 = Foo.new(foo_id=1, d1=datetime.date(2005, 11, 24))
        foo2 = Foo.new(foo_id=2, d1=datetime.date(2006, 2, 16))
        foo3 = Foo.new(foo_id=3, d1=datetime.date(2006, 5, 4))
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        conn.insert(Foo, foo3)
        # YEAR
        foo_list = conn.select(Foo, Foo.q.d1.YEAR(datetime.date(2005,1,1)))
        self.assertEquals([foo1], foo_list)
        foo_list = conn.select(Foo, Foo.q.d1.YEAR(datetime.date(2006,3,24)))
        self.assertEquals([foo2, foo3], foo_list)
        # MONTH
        foo_list = conn.select(Foo, Foo.q.d1.MONTH(datetime.date(2012,11,1)))
        self.assertEquals([foo1], foo_list)
        foo_list = conn.select(Foo, Foo.q.d1.MONTH(datetime.date(2012,2,24)))
        self.assertEquals([foo2], foo_list)
        # DAY
        foo_list = conn.select(Foo, Foo.q.d1.DAY(datetime.date(2012,2,24)))
        self.assertEquals([foo1], foo_list)
        foo_list = conn.select(Foo, Foo.q.d1.DAY(datetime.date(2012,11,4)))
        self.assertEquals([foo3], foo_list)

    def test_where_gt_ge_lt_le(self):
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101, s1="alpha", d1=datetime.date(2005, 11, 24))
        foo2 = Foo.new(foo_id=2, i1=23, s1="beta", d1=datetime.date(2006, 2, 16))
        foo3 = Foo.new(foo_id=3, i1=42, s1="alpha", d1=datetime.date(2006, 6, 10))
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        conn.insert(Foo, foo3)
        # where __gt__ & AutoIdCol
        foo_list = conn.select(Foo, Foo.q.foo_id > 2)
        self.assertEquals([foo3], foo_list)
        # where __ge__ & UnicodeCol
        foo_list = conn.select(Foo, Foo.q.s1 >= 'beta')
        self.assertEquals([foo2], foo_list)
        # where __lt__ & IntCol
        foo_list = conn.select(Foo, Foo.q.i1 < 42)
        self.assertEquals([foo2], foo_list)
        # where __le__ & IntCol
        foo_list = conn.select(Foo, Foo.q.i1 <= 42)
        # FIXME: depending on db order
        self.assertEquals([foo2, foo3], foo_list)
        # where AND, AutoIdCol, gt, IntCol, lt
        foo_list = conn.select(Foo, AND(Foo.q.foo_id > 1, Foo.q.i1 < 30))
        self.assertEquals([foo2], foo_list)
        # where AND, DateCol, gt, lt
        foo_list = conn.select(
            Foo,
            AND(
                Foo.q.d1 > datetime.date(2005, 11, 24),
                Foo.q.d1 < datetime.date(2006, 6, 10),
            )
        )
        self.assertEquals([foo2], foo_list)

        # where AND, DateCol, gt, lt
        foo_list = conn.select(
            Foo,
            AND(
                Foo.q.d1 > datetime.date(2005, 11, 24),
                Foo.q.d1 < datetime.date(2006, 6, 10),
            )
        )
        self.assertEquals([foo2], foo_list)
        # where AND, DateCol, gt, le
        foo_list = conn.select(
            Foo,
            AND(
                Foo.q.d1 >= datetime.date(2005, 11, 24),
                Foo.q.d1 <= datetime.date(2006, 6, 10),
            )
        )
        # FIXME: depending on db order
        self.assertEquals([foo1, foo2, foo3], foo_list)

    def test_where_LIKE_ILIKE(self):
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101, s1="ab pq")
        foo2 = Foo.new(foo_id=2, i1=23, s1="AB PQ XY")
        foo3 = Foo.new(foo_id=3, i1=42, s1="pq xy")
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        conn.insert(Foo, foo3)
        # where trailing %
        if connect != connect_sqlite:
            foo_list = conn.select(Foo, Foo.q.s1.LIKE('ab%'))
            self.assertEquals([foo1], foo_list)
        foo_list = conn.select(Foo, Foo.q.s1.ILIKE('ab%'))
        self.assertEquals([foo1, foo2], foo_list)
        # where trailing % - no matches
        if connect != connect_sqlite:
            foo_list = conn.select(Foo, Foo.q.s1.LIKE('%z%'))
            self.assertEquals([], foo_list)
        foo_list = conn.select(Foo, Foo.q.s1.ILIKE('%z%'))
        self.assertEquals([], foo_list)
        # where leading %
        if connect != connect_sqlite:
            foo_list = conn.select(Foo, Foo.q.s1.LIKE('%xy'))
            self.assertEquals([foo3], foo_list)
        foo_list = conn.select(Foo, Foo.q.s1.ILIKE('%xy'))
        self.assertEquals([foo2, foo3], foo_list)
        # where both sides %
        if connect != connect_sqlite:
            foo_list = conn.select(Foo, Foo.q.s1.LIKE('%x%'))
            self.assertEquals([foo3], foo_list)
        foo_list = conn.select(Foo, Foo.q.s1.ILIKE('%x%'))
        self.assertEquals([foo2, foo3], foo_list)

    def x():
        # where given date
        # FIXME: depending on db order
        self.assertEquals([foo1, foo2], foo_list)
        # where AND
        foo_list = conn.select(Foo, AND(Foo.q.i1 == 101, Foo.q.s1 == 'alpha'))
        self.assertEquals([foo1], foo_list)
        foo_list = conn.select(Foo, AND(Foo.q.foo_id == 2, Foo.q.d1 == None))
        self.assertEquals([foo2], foo_list)
        # AND 3 conditions
        foo_list = conn.select(Foo, AND(Foo.q.i1 == 23, Foo.q.s1 == 'beta', Foo.q.d1 == None))
        self.assertEquals([foo2], foo_list)
        foo_list = conn.select(Foo, AND(Foo.q.foo_id == 2, Foo.q.d1 == None, Foo.q.i1 == 3))
        self.assertEquals([], foo_list)

    def test_order_by(self):
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101, s1="a")
        foo2 = Foo.new(foo_id=2, i1=23, s1="B")
        foo3 = Foo.new(foo_id=3, i1=42, s1="c")
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        conn.insert(Foo, foo3)
        # order by AutoIdCol
        foo_list = list(conn.select(Foo, order_by=Foo.q.foo_id.ASC))
        self.assertEquals([foo1, foo2, foo3], foo_list)
        foo_list = list(conn.select(Foo, order_by=Foo.q.foo_id.DESC))
        self.assertEquals([foo3, foo2, foo1], foo_list)
        # order by IntCol
        foo_list = list(conn.select(Foo, order_by=Foo.q.i1.ASC))
        self.assertEquals([foo2, foo3, foo1], foo_list)
        foo_list = list(conn.select(Foo, order_by=Foo.q.i1.DESC))
        self.assertEquals([foo1, foo3, foo2], foo_list)
        # UnicodeCol - ignore case
        foo_list = list(conn.select(Foo, order_by=Foo.q.s1.ASC))
        if connect == connect_postgres:
            self.assertEquals([foo2, foo1, foo3], foo_list)
        else:
            self.assertEquals([foo1, foo2, foo3], foo_list)
        foo_list = list(conn.select(Foo, order_by=Foo.q.s1.DESC))
        if connect == connect_postgres:
            self.assertEquals([foo3, foo1, foo2], foo_list)
        else:
            self.assertEquals([foo3, foo2, foo1], foo_list)

    def test_where_order_by(self):
        conn = connect()
        foo1 = Foo.new(foo_id=1, i1=101, s1="alpha")
        foo2 = Foo.new(foo_id=2, i1=23, s1="beta")
        foo3 = Foo.new(foo_id=3, i1=42, s1="alpha", d1=datetime.date(2006, 6, 10))
        foo4 = Foo.new(foo_id=4, i1=84, s1="alpha")
        conn.insert(Foo, foo1)
        conn.insert(Foo, foo2)
        conn.insert(Foo, foo3)
        conn.insert(Foo, foo4)
        # where UnicodeCol / order by
        foo_list = conn.select(Foo, Foo.q.s1 == 'alpha', Foo.q.foo_id.ASC)
        self.assertEquals([foo1, foo3, foo4], foo_list)
        # where AND / order by
        foo_list = conn.select(
            Foo,
            AND(Foo.q.s1 == 'alpha', Foo.q.d1 == None),
            Foo.q.i1.DESC
        )
        self.assertEquals([foo1, foo4], foo_list)


    # select sum(...) ...




if __name__ == '__main__':
    unittest.main()
