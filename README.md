
# Binder - A Python SQL Mapper.

Binder is a lightweight SQL mapper for Python that lets you perform SQL
operations using native Python data types and methods:

    >>> conn.select(Trades, AND(Trades.q.symbol=='RHAT', Trades.q.trans=='BUY'))
    [{'date': datetime.date(2006, 1, 5), 'symbol': u'RHAT', 'trans': u'BUY', 'price'
    : 35.0, 'qty': 100}]

A database row is mapped to a Python dictionary containing native Python data
types.  Binder gives you control of what SQL query is executed, while taking
care of query generation, parameter passing and data conversion.

Currently, Binder supports the SQLite (via Python's built-in sqlite3 module),
PostgreSQL (via psycopg2) and MySQL (via the MySQLdb python module).

See manual.txt for full documentation.


## Installing

Install using pip:

    pip install binder

Install from source:

    python setup.py install


## Quick Tour

Define the schema for a table:

    >>> from binder import *
    >>> Trades = Table("trades",
    ...     DateCol("date"), UnicodeCol("trans", 4), UnicodeCol("symbol", 4),
    ...     IntCol("qty"), FloatCol("price")
    ... )

Creating a new database using SQLite and create the table we defined above:

    >>> conn = SqliteConnection("readme.db3")
    >>> conn.create_table(Trades)

Insert a row using a regular Python dictionary:

    >>> from datetime import date
    >>> row = {
    ...     "date":date(2006, 1, 5), "trans":"BUY", "symbol":"RHAT",
    ...     "qty":100, "price":35
    ... }
    >>> conn.insert(Trades, row)
    >>> conn.commit()

Finally, retrieve the data:

    >>> conn.select(Trades)
    [{'date': datetime.date(2006, 1, 5), 'symbol': u'RHAT', 'trans': u'BUY', 'price'
    : 35.0, 'qty': 100}]


