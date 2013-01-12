Welcome to Binder
=================

Binder is a lightweight SQL mapper for Python.

It lets you perform SQL operations using native Python data types and methods:

>>> conn.select(Trades, AND(Trades.q.symbol=='RHAT', Trades.q.trans=='BUY'))
[{'date': datetime.date(2006, 1, 5), 'symbol': u'RHAT', 'trans': u'BUY', 'price'
: 35.0, 'qty': 100}]

Binder is a transparent mapper that gives you control of what SQL query is
executed, while taking care of query generation, parameter passing and data
conversion.

Binder's mapping rules are simple:

- A database row is mapped to/from a Python dictionary.
- One API method corresponds to one SQL query.

Currently, Binder supports the SQLite (via Python's built-in sqlite3 module)
and MySQL (via the MySQLdb python module).


Contents
========

.. toctree::
   :maxdepth: 2



Indices and tables
==================

* :ref:`genindex`
* :ref:`search`

.. * :ref:`modindex`
