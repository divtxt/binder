Welcome to Binder
=================

Binder is a lightweight SQL mapper for Python that lets you perform SQL
operations using native Python data types and methods:

>>> conn.select(Trades, AND(Trades.q.symbol=='RHAT', Trades.q.trans=='BUY'))
[{'date': datetime.date(2006, 1, 5), 'symbol': u'RHAT', 'trans': u'BUY', 'price'
: 35.0, 'qty': 100}]

A database row is mapped to a Python dictionary containing native Python data
types.  Binder gives you control of what SQL query is executed, while taking
care of query generation, parameter passing and data conversion.

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
