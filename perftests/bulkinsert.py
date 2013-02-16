
import os, sys
PERFTEST_DIR = os.path.abspath(os.path.dirname(__file__))
PROJECT_DIR = os.path.dirname(PERFTEST_DIR)
sys.path.append(PROJECT_DIR)

from binder import *
import datetime

from bindertest.testdbconfig import connect


ZipCode = Table(
    "zipcode",
    UnicodeCol("zip", 5, unique=True),
    UnicodeCol("city", 64),
    UnicodeCol("state", 2),
    IntCol("timezone"),
    IntCol("dst"),
)

ZIPCODE_CSV = os.path.join(PERFTEST_DIR, "zipcode.csv")

LOADED_DATA_LISTS = None
LOADED_DATA_DICTS = None
CSV_COLS = ["zip", "city", "state", "latitude", "longitude", "timezone", "dst"]
DB_COLS = ["zip", "city", "state", "timezone", "dst"]

def setup():
    global LOADED_DATA_LISTS, LOADED_DATA_DICTS
    LOADED_DATA_LISTS = []
    LOADED_DATA_DICTS = []
    import csv
    reader = csv.reader(open(ZIPCODE_CSV, 'rb'))
    col_names = reader.next()
    assert col_names == CSV_COLS
    for row in reader:
        if row:
            assert len(row) == len(CSV_COLS)
            row = [row[0], row[1], row[2], row[5], row[6]]
            LOADED_DATA_LISTS.append(row)
            d = ZipCode.parse(**dict(zip(DB_COLS, row)))
            LOADED_DATA_DICTS.append(d)
    #
    conn = connect()
    conn.drop_table(ZipCode, if_exists=True)
    conn.create_table(ZipCode)
    conn.commit()

def dbapi_insert():
    conn = connect()
    conn.delete(ZipCode, None)
    conn.commit()
    dbconn = conn._dbconn
    cursor = dbconn.cursor()
    sql = "INSERT INTO ZipCode (zip, city, state, timezone, dst) VALUES (%s)" \
        % ",".join([conn.paramstr] * 5)
    for row in LOADED_DATA_LISTS:
        cursor.execute(sql, row)
    conn.commit()

def binder_insert():
    conn = connect()
    conn.delete(ZipCode, None)
    conn.commit()
    sql = "INSERT INTO ZipCode (zip, city, state, timezone, dst) VALUES (%s)" \
        % ",".join([conn.paramstr] * 5)
    for d in LOADED_DATA_DICTS:
        conn.insert(ZipCode, d)
    conn.commit()


if __name__ == "__main__":
    import timeit
    setup()
    reps = 1
    testfns = [
        "dbapi_insert",
        "binder_insert",
        ]
    print "Reps:", reps
    for f in testfns:
        t = timeit.timeit(
            "%s()" % f,
            setup="from __main__ import %s" % f,
            number=reps
            )
        print "%-40s %10f %10f" % (f, t, t/reps)

