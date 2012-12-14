
import os
import sys
import csv
from binder.api import *


def check_col_names(csv_col_names, T):
    c1 = list(csv_col_names)
    c1.sort()
    c2 = [col.col_name for col in T.cols]
    c2.sort()
    assert c1 == c2

def csv2db(sqlconn, csv_file, T, values_encoding=None):
    assert isinstance(T, Table)
    if not os.path.exists(csv_file):
        print "No CSV file for table %s (file %s)" % (T.table_name, csv_file)
        return
    infile = open(csv_file, "rU")
    reader = csv.reader(infile, dialect='excel')
    first = True
    rc = 0
    for values in reader:
        if first:
            first = False
            csv_col_names = values
            check_col_names(csv_col_names, T)
        else:
            rc += 1
            row = {}
            if values_encoding:
                values = [v.decode(values_encoding) for v in values]
            map(row.__setitem__, csv_col_names, values)
            row = T.parse(**row)
            sqlconn.insert(T, row)
    print "Inserted %d rows to table %s from file %s" % (rc, T.table_name, csv_file)

