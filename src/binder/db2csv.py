
import sys
import csv
from binder.api import *


def db2csv(sqlconn, T, csv_file, values_encoding=None):
    assert isinstance(T, Table)
    row_iter = sqlconn.select(T)
    outfile = open(csv_file, "wb") # binary since csv uses crlf
    writer = csv.writer(outfile, dialect='excel')
    writer.writerow([col.col_name for col in T.cols])
    rc = 0
    for row in row_iter:
        values = [col.to_unicode_or_str(row[col.col_name]) for col in T.cols]
        if values_encoding:
            values = [v.encode(values_encoding) for v in values]
        writer.writerow(values)
        rc += 1
    print "Wrote %d rows to file %s from table %s" % (rc, csv_file, T.table_name)

