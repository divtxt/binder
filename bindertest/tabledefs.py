
from binder.table import Table
from binder.col import *


# Table with AutoIdCol
Foo = Table(
    "foo",
    AutoIdCol("foo_id"),
    IntCol("i1"),
    UnicodeCol("s1", 10, collate_nocase=True),
    DateCol("d1"),
)

# Table without any PK
Bar = Table(
    "bar",
    IntCol("bi", False),
    UnicodeCol("bs", 10),
    DateCol("bd"),
    DateTimeUTCCol("bdt1"),
    BoolCol("bb", True),
)

# Table with unique UnicodeCol
Baz = Table(
    "baz",
    AutoIdCol("baz_id"),
    FloatCol("f3"),
    UnicodeCol("s3", 5, unique=True),
)
