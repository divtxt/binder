
from binder.table import Table
from binder.col import *


# Table with AutoIdCol
Foo = Table(
    "foo",
    AutoIdCol("foo_id"),
    IntCol("i1"),
    StringCol("s1", 10),
    DateCol("d1"),
)

# Table without any PK
Bar = Table(
    "bar",
    IntCol("bi", False),
    StringCol("bs", 10),
    DateCol("bd"),
    DateTimeUTCCol("bdt1"),
    BoolCol("bb", True),
)

# Table with unique StringCol
Baz = Table(
    "baz",
    AutoIdCol("baz_id"),
    IntCol("i3"),
    StringCol("s3", 5, unique=True),
)
