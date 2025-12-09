import pandas as pd
from undatum.cmds.analyzer import duckdb_decompose

DATA = [
    {"foo": 1, "bar": "some string", "baz": 1.23},
    {"foo": 2, "bar": "some other string", "baz": 2.34},
    {"foo": 3, "bar": "yet another string", "baz": 3.45},
]

df = pd.DataFrame(DATA)

print(duckdb_decompose(frame=df))