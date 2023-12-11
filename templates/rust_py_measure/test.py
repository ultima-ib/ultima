import ultibi as ul
import polars as pl
from rust_py_measure import my_rusty_measures

df = pl.DataFrame(
            {
                "a": [1, 2, -3],
                "b": [4, 5, 6],
                "c": ["z", "z", "w"],
                "d": ["k", "y", "s"],
            }
        )

ds = ul.DataSet.from_frame(df, bespoke_measures=my_rusty_measures)

print(ds.measures)
request = dict(measures=[["MyRustyMeasure", "scalar"]], groupby=["c"])
result = ds.compute(request)
ds.ui()