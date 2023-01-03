import polars as pl
from ultima import FRTBDataSet

# Note: sometimes Polars needs abit of help to determine the correct datatype for your
# columns. This is because Polars scans first N rows to determine correct dtype
# https://stackoverflow.com/questions/73718144/python-polars-casting-string-to-numeric
exposures = pl.read_csv("./data/frtb/Delta.csv", dtypes={"SensitivitySpot": pl.Float64})
# Feel free to join other attributes such as Hierarchy here ...
ds = FRTBDataSet.from_frame(exposures)
print(ds.frame())

# You can also set up a config and we will take care of
# castings, joins etc
ds = FRTBDataSet.from_config_path("./data/frtb/datasource_config.toml")
print(ds.frame())

hey
