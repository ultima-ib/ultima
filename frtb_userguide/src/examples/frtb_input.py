import polars as pl
import ultibi as ul

# Note: sometimes Polars needs abit of help to determine the correct datatype for your
# columns. This is because Polars scans first N rows to determine correct dtype
exposures = pl.read_csv("./data/frtb/Delta.csv", dtypes={"SensitivitySpot": pl.Float64})
# Feel free to join other attributes such as Hierarchy here ...
ds = ul.FRTBDataSet.from_frame(exposures)


# You can also set up a config and we will take care of
# castings, joins etc
ds = ul.FRTBDataSet.from_config_path("./data/frtb/datasource_config.toml")

original = ds.frame()  # keep the old value for comparison


# Prepare assigns weights and other columns to the FRTBDataSet.
# If you call it twice you will get an error
ds.prepare()

# Let's see what happened
def diff(df1: pl.DataFrame, df2: pl.DataFrame) -> pl.DataFrame:
    """Columns in df1 which are not in df2

    Args:
        df1 (pl.DataFrame): First to compare
        df2 (pl.DataFrame): Second to compare

    Returns:
        pl.DataFrame: _description_
    """
    return df1.select([c for c in df1 if c.name not in df2])


# print(diff(ds.frame(), original))
