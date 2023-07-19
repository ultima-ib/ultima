from polars import DataFrame, LazyFrame
from typing_extensions import Self

from ..rust_module.ultibi_engine import DataSourceWrapper


class DataSource:
    """Represents a Source of your Data. The source can be:
    1) An In Memory Dataframe. This is the fastest way to perform computations.
    The price you pay for efficiency is, however, the process memory.
        i) (if relevant) .prepare() method will not be executed with every request
    2) A scan. Note! it's caller's  responsibiity to ensure that the
    LazyFrame is a scan (see scan_... operations https://pola-rs.github.io/polars/py-polars/html/reference/api.html#)
    It is not as fast as In Memory because you don't load data into process memory and
    hence you must load it with every request,
        i) (if relevant) Moreover .prepare() will run with every request, making it
            potentially too slow.

    3) A DB connection string. Similar to 2. We use connectorX library.

    Parameters
    ----------
    data : DataFrame, LazyFrame, str
        * DataFrame for In Memory Data
        * LazyFrame for a Scan. It's caller's responsibility to ensure the object was
            created with pl.scan_... function and was not collected at any point
        * str for a DB Connection String

    Examples
    --------
    Constructing:

    >>> import ultibi as ul
    >>> import polars as pl
    >>> # Note that the LazyFrame query must start with scan_
    >>> # and must've NOT been collected
    >>> scan = pl.scan_csv(
    ...     "../frtb_engine/data/frtb/Delta.csv", dtypes={"SensitivitySpot": pl.Float64}
    ... )
    >>> dsource = ul.DataSource.scan(scan)
    >>> ds = ul.DataSet.from_source(dsource)
    >>> # Such dataSet cannot be prepared because this would require reading the whole
    >>> # Data at once
    >>> # Prepare will run with each request.
    >>> ############################################################
    >>> scan = pl.read_csv(
    ...     "../frtb_engine/data/frtb/Delta.csv", dtypes={"SensitivitySpot": pl.Float64}
    ... )
    >>> dsource = ul.DataSource.inmemory(scan)
    >>> ds = ul.DataSet.from_source(dsource)
    >>> ds.prepare()

    """

    inner: DataSourceWrapper

    @classmethod
    def inmemory(cls, data: DataFrame) -> Self:
        return cls(data)

    @classmethod
    def scan(cls, data: LazyFrame) -> Self:
        return cls(data)

    def __init__(self, data: "DataFrame | LazyFrame | str"):
        if isinstance(data, DataFrame):
            self.inner = DataSourceWrapper.from_frame(data)

        elif isinstance(data, LazyFrame):
            self.inner = DataSourceWrapper.from_scan(data)

        elif isinstance(data, str):
            raise NotImplementedError("Scan DB is not yet implemented")
