from ..rust_module.ultibi_engine import DataSourceWrapper

from polars import DataFrame, LazyFrame


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
    """

    inner: DataSourceWrapper

    @classmethod
    def inmemory(cls, data: DataFrame):
        return cls(data)
    
    @classmethod
    def scan(cls, data: LazyFrame):
        return cls(data)

    def __init__(self, data: DataFrame|LazyFrame|str):

        if isinstance(data, DataFrame):
            self.inner = DataSourceWrapper.from_frame(data)
        
        elif isinstance(data, LazyFrame):
            self.inner = DataSourceWrapper.from_scan(data)

        elif isinstance(data, str):
            raise NotImplementedError("Scan DB is not yet implemented")
        
if __name__ == "__main__":
    a = DataSource()
    print(a.inner)