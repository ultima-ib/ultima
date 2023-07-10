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

    3) A DB connection. Similar to 2. We use connectorX library to 
    """