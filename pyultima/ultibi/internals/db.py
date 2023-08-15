from polars.type_aliases import PolarsDataType

from ultibi.rust_module.ultibi_engine import DbInfo as DbInfoWrap


class DbInfo:
    """Db Info for Db Connections

    Parameters
    ----------
        table (str): name of your table such that we can call SELECT * FROM table
        db_type (str): currently has been tested for "MySQL" only
        conn_uri (str): connection string (see example)
        schema (list[tuple[str, DataType]]): SQL is bad at preserving/determening
            schemas. We strongly recommend that you provide this, so that we cast
            columns into the correct Type. If provided - we expect this
            to be correct.

    Examples
    --------
    Usage:

    >>> import ultibi as ul
    >>> import polars as pl
    >>> conn_uri = "mysql://%s:%s@%s:%d/%s?cxprotocol=binary" % (
    ...     "dummyUser",
    ...     "password",
    ...     "localhost",
    ...     3306,
    ...     "ultima",
    ... )
    >>> schema = [
    ...     ("COB", pl.Utf8),
    ...     ("TradeId", pl.Utf8),
    ...     ("SensitivitySpot", pl.Float64),
    ...     ("Sensitivity_025Y", pl.Float64),
    ...     ("EXOTIC_RRAO", pl.Boolean),
    ...     ("OTHER_RRAO", pl.Boolean),
    ... ]
    >>> db = ul.DbInfo("frtb", "MySQL", conn_uri, schema)
    >>> source = ul.DataSource.db(db)
    >>> build_params = dict(
    ...     fx_sqrt2_div="true",
    ...     girr_sqrt2_div="true",
    ...     csrnonsec_covered_bond_15="true",
    ...     DayCountConvention="2",
    ...     DateFormat="DateFormat",
    ... )
    >>> ds = ul.FRTBDataSet.from_source(source, build_params=build_params)
    >>> # ds.ui()

    """

    inner: DbInfoWrap
    "Rust code"

    def __init__(
        self,
        table: str,
        db_type: str,
        conn_uri: str,
        schema: "list[tuple[str, PolarsDataType]] | None" = None,
    ):
        self.table = table
        self.db_type = db_type
        self.conn_uri = conn_uri
        self.schema = schema
        self.inner = DbInfoWrap(table, db_type, conn_uri, schema)
